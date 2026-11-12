//! Storage for the RAX control plane.
//!
//! The store holds only what AX owns: specs, metadata, and the parts of a
//! task's status that describe AX's own work. Everything that belongs to the
//! task's actor is read from Agent Substrate by the server and never stored.
//!
//! Two backends are provided: [`MemoryStore`] for tests and local runs, and
//! [`PostgresStore`], which also implements [`Locker`] so that locks live next
//! to the data they protect and are shared by every server replica.

mod lock;
mod memory;
mod postgres;

use async_trait::async_trait;
use rax_api::v1alpha1::{Gateway, Model, ObjectMeta, Task, TaskStatus, Workspace};

pub use lock::{LockGuard, Locker, MemoryLocker, lock_key};
pub use memory::MemoryStore;
pub use postgres::{PostgresOptions, PostgresStore, describe_url};

/// The atespace a resource lands in when it names none.
pub const DEFAULT_ATESPACE: &str = "default";

/// How many tasks a list returns when the caller sets no limit.
pub const DEFAULT_TASK_LIST_LIMIT: i64 = 50;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("resource not found")]
    NotFound,
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Backend(String),
}

pub type Result<T, E = StoreError> = std::result::Result<T, E>;

/// Task records. The server reaches these only through its task helpers, which
/// strip the actor-owned status before writing and fill it in after reading.
#[async_trait]
pub trait TaskStore: Send + Sync {
    /// Stores or replaces a task, filling in its atespace, apiVersion, kind and
    /// a Pending phase when they are unset.
    async fn save_task(&self, task: &mut Task) -> Result<()>;
    async fn get_task(&self, atespace: &str, name: &str) -> Result<Task>;
    /// Lists tasks in an atespace, or across all of them when `atespace` is
    /// empty or `*`, most recently saved first. A `limit` of zero or less
    /// means [`DEFAULT_TASK_LIST_LIMIT`].
    async fn list_tasks(&self, atespace: &str, limit: i64, offset: i64) -> Result<Vec<Task>>;
    /// Replaces only the status of a task. It does not change the task's
    /// position in lists.
    async fn update_task_status(
        &self,
        atespace: &str,
        name: &str,
        status: TaskStatus,
    ) -> Result<()>;
    /// Removes a task. Deleting a missing task is not an error.
    async fn delete_task(&self, atespace: &str, name: &str) -> Result<()>;
}

/// Workspace, gateway and model records.
#[async_trait]
pub trait ResourceStore: Send + Sync {
    async fn save_workspace(&self, ws: &mut Workspace) -> Result<()>;
    async fn get_workspace(&self, atespace: &str, name: &str) -> Result<Workspace>;
    /// Lists workspaces in an atespace, or across all of them when `atespace`
    /// is empty or `*`, most recently saved first.
    async fn list_workspaces(&self, atespace: &str) -> Result<Vec<Workspace>>;
    async fn delete_workspace(&self, atespace: &str, name: &str) -> Result<()>;

    async fn save_gateway(&self, gw: &mut Gateway) -> Result<()>;
    async fn get_gateway(&self, atespace: &str, name: &str) -> Result<Gateway>;
    /// Lists gateways in an atespace, or across all of them when `atespace`
    /// is empty or `*`, most recently saved first.
    async fn list_gateways(&self, atespace: &str) -> Result<Vec<Gateway>>;
    /// Removes a gateway. Deleting a missing gateway is not an error.
    async fn delete_gateway(&self, atespace: &str, name: &str) -> Result<()>;

    async fn save_model(&self, model: &mut Model) -> Result<()>;
    async fn get_model(&self, atespace: &str, name: &str) -> Result<Model>;
    async fn list_models(&self, atespace: &str) -> Result<Vec<Model>>;
    async fn delete_model(&self, atespace: &str, name: &str) -> Result<()>;
}

/// A complete store backend.
pub trait Store: TaskStore + ResourceStore {}

impl<T: TaskStore + ResourceStore> Store for T {}

pub(crate) fn default_atespace(atespace: &str) -> &str {
    if atespace.is_empty() {
        DEFAULT_ATESPACE
    } else {
        atespace
    }
}

/// Reports whether a list filter selects every atespace.
pub(crate) fn all_atespaces(atespace: &str) -> bool {
    atespace.is_empty() || atespace == "*"
}

/// Fills in the metadata every stored resource has, returning an error when
/// it has no name.
fn default_meta<'a>(meta: &'a mut Option<ObjectMeta>, kind: &str) -> Result<&'a ObjectMeta> {
    let meta = meta.get_or_insert_with(Default::default);
    if meta.name.is_empty() {
        return Err(StoreError::Invalid(format!("{kind} name is required")));
    }
    if meta.atespace.is_empty() {
        meta.atespace = DEFAULT_ATESPACE.into();
    }
    Ok(meta)
}

fn default_type(api_version: &mut String, kind: &mut String, want_kind: &str) {
    if api_version.is_empty() {
        *api_version = rax_api::API_VERSION.into();
    }
    if kind.is_empty() {
        *kind = want_kind.into();
    }
}

pub(crate) fn prepare_task(task: &mut Task) -> Result<()> {
    default_meta(&mut task.metadata, "task")?;
    default_type(&mut task.api_version, &mut task.kind, rax_api::KIND_TASK);
    let status = task.status.get_or_insert_with(Default::default);
    if status.phase.is_empty() {
        status.phase = rax_api::phase::PENDING.into();
    }
    Ok(())
}

pub(crate) fn prepare_workspace(ws: &mut Workspace) -> Result<()> {
    default_meta(&mut ws.metadata, "workspace")?;
    default_type(&mut ws.api_version, &mut ws.kind, rax_api::KIND_WORKSPACE);
    Ok(())
}

pub(crate) fn prepare_gateway(gw: &mut Gateway) -> Result<()> {
    default_meta(&mut gw.metadata, "gateway")?;
    default_type(&mut gw.api_version, &mut gw.kind, rax_api::KIND_GATEWAY);
    Ok(())
}

pub(crate) fn prepare_model(model: &mut Model) -> Result<()> {
    default_meta(&mut model.metadata, "model")?;
    default_type(&mut model.api_version, &mut model.kind, rax_api::KIND_MODEL);
    Ok(())
}

pub(crate) fn meta_key(meta: &Option<ObjectMeta>) -> (String, String) {
    let m = meta.as_ref().expect("prepared resources have metadata");
    (m.atespace.clone(), m.name.clone())
}

#[cfg(test)]
pub(crate) mod testing {
    //! Behavior every backend must share, run against each of them.

    use super::*;
    use rax_api::v1alpha1::{Condition, TaskSpec};

    pub fn task(atespace: &str, name: &str) -> Task {
        Task {
            metadata: Some(ObjectMeta {
                name: name.into(),
                atespace: atespace.into(),
                create_time: None,
            }),
            spec: Some(TaskSpec {
                image: "alpine".into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn names<T>(items: &[T], meta: impl Fn(&T) -> &Option<ObjectMeta>) -> Vec<String> {
        items
            .iter()
            .map(|i| meta(i).as_ref().unwrap().name.clone())
            .collect()
    }

    pub async fn conformance(s: &dyn Store) {
        // Saving fills in defaults.
        let mut t = task("", "a");
        s.save_task(&mut t).await.unwrap();
        let got = s.get_task("default", "a").await.unwrap();
        assert_eq!(got.metadata.as_ref().unwrap().atespace, "default");
        assert_eq!(got.api_version, rax_api::API_VERSION);
        assert_eq!(got.kind, rax_api::KIND_TASK);
        assert_eq!(got.status.as_ref().unwrap().phase, rax_api::phase::PENDING);
        assert_eq!(got, t);
        // An empty atespace on reads means the default one.
        assert_eq!(s.get_task("", "a").await.unwrap(), t);

        assert!(matches!(
            s.get_task("default", "missing").await,
            Err(StoreError::NotFound)
        ));
        assert!(matches!(
            s.save_task(&mut task("default", "")).await,
            Err(StoreError::Invalid(_))
        ));

        // Lists are newest first, re-saving moves a task to the front, and a
        // status update does not.
        for name in ["b", "c"] {
            s.save_task(&mut task("default", name)).await.unwrap();
        }
        s.save_task(&mut task("other", "d")).await.unwrap();
        let list = s.list_tasks("default", 0, 0).await.unwrap();
        assert_eq!(names(&list, |t| &t.metadata), ["c", "b", "a"]);
        s.save_task(&mut task("default", "a")).await.unwrap();
        let status = TaskStatus {
            id: "id-b".into(),
            conditions: vec![Condition {
                r#type: "WorkspaceReady".into(),
                status: "True".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        s.update_task_status("default", "b", status.clone())
            .await
            .unwrap();
        let list = s.list_tasks("default", 0, 0).await.unwrap();
        assert_eq!(names(&list, |t| &t.metadata), ["a", "c", "b"]);
        assert_eq!(list[2].status.as_ref(), Some(&status));
        let all = s.list_tasks("*", 0, 0).await.unwrap();
        assert_eq!(names(&all, |t| &t.metadata), ["a", "d", "c", "b"]);
        assert_eq!(s.list_tasks("", 0, 0).await.unwrap().len(), 4);

        // Pagination.
        let page = s.list_tasks("default", 2, 1).await.unwrap();
        assert_eq!(names(&page, |t| &t.metadata), ["c", "b"]);
        assert!(s.list_tasks("default", 2, 10).await.unwrap().is_empty());

        assert!(matches!(
            s.update_task_status("default", "missing", TaskStatus::default())
                .await,
            Err(StoreError::NotFound)
        ));

        s.delete_task("default", "a").await.unwrap();
        s.delete_task("default", "a").await.unwrap();
        assert!(matches!(
            s.get_task("default", "a").await,
            Err(StoreError::NotFound)
        ));

        // Workspaces and models.
        let mut ws = Workspace {
            metadata: Some(ObjectMeta {
                name: "ws".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        s.save_workspace(&mut ws).await.unwrap();
        assert_eq!(ws.kind, rax_api::KIND_WORKSPACE);
        assert_eq!(s.get_workspace("", "ws").await.unwrap(), ws);
        let mut ws2 = Workspace {
            metadata: Some(ObjectMeta {
                name: "ws2".into(),
                atespace: "team".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        s.save_workspace(&mut ws2).await.unwrap();
        assert_eq!(
            names(&s.list_workspaces("").await.unwrap(), |w| &w.metadata),
            ["ws2", "ws"]
        );
        assert_eq!(
            names(&s.list_workspaces("team").await.unwrap(), |w| &w.metadata),
            ["ws2"]
        );
        s.delete_workspace("team", "ws2").await.unwrap();
        assert!(matches!(
            s.get_workspace("team", "ws2").await,
            Err(StoreError::NotFound)
        ));

        for g in ["g1", "g2"] {
            let mut gw = Gateway {
                metadata: Some(ObjectMeta {
                    name: g.into(),
                    ..Default::default()
                }),
                ..Default::default()
            };
            s.save_gateway(&mut gw).await.unwrap();
            assert_eq!(gw.kind, rax_api::KIND_GATEWAY);
            assert_eq!(s.get_gateway("default", g).await.unwrap(), gw);
        }
        assert_eq!(
            names(&s.list_gateways("*").await.unwrap(), |g| &g.metadata),
            ["g2", "g1"]
        );
        s.delete_gateway("default", "g1").await.unwrap();
        s.delete_gateway("default", "g1").await.unwrap();
        assert!(matches!(
            s.get_gateway("default", "g1").await,
            Err(StoreError::NotFound)
        ));
        assert_eq!(
            names(&s.list_gateways("default").await.unwrap(), |g| &g.metadata),
            ["g2"]
        );

        let mut m = Model {
            metadata: Some(ObjectMeta {
                name: "gemini".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        s.save_model(&mut m).await.unwrap();
        assert_eq!(m.kind, rax_api::KIND_MODEL);
        assert_eq!(s.get_model("default", "gemini").await.unwrap(), m);
        assert_eq!(s.list_models("*").await.unwrap(), vec![m]);
        s.delete_model("default", "gemini").await.unwrap();
        assert!(s.list_models("").await.unwrap().is_empty());
    }
}
