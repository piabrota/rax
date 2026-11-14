//! The AX service against an in-memory store, with Substrate stood in for by
//! fake reconcilers and actor readers.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rax_api::v1alpha1::{self as api, *};
use rax_api::{format_task_state, reason};
use rax_server::{Actor, ActorState, Actors, AxService, Reconciler, ServerOptions, router};
use rax_store::{MemoryStore, ResourceStore, StoreError, TaskStore};
use tonic::{Code, Status};

fn meta(name: &str) -> Option<ObjectMeta> {
    Some(ObjectMeta {
        name: name.into(),
        ..Default::default()
    })
}

fn new_task(name: &str, image: &str) -> api::CreateTaskRequest {
    api::CreateTaskRequest {
        task: Some(Task {
            metadata: meta(name),
            spec: Some(TaskSpec {
                profile: Some(Profile {
                    image: image.into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
    }
}

fn get(name: &str) -> api::GetTaskRequest {
    api::GetTaskRequest {
        atespace: String::new(),
        name: name.into(),
    }
}

fn state_of(t: &Task) -> &'static str {
    format_task_state(t)
}

fn service(store: Arc<MemoryStore>, opts: ServerOptions) -> AxService {
    AxService::new(store, opts)
}

/// A reconciler that records what it was asked and fails on demand. It also
/// reads back the actors it drove, as Substrate would.
#[derive(Default)]
struct FakeReconciler {
    state: Mutex<FakeState>,
}

#[derive(Default)]
struct FakeState {
    /// Suspends and resumes.
    calls: usize,
    deletes: usize,
    err: Option<Status>,
    /// The conditions a failed call leaves on the task.
    err_conditions: Option<Vec<Condition>>,
    observed: Option<Task>,
    delete_err: Option<Status>,
    /// Runs on delete, with the task's stored record at that moment.
    on_delete: Option<Arc<dyn Fn() + Send + Sync>>,
    /// The state of each task's actor, by task name.
    actors: HashMap<String, ActorState>,
    /// The workspace the last suspend or resume was given.
    workspace: Option<Workspace>,
    /// The gateway the last suspend or resume was given.
    gateway: Option<Gateway>,
}

impl FakeReconciler {
    fn with(f: impl FnOnce(&mut FakeState)) -> Arc<Self> {
        let r = Arc::new(Self::default());
        f(&mut r.state.lock().unwrap());
        r
    }

    fn set(&self, f: impl FnOnce(&mut FakeState)) {
        f(&mut self.state.lock().unwrap());
    }

    fn counts(&self) -> (usize, usize) {
        let s = self.state.lock().unwrap();
        (s.calls, s.deletes)
    }

    fn observed(&self) -> Task {
        self.state.lock().unwrap().observed.clone().unwrap()
    }

    fn drive(&self, task: &mut Task, to: ActorState) -> Result<(), Status> {
        let mut s = self.state.lock().unwrap();
        s.calls += 1;
        s.observed = Some(task.clone());
        if let Some(err) = s.err.clone() {
            if let Some(c) = &s.err_conditions {
                task.status.get_or_insert_with(Default::default).conditions = c.clone();
            }
            return Err(err);
        }
        let name = task.metadata.as_ref().unwrap().name.clone();
        s.actors.insert(name, to);
        Ok(())
    }

    fn actor(name: &str, state: ActorState) -> Actor {
        Actor {
            name: name.into(),
            uid: format!("uid-{name}"),
            state,
            worker_ip: if state == ActorState::Running {
                "10.244.1.42".into()
            } else {
                String::new()
            },
            crash_message: String::new(),
        }
    }
}

#[async_trait]
impl Reconciler for FakeReconciler {
    async fn suspend(
        &self,
        task: &mut Task,
        ws: Option<Workspace>,
        gw: Option<Gateway>,
    ) -> Result<(), Status> {
        let mut s = self.state.lock().unwrap();
        (s.workspace, s.gateway) = (ws, gw);
        drop(s);
        self.drive(task, ActorState::Suspended)
    }

    async fn resume(
        &self,
        task: &mut Task,
        ws: Option<Workspace>,
        gw: Option<Gateway>,
    ) -> Result<(), Status> {
        let mut s = self.state.lock().unwrap();
        (s.workspace, s.gateway) = (ws, gw);
        drop(s);
        self.drive(task, ActorState::Running)
    }

    async fn delete(&self, task: &Task) -> Result<(), Status> {
        let name = task.metadata.as_ref().map_or("", |m| m.name.as_str());
        let (hook, err) = {
            let mut s = self.state.lock().unwrap();
            s.deletes += 1;
            if s.delete_err.is_none() {
                s.actors.remove(name);
            }
            (s.on_delete.clone(), s.delete_err.clone())
        };
        if let Some(h) = hook {
            h();
        }
        err.map_or(Ok(()), Err)
    }
}

#[async_trait]
impl Actors for FakeReconciler {
    async fn get_actor(&self, atespace: &str, name: &str) -> Result<Option<Actor>, Status> {
        let s = self.state.lock().unwrap();
        Ok((atespace == "default")
            .then(|| s.actors.get(name).map(|st| Self::actor(name, *st)))
            .flatten())
    }

    async fn list_actors(&self, atespace: &str) -> Result<Vec<Actor>, Status> {
        let s = self.state.lock().unwrap();
        Ok(if atespace == "default" {
            s.actors.iter().map(|(n, st)| Self::actor(n, *st)).collect()
        } else {
            vec![]
        })
    }
}

/// Options that drive and read actors through `rec`, as rax-server does
/// through one Substrate client.
fn on(rec: Arc<FakeReconciler>) -> ServerOptions {
    ServerOptions {
        reconciler: Some(rec.clone()),
        actors: Some(rec),
        ..Default::default()
    }
}

/// Every actor named in `names` exists in the default atespace, in `state`.
struct FakeActors {
    names: Vec<String>,
    state: Mutex<ActorState>,
    worker_ip: String,
}

impl FakeActors {
    fn actor(&self, name: &str) -> Actor {
        Actor {
            name: name.into(),
            uid: format!("uid-{name}"),
            state: *self.state.lock().unwrap(),
            worker_ip: self.worker_ip.clone(),
            crash_message: String::new(),
        }
    }
}

#[async_trait]
impl Actors for FakeActors {
    async fn get_actor(&self, atespace: &str, name: &str) -> Result<Option<Actor>, Status> {
        Ok(
            (atespace == "default" && self.names.iter().any(|n| n == name))
                .then(|| self.actor(name)),
        )
    }

    async fn list_actors(&self, atespace: &str) -> Result<Vec<Actor>, Status> {
        Ok(if atespace == "default" {
            self.names.iter().map(|n| self.actor(n)).collect()
        } else {
            vec![]
        })
    }
}

fn assert_no_actor_state(stored: &Task) {
    let st = stored.status.as_ref().unwrap();
    assert!(
        st.state() == TaskState::Unspecified && st.actor.is_empty() && st.actor_uid.is_empty(),
        "stored status {st:?}"
    );
    assert!(
        st.conditions.iter().all(|c| c.r#type != "Ready"),
        "stored status has a Ready condition: {st:?}"
    );
}

#[tokio::test]
async fn grpc_round_trip() {
    let svc = service(Arc::new(MemoryStore::new()), Default::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router(svc)).await.unwrap() });
    let mut c = api::ax_client::AxClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    // Metadata is left partially empty to exercise server-side defaulting.
    c.create_workspace(CreateWorkspaceRequest {
        workspace: Some(Workspace {
            metadata: meta("grpc-ws"),
            spec: Some(Default::default()),
            ..Default::default()
        }),
    })
    .await
    .unwrap();
    c.update_model(UpdateModelRequest {
        model: Some(Model {
            metadata: meta("grpc-model"),
            spec: Some(ModelSpec {
                provider: "google".into(),
                model: "gemini-3.8-flash".into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
    })
    .await
    .unwrap();
    c.create_task(new_task("grpc-task", "alpine"))
        .await
        .unwrap();

    let ws = c
        .get_workspace(GetWorkspaceRequest {
            atespace: "default".into(),
            name: "grpc-ws".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let m = ws.metadata.unwrap();
    assert_eq!(m.atespace, "default");
    assert!(m.create_time.is_some());

    let mut task = c.get_task(get("grpc-task")).await.unwrap().into_inner();
    assert!(task.metadata.as_ref().unwrap().create_time.is_some());
    let list = c
        .list_tasks(ListTasksRequest {
            atespace: "default".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.tasks.len(), 1);
    assert!(
        list.tasks[0]
            .metadata
            .as_ref()
            .unwrap()
            .create_time
            .is_some()
    );

    // Tasks are immutable.
    task.spec.as_mut().unwrap().profile = Some(Profile {
        image: "ghcr.io/test/updated-image".into(),
        ..Default::default()
    });
    let err = c
        .create_task(CreateTaskRequest { task: Some(task) })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    let t = c
        .suspend_task(SuspendTaskRequest {
            atespace: "default".into(),
            name: "grpc-task".into(),
        })
        .await
        .unwrap();
    assert_eq!(state_of(t.get_ref()), "Suspended");
    let t = c
        .resume_task(ResumeTaskRequest {
            atespace: "default".into(),
            name: "grpc-task".into(),
        })
        .await
        .unwrap();
    assert_eq!(state_of(t.get_ref()), "Working");

    let wss = c
        .list_workspaces(ListWorkspacesRequest {
            atespace: "default".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(wss.get_ref().workspaces.len(), 1);
    let model = c
        .get_model(GetModelRequest {
            atespace: "default".into(),
            name: "grpc-model".into(),
        })
        .await
        .unwrap();
    assert_eq!(
        model.get_ref().metadata.as_ref().unwrap().name,
        "grpc-model"
    );
    let models = c
        .list_models(ListModelsRequest {
            atespace: "default".into(),
        })
        .await
        .unwrap();
    assert_eq!(models.get_ref().models.len(), 1);

    c.delete_task(DeleteTaskRequest {
        atespace: "default".into(),
        name: "grpc-task".into(),
    })
    .await
    .unwrap();
    let err = c
        .delete_task(DeleteTaskRequest {
            atespace: "default".into(),
            name: "no-such-task".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    c.delete_workspace(DeleteWorkspaceRequest {
        atespace: "default".into(),
        name: "grpc-ws".into(),
    })
    .await
    .unwrap();
    c.delete_model(DeleteModelRequest {
        atespace: "default".into(),
        name: "grpc-model".into(),
    })
    .await
    .unwrap();
    assert_eq!(
        c.get_task(get("grpc-task")).await.unwrap_err().code(),
        Code::NotFound
    );
}

#[tokio::test]
async fn rejects_invalid_names() {
    let svc = service(Arc::new(MemoryStore::new()), Default::default());
    for m in [
        ObjectMeta {
            name: "Task-With-Caps".into(),
            ..Default::default()
        },
        ObjectMeta {
            name: "under_score".into(),
            ..Default::default()
        },
        ObjectMeta::default(),
        ObjectMeta {
            name: "ok".into(),
            atespace: "Not-Lowercase".into(),
            ..Default::default()
        },
    ] {
        let task = Task {
            metadata: Some(m.clone()),
            ..Default::default()
        };
        let err = svc
            .create_task(CreateTaskRequest { task: Some(task) })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{m:?}");
        let ws = Workspace {
            metadata: Some(m.clone()),
            ..Default::default()
        };
        let err = svc
            .create_workspace(CreateWorkspaceRequest {
                workspace: Some(ws),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{m:?}");
        let model = Model {
            metadata: Some(m.clone()),
            ..Default::default()
        };
        let err = svc
            .update_model(UpdateModelRequest { model: Some(model) })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{m:?}");
    }
    // Nothing invalid was persisted.
    assert!(
        svc.list_tasks(Default::default())
            .await
            .unwrap()
            .tasks
            .is_empty()
    );

    let good = ObjectMeta {
        name: "task-with-caps".into(),
        atespace: "team-a".into(),
        ..Default::default()
    };
    svc.create_task(CreateTaskRequest {
        task: Some(Task {
            metadata: Some(good),
            ..Default::default()
        }),
    })
    .await
    .unwrap();
    svc.create_workspace(CreateWorkspaceRequest {
        workspace: Some(Workspace {
            metadata: meta("ws-1"),
            ..Default::default()
        }),
    })
    .await
    .unwrap();
    svc.update_model(UpdateModelRequest {
        model: Some(Model {
            metadata: meta("gemini"),
            ..Default::default()
        }),
    })
    .await
    .unwrap();
    assert!(
        svc.create_task(CreateTaskRequest { task: None })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn validates_workspace_binding() {
    let svc = service(Arc::new(MemoryStore::new()), Default::default());
    let bind = |name: &str, ws: WorkspaceRef| CreateTaskRequest {
        task: Some(Task {
            metadata: meta(name),
            spec: Some(TaskSpec {
                workspace: Some(ws),
                ..Default::default()
            }),
            ..Default::default()
        }),
    };
    let err = svc
        .create_task(bind(
            "bad",
            WorkspaceRef {
                name: "".into(),
                path: "/w".into(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    svc.create_workspace(CreateWorkspaceRequest {
        workspace: Some(Workspace {
            metadata: meta("a"),
            ..Default::default()
        }),
    })
    .await
    .unwrap();
    svc.create_task(bind(
        "good",
        WorkspaceRef {
            name: "a".into(),
            path: "".into(),
        },
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn reconciler_lifecycle() {
    let rec = Arc::new(FakeReconciler::default());
    let svc = service(Arc::new(MemoryStore::new()), on(rec.clone()));

    let t = svc
        .create_task(new_task("task-rec", "alpine"))
        .await
        .unwrap();
    assert_eq!(rec.counts(), (1, 0));
    assert_eq!(state_of(&t), "Suspended");

    let t = svc
        .resume_task(ResumeTaskRequest {
            name: "task-rec".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(rec.counts(), (2, 0));
    assert_eq!(state_of(&t), "Working");

    let t = svc
        .suspend_task(SuspendTaskRequest {
            name: "task-rec".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(rec.counts(), (3, 0));
    assert_eq!(state_of(&t), "Suspended");

    svc.delete_task(DeleteTaskRequest {
        name: "task-rec".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(rec.counts(), (3, 1));
    assert_eq!(
        svc.get_task(get("task-rec")).await.unwrap_err().code(),
        Code::NotFound
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_marks_terminating_while_cleaning_up() {
    let store = Arc::new(MemoryStore::new());
    let observed = Arc::new(Mutex::new(String::new()));
    let rec = Arc::new(FakeReconciler::default());
    {
        let (store, observed) = (store.clone(), observed.clone());
        rec.set(|s| {
            s.on_delete = Some(Arc::new(move || {
                let store = store.clone();
                let t = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(store.get_task("default", "task-term"))
                });
                *observed.lock().unwrap() = t.map(|t| state_of(&t).to_owned()).unwrap_or_default();
            }))
        });
    }
    let svc = service(store, on(rec));
    svc.create_task(new_task("task-term", "alpine"))
        .await
        .unwrap();
    svc.delete_task(DeleteTaskRequest {
        name: "task-term".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(*observed.lock().unwrap(), "Terminating");
    assert_eq!(
        svc.get_task(get("task-term")).await.unwrap_err().code(),
        Code::NotFound
    );
}

#[tokio::test]
async fn delete_keeps_task_when_cleanup_fails() {
    let rec = FakeReconciler::with(|s| {
        s.delete_err = Some(Status::unknown("substrate timeout deleting actor"))
    });
    let svc = service(Arc::new(MemoryStore::new()), on(rec));
    svc.create_task(new_task("task-err", "alpine"))
        .await
        .unwrap();
    let err = svc
        .delete_task(DeleteTaskRequest {
            name: "task-err".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    let t = svc.get_task(get("task-err")).await.unwrap();
    assert_eq!(state_of(&t), "Terminating");
}

/// A store whose status updates fail on demand.
#[derive(Default)]
struct FailingStore {
    inner: MemoryStore,
    fail_updates: AtomicBool,
    /// What every workspace lookup fails with, if anything.
    get_workspace_err: Mutex<Option<StoreError>>,
    /// What every gateway lookup fails with, if anything.
    get_gateway_err: Mutex<Option<StoreError>>,
}

#[async_trait]
impl TaskStore for FailingStore {
    async fn save_task(&self, task: &mut Task) -> rax_store::Result<()> {
        self.inner.save_task(task).await
    }
    async fn get_task(&self, a: &str, n: &str) -> rax_store::Result<Task> {
        self.inner.get_task(a, n).await
    }
    async fn list_tasks(&self, a: &str, l: i64, o: i64) -> rax_store::Result<Vec<Task>> {
        self.inner.list_tasks(a, l, o).await
    }
    async fn update_task_status(&self, a: &str, n: &str, s: TaskStatus) -> rax_store::Result<()> {
        if self.fail_updates.load(Ordering::SeqCst) {
            return Err(StoreError::Backend("simulated store failure".into()));
        }
        self.inner.update_task_status(a, n, s).await
    }
    async fn set_task_delete_time(
        &self,
        a: &str,
        n: &str,
        t: Option<pbjson_types::Timestamp>,
    ) -> rax_store::Result<()> {
        if self.fail_updates.load(Ordering::SeqCst) {
            return Err(StoreError::Backend("simulated store failure".into()));
        }
        self.inner.set_task_delete_time(a, n, t).await
    }
    async fn delete_task(&self, a: &str, n: &str) -> rax_store::Result<()> {
        self.inner.delete_task(a, n).await
    }
}

#[async_trait]
impl ResourceStore for FailingStore {
    async fn save_workspace(&self, ws: &mut Workspace) -> rax_store::Result<()> {
        self.inner.save_workspace(ws).await
    }
    async fn get_workspace(&self, a: &str, n: &str) -> rax_store::Result<Workspace> {
        if let Some(e) = self.get_workspace_err.lock().unwrap().take() {
            return Err(e);
        }
        self.inner.get_workspace(a, n).await
    }
    async fn list_workspaces(&self, a: &str, l: i64, o: i64) -> rax_store::Result<Vec<Workspace>> {
        self.inner.list_workspaces(a, l, o).await
    }
    async fn delete_workspace(&self, a: &str, n: &str) -> rax_store::Result<()> {
        self.inner.delete_workspace(a, n).await
    }
    async fn save_gateway(&self, g: &mut Gateway) -> rax_store::Result<()> {
        self.inner.save_gateway(g).await
    }
    async fn get_gateway(&self, a: &str, n: &str) -> rax_store::Result<Gateway> {
        if let Some(e) = self.get_gateway_err.lock().unwrap().take() {
            return Err(e);
        }
        self.inner.get_gateway(a, n).await
    }
    async fn list_gateways(&self, a: &str) -> rax_store::Result<Vec<Gateway>> {
        self.inner.list_gateways(a).await
    }
    async fn delete_gateway(&self, a: &str, n: &str) -> rax_store::Result<()> {
        self.inner.delete_gateway(a, n).await
    }
    async fn save_model(&self, m: &mut Model) -> rax_store::Result<()> {
        self.inner.save_model(m).await
    }
    async fn get_model(&self, a: &str, n: &str) -> rax_store::Result<Model> {
        self.inner.get_model(a, n).await
    }
    async fn list_models(&self, a: &str) -> rax_store::Result<Vec<Model>> {
        self.inner.list_models(a).await
    }
    async fn delete_model(&self, a: &str, n: &str) -> rax_store::Result<()> {
        self.inner.delete_model(a, n).await
    }
}

/// A task is immutable, so CreateTask must not save or provision a task whose
/// Workspace does not exist yet: it would run with an empty workspace forever.
#[tokio::test]
async fn create_task_requires_bound_workspace() {
    struct Case {
        name: &'static str,
        task_atespace: &'static str,
        /// Where the "data" Workspace exists; empty for nowhere.
        ws_atespace: &'static str,
        binding: &'static str,
        get_err: Option<StoreError>,
        want_code: Code,
        want_msg: &'static str,
    }
    let base = || Case {
        name: "",
        task_atespace: "",
        ws_atespace: "",
        binding: "data",
        get_err: None,
        want_code: Code::Ok,
        want_msg: "",
    };
    for tc in [
        Case {
            name: "missing",
            want_code: Code::FailedPrecondition,
            want_msg: r#"workspace "data" not found in atespace "default""#,
            ..base()
        },
        Case {
            name: "other atespace",
            task_atespace: "team-a",
            ws_atespace: "team-b",
            want_code: Code::FailedPrecondition,
            want_msg: r#"workspace "data" not found in atespace "team-a""#,
            ..base()
        },
        Case {
            name: "store error",
            get_err: Some(StoreError::Backend("store unavailable".into())),
            want_code: Code::Internal,
            want_msg: r#"getting workspace "data": store unavailable"#,
            ..base()
        },
        Case {
            name: "present",
            ws_atespace: "default",
            ..base()
        },
        Case {
            name: "present in task atespace",
            task_atespace: "team-a",
            ws_atespace: "team-a",
            ..base()
        },
        Case {
            name: "no binding",
            binding: "",
            ..base()
        },
    ] {
        let store = Arc::new(FailingStore::default());
        *store.get_workspace_err.lock().unwrap() = tc.get_err;
        if !tc.ws_atespace.is_empty() {
            store
                .inner
                .save_workspace(&mut Workspace {
                    metadata: Some(ObjectMeta {
                        atespace: tc.ws_atespace.into(),
                        name: "data".into(),
                        ..Default::default()
                    }),
                    spec: Some(WorkspaceSpec::default()),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        let rec = Arc::new(FakeReconciler::default());
        let svc = AxService::new(store.clone(), on(rec.clone()));
        let task = Task {
            metadata: Some(ObjectMeta {
                atespace: tc.task_atespace.into(),
                name: "task".into(),
                ..Default::default()
            }),
            spec: Some(TaskSpec {
                profile: Some(Profile {
                    image: "alpine".into(),
                    ..Default::default()
                }),
                workspace: (!tc.binding.is_empty()).then(|| WorkspaceRef {
                    name: tc.binding.into(),
                    path: String::new(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let result = svc
            .create_task(CreateTaskRequest { task: Some(task) })
            .await;
        let (code, msg) = match &result {
            Ok(_) => (Code::Ok, String::new()),
            Err(e) => (e.code(), e.message().to_owned()),
        };
        assert_eq!(code, tc.want_code, "{}: {msg}", tc.name);
        assert!(msg.contains(tc.want_msg), "{}: {msg:?}", tc.name);

        let atespace = if tc.task_atespace.is_empty() {
            "default"
        } else {
            tc.task_atespace
        };
        let stored = store.inner.get_task(atespace, "task").await;
        if tc.want_code != Code::Ok {
            assert!(
                matches!(stored, Err(StoreError::NotFound)),
                "{}: rejected task was stored",
                tc.name
            );
            assert_eq!(
                rec.counts(),
                (0, 0),
                "{}: rejected task was provisioned",
                tc.name
            );
            continue;
        }
        stored.unwrap_or_else(|e| panic!("{}: stored task: {e}", tc.name));
        assert_eq!(
            rec.counts(),
            (1, 0),
            "{}: accepted task not provisioned",
            tc.name
        );
        // The reconciler gets the workspace the server resolved.
        let ws = rec.state.lock().unwrap().workspace.clone();
        assert_eq!(
            ws.and_then(|w| w.metadata).map(|m| m.name),
            (!tc.binding.is_empty()).then(|| "data".to_owned()),
            "{}",
            tc.name
        );
    }
}

#[tokio::test]
async fn delete_stops_when_status_update_fails() {
    let store = Arc::new(FailingStore::default());
    let rec = Arc::new(FakeReconciler::default());
    let svc = AxService::new(store.clone(), on(rec.clone()));
    svc.create_task(new_task("task-status-fail", "alpine"))
        .await
        .unwrap();
    store.fail_updates.store(true, Ordering::SeqCst);
    assert!(
        svc.delete_task(DeleteTaskRequest {
            name: "task-status-fail".into(),
            ..Default::default()
        })
        .await
        .is_err()
    );
    assert_eq!(
        rec.counts().1,
        0,
        "Delete must not run when the status update fails"
    );
}

async fn save_running(store: &MemoryStore, name: &str) {
    let mut t = Task {
        metadata: Some(ObjectMeta {
            name: name.into(),
            atespace: "default".into(),
            ..Default::default()
        }),
        spec: Some(TaskSpec {
            profile: Some(Profile {
                image: "alpine".into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        status: Some(TaskStatus {
            state: TaskState::Working as i32,
            actor: name.into(),
            conditions: vec![Condition {
                r#type: "Ready".into(),
                status: "True".into(),
                reason: reason::TASK_RUNNING.into(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    store.save_task(&mut t).await.unwrap();
}

#[tokio::test]
async fn failed_suspend_keeps_running_state() {
    let store = Arc::new(MemoryStore::new());
    let rec = FakeReconciler::with(|s| s.err = Some(Status::unknown("substrate unavailable")));
    // No actor reader: the stored status is what a read returns.
    let svc = service(
        store.clone(),
        ServerOptions {
            reconciler: Some(rec),
            ..Default::default()
        },
    );
    save_running(&store, "task-suspend-err").await;

    let err = svc
        .suspend_task(SuspendTaskRequest {
            name: "task-suspend-err".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    let t = svc.get_task(get("task-suspend-err")).await.unwrap();
    let st = t.status.unwrap();
    assert_eq!(st.state(), TaskState::Working);
    assert_eq!(st.conditions.len(), 1);
    assert_eq!(st.conditions[0].reason, reason::TASK_RUNNING);
}

#[tokio::test]
async fn successful_suspend_stores_no_actor_state() {
    let store = Arc::new(MemoryStore::new());
    let svc = service(store.clone(), on(Arc::new(FakeReconciler::default())));
    save_running(&store, "task-suspend-ok").await;
    let t = svc
        .suspend_task(SuspendTaskRequest {
            name: "task-suspend-ok".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(state_of(&t), "Suspended");
    assert_no_actor_state(&store.get_task("default", "task-suspend-ok").await.unwrap());
}

#[tokio::test]
async fn reads_actor_state_from_substrate() {
    let store = Arc::new(MemoryStore::new());
    let actors = Arc::new(FakeActors {
        names: vec!["observed-a".into(), "observed-b".into()],
        state: Mutex::new(ActorState::Running),
        worker_ip: "10.244.1.7".into(),
    });
    let svc = service(
        store.clone(),
        ServerOptions {
            reconciler: Some(Arc::new(FakeReconciler::default())),
            actors: Some(actors.clone()),
            ..Default::default()
        },
    );
    for name in ["observed-a", "observed-b"] {
        svc.create_task(new_task(name, "alpine")).await.unwrap();
    }

    let st = svc
        .get_task(get("observed-a"))
        .await
        .unwrap()
        .status
        .unwrap();
    assert_eq!(
        (st.state(), st.actor.as_str(), st.actor_uid.as_str()),
        (TaskState::Working, "observed-a", "uid-observed-a")
    );

    let list = svc
        .list_tasks(ListTasksRequest {
            atespace: "default".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    for t in &list.tasks {
        let st = t.status.as_ref().unwrap();
        assert_eq!(st.state(), TaskState::Working);
        assert_eq!(st.actor, t.metadata.as_ref().unwrap().name);
    }

    // A change on Substrate alone is visible on the next read.
    *actors.state.lock().unwrap() = ActorState::Suspended;
    assert_eq!(
        state_of(&svc.get_task(get("observed-a")).await.unwrap()),
        "Suspended"
    );
    assert_no_actor_state(&store.get_task("default", "observed-a").await.unwrap());
}

#[tokio::test]
async fn new_tasks_start_suspended() {
    let store = Arc::new(MemoryStore::new());
    let svc = service(store.clone(), on(Arc::new(FakeReconciler::default())));
    let t = svc
        .create_task(new_task("task-create-suspended", "alpine"))
        .await
        .unwrap();
    assert_eq!(state_of(&t), "Suspended");
    assert_no_actor_state(
        &store
            .get_task("default", "task-create-suspended")
            .await
            .unwrap(),
    );
}

#[tokio::test]
async fn failed_provisioning_marks_failed() {
    let rec =
        FakeReconciler::with(|s| s.err = Some(Status::unknown("substrate suspend actor failure")));
    let svc = service(Arc::new(MemoryStore::new()), on(rec));
    let err = svc
        .create_task(new_task("task-create-fail", "alpine"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Internal);
    assert_eq!(
        state_of(&svc.get_task(get("task-create-fail")).await.unwrap()),
        "Failed"
    );
    // The task exists now, so creating it again is refused.
    let err = svc
        .create_task(new_task("task-create-fail", "alpine"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn recovers_from_transient_substrate_outage() {
    let rec = FakeReconciler::with(|s| {
        s.err = Some(Status::unavailable("connection refused"));
        s.err_conditions = Some(vec![Condition {
            r#type: "Ready".into(),
            status: "False".into(),
            reason: reason::SUBSTRATE_UNAVAILABLE.into(),
            ..Default::default()
        }]);
    });
    let svc = service(Arc::new(MemoryStore::new()), on(rec.clone()));

    // Creating while Substrate is unreachable records Pending, not Failed.
    let err = svc
        .create_task(new_task("simple-task", "alpine"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    let stored = svc.get_task(get("simple-task")).await.unwrap();
    assert_eq!(state_of(&stored), "Queued");

    // Re-applying a Pending task with a different spec is refused.
    let err = svc
        .create_task(new_task("simple-task", "debian"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);

    // Re-applying with the status omitted keeps the stored conditions.
    rec.set(|s| s.err_conditions = None);
    let err = svc
        .create_task(new_task("simple-task", "alpine"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    let conds = rec.observed().status.unwrap().conditions;
    assert_eq!(conds.len(), 1);
    assert_eq!(conds[0].reason, reason::SUBSTRATE_UNAVAILABLE);

    // Client-supplied status is ignored on a Pending retry.
    let mut forged = new_task("simple-task", "alpine");
    forged.task.as_mut().unwrap().status = Some(TaskStatus {
        state: TaskState::Working as i32,
        conditions: vec![Condition {
            r#type: "Ready".into(),
            status: "True".into(),
            reason: "ClientForged".into(),
            ..Default::default()
        }],
        ..Default::default()
    });
    assert_eq!(
        svc.create_task(forged).await.unwrap_err().code(),
        Code::Unavailable
    );
    let conds = rec.observed().status.unwrap().conditions;
    assert_eq!(conds.len(), 1);
    assert_eq!(conds[0].reason, reason::SUBSTRATE_UNAVAILABLE);

    // Once Substrate recovers, the same apply succeeds and keeps the creation time.
    rec.set(|s| {
        s.err = None;
    });
    let created = svc
        .create_task(new_task("simple-task", "alpine"))
        .await
        .unwrap();
    assert_eq!(state_of(&created), "Suspended");
    assert_eq!(
        created.metadata.unwrap().create_time,
        stored.metadata.unwrap().create_time
    );

    // A brief outage during resume keeps the task Pending instead of Failed.
    rec.set(|s| {
        s.err = Some(Status::unavailable("substrate restarting"));
    });
    let err = svc
        .resume_task(ResumeTaskRequest {
            name: "simple-task".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    assert_eq!(
        state_of(&svc.get_task(get("simple-task")).await.unwrap()),
        "Queued"
    );
}

#[tokio::test]
async fn update_keeps_creation_time() {
    let svc = service(Arc::new(MemoryStore::new()), Default::default());
    let req = || UpdateModelRequest {
        model: Some(Model {
            metadata: meta("m"),
            ..Default::default()
        }),
    };
    let first = svc
        .update_model(req())
        .await
        .unwrap()
        .metadata
        .unwrap()
        .create_time;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let second = svc
        .update_model(req())
        .await
        .unwrap()
        .metadata
        .unwrap()
        .create_time;
    assert!(first.is_some());
    assert_eq!(first, second);
}

fn gateway(name: &str, atespace: &str, host: &str) -> Gateway {
    Gateway {
        metadata: Some(ObjectMeta {
            name: name.into(),
            atespace: atespace.into(),
            ..Default::default()
        }),
        spec: Some(GatewaySpec {
            egress: vec![rax_api::substrate::ateapi::v1::EgressRule {
                https: Some(rax_api::substrate::ateapi::v1::HttpsRule {
                    hostnames: vec![host.into()],
                    ..Default::default()
                }),
                ..Default::default()
            }],
        }),
        ..Default::default()
    }
}

/// A task that binds a gateway needs it to exist: CreateTask neither saves
/// nor provisions a task whose Gateway is missing. The reconciler gets the
/// gateway the server resolved.
#[tokio::test]
async fn create_task_requires_bound_gateway() {
    for (name, task_atespace, gw_atespace, get_err, want_code, want_msg) in [
        (
            "missing",
            "",
            "",
            None,
            Code::FailedPrecondition,
            r#"gateway "gw" not found in atespace "default""#,
        ),
        (
            "store error",
            "",
            "",
            Some(StoreError::Backend("store unavailable".into())),
            Code::Internal,
            r#"getting gateway "gw": store unavailable"#,
        ),
        ("present", "team-a", "team-a", None, Code::Ok, ""),
    ] {
        let store = Arc::new(FailingStore::default());
        *store.get_gateway_err.lock().unwrap() = get_err;
        if !gw_atespace.is_empty() {
            store
                .inner
                .save_gateway(&mut gateway("gw", gw_atespace, "api.example.com"))
                .await
                .unwrap();
        }
        let rec = Arc::new(FakeReconciler::default());
        let svc = AxService::new(store.clone(), on(rec.clone()));
        let mut req = new_task("task", "alpine");
        let task = req.task.as_mut().unwrap();
        task.metadata.as_mut().unwrap().atespace = task_atespace.into();
        task.spec.as_mut().unwrap().gateway = Some(GatewayRef { name: "gw".into() });
        let result = svc.create_task(req).await;
        let (code, msg) = match &result {
            Ok(_) => (Code::Ok, String::new()),
            Err(e) => (e.code(), e.message().to_owned()),
        };
        assert_eq!(code, want_code, "{name}: {msg}");
        assert!(msg.contains(want_msg), "{name}: {msg:?}");
        let atespace = if task_atespace.is_empty() {
            "default"
        } else {
            task_atespace
        };
        let stored = store.inner.get_task(atespace, "task").await;
        if want_code != Code::Ok {
            assert!(matches!(stored, Err(StoreError::NotFound)), "{name}");
            assert_eq!(rec.counts(), (0, 0), "{name}");
            continue;
        }
        stored.unwrap();
        let gw = rec.state.lock().unwrap().gateway.clone();
        assert_eq!(
            gw.and_then(|g| g.metadata).map(|m| m.name).as_deref(),
            Some("gw"),
            "{name}"
        );
    }
}

/// Fields the API has but AX does not implement yet are refused, rather
/// than saved and ignored, unless validation rejects them first.
#[tokio::test]
async fn create_task_rejects_unimplemented_fields() {
    let store = Arc::new(MemoryStore::new());
    let svc = service(store.clone(), on(Arc::new(FakeReconciler::default())));
    let create = |name: &str, spec: TaskSpec| CreateTaskRequest {
        task: Some(Task {
            metadata: meta(name),
            spec: Some(spec),
            ..Default::default()
        }),
    };
    for (spec, want) in [
        (
            TaskSpec {
                profile: Some(Profile {
                    ate_actor_template: "user-tpl".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            "spec.profile.ateActorTemplate is not supported yet",
        ),
        (
            TaskSpec {
                retry: Some(RetryConfig {
                    max_retries: 2,
                    ..Default::default()
                }),
                ..Default::default()
            },
            "spec.retry is not supported yet",
        ),
    ] {
        let err = svc
            .create_task(create("unimpl-task", spec))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unimplemented, "{err:?}");
        assert!(err.message().contains(want), "{err:?}");
        assert!(matches!(
            store.get_task("default", "unimpl-task").await,
            Err(StoreError::NotFound)
        ));
    }
    let err = svc
        .create_task(create(
            "bad-retry",
            TaskSpec {
                retry: Some(RetryConfig {
                    max_retries: -1,
                    ..Default::default()
                }),
                ..Default::default()
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");

    // A task without a profile inherits its workspace's ateActorTemplate and
    // is refused; one with its own profile is not.
    store
        .save_workspace(&mut Workspace {
            metadata: meta("ws-tpl"),
            spec: Some(WorkspaceSpec {
                profile: Some(Profile {
                    ate_actor_template: "ws-user-tpl".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    let bound = |profile: Option<Profile>| TaskSpec {
        workspace: Some(WorkspaceRef {
            name: "ws-tpl".into(),
            ..Default::default()
        }),
        profile,
        ..Default::default()
    };
    let err = svc
        .create_task(create("inherits-ws-tpl", bound(None)))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented, "{err:?}");
    svc.create_task(create(
        "overrides-ws-profile",
        bound(Some(Profile {
            resources: Some(ResourcesSpec {
                limits: Some(ResourceList {
                    memory: "1Gi".into(),
                    ..Default::default()
                }),
            }),
            ..Default::default()
        })),
    ))
    .await
    .unwrap();
}

/// A task whose actor is still resuming or reverting reads as Queued, and
/// creating it again is refused without touching the actor.
#[tokio::test]
async fn create_task_rejects_a_resuming_task() {
    for state in [ActorState::Resuming, ActorState::Reverting] {
        let rec = Arc::new(FakeReconciler::default());
        let svc = service(Arc::new(MemoryStore::new()), on(rec.clone()));
        svc.create_task(new_task("resuming-task", "alpine"))
            .await
            .unwrap();
        rec.set(|s| {
            s.actors.insert("resuming-task".into(), state);
        });
        let got = svc.get_task(get("resuming-task")).await.unwrap();
        assert_eq!(got.status.as_ref().unwrap().state(), TaskState::Queued);
        let ready = got
            .status
            .as_ref()
            .unwrap()
            .conditions
            .iter()
            .find(|c| c.r#type == "Ready")
            .cloned()
            .unwrap_or_default();
        assert_eq!(ready.reason, reason::RESUMING, "{state:?}");
        let err = svc
            .create_task(new_task("resuming-task", "alpine"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "{state:?}");
        assert_eq!(rec.counts(), (1, 0), "{state:?}");
        assert_eq!(rec.state.lock().unwrap().actors["resuming-task"], state);
    }
}

/// The actor's UID is read live and never stored, even when a client sends
/// one.
#[tokio::test]
async fn actor_uids_are_live_and_not_stored() {
    let store = Arc::new(MemoryStore::new());
    let svc = service(store.clone(), on(Arc::new(FakeReconciler::default())));
    let mut req = new_task("identity", "alpine");
    req.task.as_mut().unwrap().status = Some(TaskStatus {
        actor_uid: "client-uid".into(),
        ..Default::default()
    });
    svc.create_task(req).await.unwrap();
    let stored_uid = || async {
        let t = store.get_task("default", "identity").await.unwrap();
        t.status.unwrap_or_default().actor_uid
    };
    assert_eq!(stored_uid().await, "");
    let got = svc.get_task(get("identity")).await.unwrap();
    assert_eq!(got.status.unwrap().actor_uid, "uid-identity");
    let list = svc
        .list_tasks(ListTasksRequest {
            atespace: "default".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        list.tasks[0].status.as_ref().unwrap().actor_uid,
        "uid-identity"
    );
    svc.resume_task(ResumeTaskRequest {
        name: "identity".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(stored_uid().await, "");
    svc.suspend_task(SuspendTaskRequest {
        name: "identity".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(stored_uid().await, "");
}

/// Workspaces are immutable: creating one again with the same spec returns
/// it unchanged, and with another spec is refused.
#[tokio::test]
async fn workspaces_are_immutable() {
    let svc = service(Arc::new(MemoryStore::new()), Default::default());
    let create = |cmd: &str| CreateWorkspaceRequest {
        workspace: Some(Workspace {
            metadata: meta("imm-ws"),
            spec: Some(WorkspaceSpec {
                setup: Some(WorkspaceSetup {
                    commands: vec![cmd.into()],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
    };
    let created = svc.create_workspace(create("echo setup")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let again = svc.create_workspace(create("echo setup")).await.unwrap();
    assert_eq!(
        again.metadata.unwrap().create_time,
        created.metadata.unwrap().create_time
    );
    let err = svc
        .create_workspace(create("echo mutated"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    assert!(err.message().contains("already exists and is immutable"));

    for (spec, want) in [
        (
            WorkspaceSpec {
                profile: Some(Profile {
                    resources: Some(ResourcesSpec {
                        limits: Some(ResourceList {
                            cpu: "1".into(),
                            memory: "512Mi".into(),
                        }),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            "spec.profile is not supported yet",
        ),
        (
            WorkspaceSpec {
                setup: Some(WorkspaceSetup {
                    gateway: Some(GatewayRef {
                        name: "build-gw".into(),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            "spec.setup.gateway is not supported yet",
        ),
    ] {
        let err = svc
            .create_workspace(CreateWorkspaceRequest {
                workspace: Some(Workspace {
                    metadata: meta("unimpl-ws"),
                    spec: Some(spec),
                    ..Default::default()
                }),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unimplemented, "{err:?}");
        assert!(err.message().contains(want), "{err:?}");
    }
}

#[tokio::test]
async fn lists_workspaces_by_page() {
    let svc = service(Arc::new(MemoryStore::new()), Default::default());
    for name in ["ws-a", "ws-b", "ws-c"] {
        svc.create_workspace(CreateWorkspaceRequest {
            workspace: Some(Workspace {
                metadata: meta(name),
                ..Default::default()
            }),
        })
        .await
        .unwrap();
    }
    for (limit, offset, want) in [
        // Most recently saved first, as both stores list.
        (2, 0, vec!["ws-c", "ws-b"]),
        (1, 1, vec!["ws-b"]),
        (10, 99, vec![]),
    ] {
        let got: Vec<_> = svc
            .list_workspaces(ListWorkspacesRequest {
                atespace: "default".into(),
                limit,
                offset,
            })
            .await
            .unwrap()
            .workspaces
            .into_iter()
            .map(|w| w.metadata.unwrap().name)
            .collect();
        assert_eq!(got, want, "limit {limit}, offset {offset}");
    }
}

#[tokio::test]
async fn gateway_crud() {
    let svc = service(Arc::new(MemoryStore::new()), Default::default());
    let gw = |host: &str| Gateway {
        metadata: meta("gw"),
        spec: Some(GatewaySpec {
            egress: vec![rax_api::substrate::ateapi::v1::EgressRule {
                https: Some(rax_api::substrate::ateapi::v1::HttpsRule {
                    hostnames: vec![host.into()],
                    ..Default::default()
                }),
                ..Default::default()
            }],
        }),
        ..Default::default()
    };
    let create = |g| CreateGatewayRequest { gateway: Some(g) };
    let update = |g| UpdateGatewayRequest { gateway: Some(g) };

    let err = svc
        .update_gateway(update(gw("a.example.com")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    let first = svc
        .create_gateway(create(gw("a.example.com")))
        .await
        .unwrap();
    let meta_of = |g: &Gateway| g.metadata.clone().unwrap();
    assert_eq!(meta_of(&first).atespace, "default");
    assert_eq!(first.kind, rax_api::KIND_GATEWAY);
    let err = svc
        .create_gateway(create(gw("a.example.com")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists, "{err}");
    assert_eq!(
        err.message(),
        r#"gateway "gw" already exists in atespace "default""#
    );

    tokio::time::sleep(Duration::from_millis(5)).await;
    let second = svc
        .update_gateway(update(gw("b.example.com")))
        .await
        .unwrap();
    assert_eq!(meta_of(&first).create_time, meta_of(&second).create_time);
    let got = svc
        .get_gateway(GetGatewayRequest {
            atespace: String::new(),
            name: "gw".into(),
        })
        .await
        .unwrap();
    assert_eq!(got, second);
    let list = svc
        .list_gateways(ListGatewaysRequest::default())
        .await
        .unwrap();
    assert_eq!(list.gateways, [second]);

    let err = svc.create_gateway(create(gw("*"))).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");
    let err = svc
        .create_gateway(CreateGatewayRequest { gateway: None })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument, "{err}");

    svc.delete_gateway(DeleteGatewayRequest {
        atespace: String::new(),
        name: "gw".into(),
    })
    .await
    .unwrap();
    let err = svc
        .get_gateway(GetGatewayRequest {
            atespace: "default".into(),
            name: "gw".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound, "{err}");
    assert_eq!(
        err.message(),
        r#"gateway "gw" not found in atespace "default""#
    );
}

#[tokio::test]
async fn concurrent_creates_of_one_task_are_serialized() {
    let rec = Arc::new(FakeReconciler::default());
    let svc = service(Arc::new(MemoryStore::new()), on(rec.clone()));
    let runs: Vec<_> = (0..8)
        .map(|_| {
            let svc = svc.clone();
            tokio::spawn(async move { svc.create_task(new_task("racy", "alpine")).await })
        })
        .collect();
    let mut ok = 0;
    for r in runs {
        match r.await.unwrap() {
            Ok(_) => ok += 1,
            Err(e) => assert_eq!(e.code(), Code::FailedPrecondition),
        }
    }
    assert_eq!(ok, 1, "exactly one create must win");
    assert_eq!(rec.counts().0, 1);
}

/// Port of AX's TestServer_SuccessClearsStoredFailure: a task whose create
/// failed reads as Failed until a suspend or resume succeeds, after which its
/// phase comes from the actor again.
#[tokio::test]
async fn success_clears_a_stored_failure() {
    let rec = Arc::new(FakeReconciler::default());
    let svc = service(Arc::new(MemoryStore::new()), on(rec.clone()));
    let create_failed = |name: &'static str| {
        let (svc, rec) = (svc.clone(), rec.clone());
        async move {
            rec.set(|s| s.err = Some(Status::internal("substrate suspend failure")));
            let err = svc.create_task(new_task(name, "alpine")).await.unwrap_err();
            assert_eq!(err.code(), Code::Internal);
            rec.set(|s| s.err = None);
            let t = svc.get_task(get(name)).await.unwrap();
            assert_eq!(state_of(&t), "Failed", "{name} after a failed create");
        }
    };

    create_failed("task-suspend").await;
    let t = svc
        .suspend_task(SuspendTaskRequest {
            name: "task-suspend".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(state_of(&t), "Suspended");

    create_failed("task-resume").await;
    let t = svc
        .resume_task(ResumeTaskRequest {
            name: "task-resume".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(state_of(&t), "Working");
    assert_eq!(
        state_of(&svc.get_task(get("task-resume")).await.unwrap()),
        "Working"
    );
}

/// A suspend after a failed delete clears its Terminating, which cancels the
/// deletion, as in AX.
#[tokio::test]
async fn suspending_cancels_a_failed_delete() {
    let rec = FakeReconciler::with(|s| s.delete_err = Some(Status::unavailable("timeout")));
    let svc = service(Arc::new(MemoryStore::new()), on(rec));
    svc.create_task(new_task("task-undelete", "alpine"))
        .await
        .unwrap();
    svc.delete_task(DeleteTaskRequest {
        name: "task-undelete".into(),
        ..Default::default()
    })
    .await
    .unwrap_err();
    assert_eq!(
        state_of(&svc.get_task(get("task-undelete")).await.unwrap()),
        "Terminating"
    );
    let t = svc
        .suspend_task(SuspendTaskRequest {
            name: "task-undelete".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(state_of(&t), "Suspended");
}
