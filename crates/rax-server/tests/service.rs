//! The AX service against an in-memory store, with Substrate stood in for by
//! fake reconcilers and actor readers.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rax_api::v1alpha1::{self as api, *};
use rax_api::{phase, reason};
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
                image: image.into(),
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

fn phase_of(t: &Task) -> &str {
    t.status.as_ref().map_or("", |s| s.phase.as_str())
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
    async fn suspend(&self, task: &mut Task, _ws: Option<Workspace>) -> Result<(), Status> {
        self.drive(task, ActorState::Suspended)
    }

    async fn resume(&self, task: &mut Task, _ws: Option<Workspace>) -> Result<(), Status> {
        self.drive(task, ActorState::Running)
    }

    async fn delete(&self, _atespace: &str, name: &str) -> Result<(), Status> {
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
        st.phase.is_empty() && st.actor.is_empty() && st.worker_ip.is_empty(),
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
    c.update_workspace(UpdateWorkspaceRequest {
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
    task.spec.as_mut().unwrap().image = "ghcr.io/test/updated-image".into();
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
    assert_eq!(phase_of(t.get_ref()), "Suspended");
    let t = c
        .resume_task(ResumeTaskRequest {
            atespace: "default".into(),
            name: "grpc-task".into(),
        })
        .await
        .unwrap();
    assert_eq!(phase_of(t.get_ref()), "Running");

    let wss = c
        .list_workspaces(ListWorkspacesRequest {
            atespace: "default".into(),
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
            .update_workspace(UpdateWorkspaceRequest {
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
    svc.update_workspace(UpdateWorkspaceRequest {
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
    assert_eq!(phase_of(&t), "Suspended");

    let t = svc
        .resume_task(ResumeTaskRequest {
            name: "task-rec".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(rec.counts(), (2, 0));
    assert_eq!(phase_of(&t), "Running");

    let t = svc
        .suspend_task(SuspendTaskRequest {
            name: "task-rec".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(rec.counts(), (3, 0));
    assert_eq!(phase_of(&t), "Suspended");

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
                *observed.lock().unwrap() = t.map(|t| phase_of(&t).to_owned()).unwrap_or_default();
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
    assert_eq!(*observed.lock().unwrap(), phase::TERMINATING);
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
    assert_eq!(phase_of(&t), phase::TERMINATING);
}

/// A store whose status updates fail on demand.
#[derive(Default)]
struct FailingStore {
    inner: MemoryStore,
    fail_updates: AtomicBool,
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
        self.inner.get_workspace(a, n).await
    }
    async fn list_workspaces(&self, a: &str) -> rax_store::Result<Vec<Workspace>> {
        self.inner.list_workspaces(a).await
    }
    async fn delete_workspace(&self, a: &str, n: &str) -> rax_store::Result<()> {
        self.inner.delete_workspace(a, n).await
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
            image: "alpine".into(),
            ..Default::default()
        }),
        status: Some(TaskStatus {
            phase: "Running".into(),
            actor: name.into(),
            worker_ip: "10.244.1.42".into(),
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
    assert_eq!(st.phase, "Running");
    assert_eq!(st.worker_ip, "10.244.1.42");
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
    assert_eq!(phase_of(&t), "Suspended");
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
        (st.phase.as_str(), st.worker_ip.as_str(), st.actor.as_str()),
        ("Running", "10.244.1.7", "observed-a")
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
        assert_eq!(st.phase, "Running");
        assert_eq!(st.actor, t.metadata.as_ref().unwrap().name);
    }

    // A change on Substrate alone is visible on the next read.
    *actors.state.lock().unwrap() = ActorState::Suspended;
    assert_eq!(
        phase_of(&svc.get_task(get("observed-a")).await.unwrap()),
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
    assert_eq!(phase_of(&t), "Suspended");
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
        phase_of(&svc.get_task(get("task-create-fail")).await.unwrap()),
        phase::FAILED
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
    assert_eq!(phase_of(&stored), phase::PENDING);

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
        phase: "Running".into(),
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
    assert_eq!(phase_of(&created), "Suspended");
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
        phase_of(&svc.get_task(get("simple-task")).await.unwrap()),
        phase::PENDING
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
            assert_eq!(phase_of(&t), phase::FAILED, "{name} after a failed create");
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
    assert_eq!(phase_of(&t), "Suspended");

    create_failed("task-resume").await;
    let t = svc
        .resume_task(ResumeTaskRequest {
            name: "task-resume".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(phase_of(&t), "Running");
    assert_eq!(
        phase_of(&svc.get_task(get("task-resume")).await.unwrap()),
        "Running"
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
        phase_of(&svc.get_task(get("task-undelete")).await.unwrap()),
        phase::TERMINATING
    );
    let t = svc
        .suspend_task(SuspendTaskRequest {
            name: "task-undelete".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(phase_of(&t), "Suspended");
}
