//! The `ax.v1alpha1.AX` gRPC service.

use std::future::Future;
use std::sync::Arc;

use rax_api::v1alpha1::{
    self as api, Gateway, ObjectMeta, StatusDetail, Task, TaskSpec, TaskState, TaskStatus,
    TaskStatusReason, Workspace, WorkspaceSpec, ax_server::Ax,
};
use rax_store::{DEFAULT_ATESPACE, Locker, MemoryLocker, ResourceStore, Store, StoreError};
use tonic::{Request, Response, Status};

use crate::tasks::{Tasks, now};
use crate::{Actors, Reconciler, is_transient};

#[derive(Default, Clone)]
pub struct ServerOptions {
    /// Serializes changes to a resource. It must be shared by every server
    /// replica; it defaults to an in-process [`MemoryLocker`].
    pub locker: Option<Arc<dyn Locker>>,
    /// Drives task actors on Substrate. Without it, tasks are only stored.
    pub reconciler: Option<Arc<dyn Reconciler>>,
    /// Reads the parts of a task's status owned by its actor, which the store
    /// never holds. Without it, tasks are returned as stored.
    pub actors: Option<Arc<dyn Actors>>,
}

/// The AX API. Its methods can be called directly, or served over gRPC
/// through the [`Ax`] trait.
#[derive(Clone)]
pub struct AxService {
    tasks: Tasks,
    resources: Arc<dyn ResourceStore>,
    locker: Arc<dyn Locker>,
    reconciler: Option<Arc<dyn Reconciler>>,
}

fn atespace_or_default(atespace: &str) -> String {
    if atespace.is_empty() {
        DEFAULT_ATESPACE.into()
    } else {
        atespace.into()
    }
}

fn not_found(kind: &str, name: &str, atespace: &str) -> Status {
    Status::not_found(format!(
        "{kind} {name:?} not found in atespace {atespace:?}"
    ))
}

/// Maps a store error from a read to NotFound or Internal.
fn read_error(kind: &str, name: &str, atespace: &str) -> impl FnOnce(StoreError) -> Status {
    move |e| match e {
        StoreError::NotFound => not_found(kind, name, atespace),
        e => Status::internal(format!("getting {kind}: {e}")),
    }
}

fn internal(context: &str) -> impl FnOnce(StoreError) -> Status + '_ {
    move |e| Status::internal(format!("{context}: {e}"))
}

impl AxService {
    pub fn new(store: Arc<dyn Store>, opts: ServerOptions) -> Self {
        Self {
            tasks: Tasks::new(store.clone(), opts.actors),
            resources: store,
            locker: opts.locker.unwrap_or_else(|| Arc::new(MemoryLocker::new())),
            reconciler: opts.reconciler,
        }
    }

    /// Runs `f` holding the exclusive lock for one resource.
    async fn locked<T>(
        &self,
        kind: &str,
        atespace: &str,
        name: &str,
        f: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        let guard = self
            .locker
            .lock(kind, atespace, name)
            .await
            .map_err(|e| Status::aborted(format!("locking {kind} {atespace}/{name}: {e}")))?;
        let out = f.await;
        guard.release().await;
        out
    }

    // --- Tasks ---

    pub async fn get_task(&self, req: api::GetTaskRequest) -> Result<Task, Status> {
        let atespace = atespace_or_default(&req.atespace);
        self.tasks
            .get(&atespace, &req.name)
            .await
            .map_err(read_error("task", &req.name, &atespace))
    }

    pub async fn list_tasks(
        &self,
        req: api::ListTasksRequest,
    ) -> Result<api::ListTasksResponse, Status> {
        let limit = if req.limit > 0 {
            req.limit
        } else {
            rax_store::DEFAULT_PAGE_LIMIT
        };
        let offset = req.offset.max(0);
        let tasks = self
            .tasks
            .list(&req.atespace, limit, offset)
            .await
            .map_err(internal("listing tasks"))?;
        Ok(api::ListTasksResponse { tasks })
    }

    pub async fn create_task(&self, req: api::CreateTaskRequest) -> Result<Task, Status> {
        let mut task = req
            .task
            .ok_or_else(|| Status::invalid_argument("task required"))?;
        rax_api::validate_task(&task).map_err(|e| Status::invalid_argument(e.to_string()))?;
        // Tasks are immutable, so reject fields nothing honors yet (running
        // from an existing ActorTemplate needs pull-based runner assignment,
        // and retries need runner exit events) rather than store them.
        if let Some(field) = first_unimplemented_task_field(task.spec.as_ref()) {
            return Err(Status::unimplemented(format!(
                "{field} is not supported yet"
            )));
        }
        let meta = task.metadata.get_or_insert_with(Default::default);
        if meta.atespace.is_empty() {
            meta.atespace = DEFAULT_ATESPACE.into();
        }
        let (atespace, name) = (meta.atespace.clone(), meta.name.clone());
        self.locked(
            "task",
            &atespace,
            &name,
            self.create_task_locked(task, &atespace, &name),
        )
        .await
    }

    async fn create_task_locked(
        &self,
        mut task: Task,
        atespace: &str,
        name: &str,
    ) -> Result<Task, Status> {
        // Only an unfinished first create (stored as Queued with no template
        // yet) may be applied again, and only unchanged. The stored record is
        // checked, not the actor's state: a resuming actor also reads as
        // Queued.
        match self.tasks.get_stored(atespace, name).await {
            Ok(existing) => {
                let st = existing.status.clone().unwrap_or_default();
                let deleting = existing
                    .metadata
                    .as_ref()
                    .is_some_and(|m| m.delete_time.is_some());
                if st.state() != TaskState::Queued
                    || !st.ate_actor_template.is_empty()
                    || deleting
                    || !task_spec_matches_existing(&existing, task.spec.as_ref())
                {
                    return Err(Status::failed_precondition(format!(
                        "task {atespace}/{name} already exists and is immutable"
                    )));
                }
                task = existing;
            }
            Err(StoreError::NotFound) => {}
            Err(e) => return Err(Status::internal(format!("checking existing task: {e}"))),
        }

        // Tasks are immutable, so a task created before its Workspace would
        // keep an empty workspace for its whole life. Reject it instead.
        let spec = task.spec.clone().unwrap_or_default();
        let workspace = match spec
            .workspace
            .as_ref()
            .map(|w| w.name.as_str())
            .filter(|n| !n.is_empty())
        {
            None => None,
            Some(ws_name) => match self.resources.get_workspace(atespace, ws_name).await {
                Ok(ws) => Some(ws),
                Err(StoreError::NotFound) => {
                    return Err(Status::failed_precondition(format!(
                        "workspace {ws_name:?} not found in atespace {atespace:?}"
                    )));
                }
                Err(e) => {
                    return Err(Status::internal(format!(
                        "getting workspace {ws_name:?}: {e}"
                    )));
                }
            },
        };
        // A task gets its Gateway's rules when it is created, so the Gateway
        // must already exist.
        let gateway = match spec
            .gateway
            .as_ref()
            .map(|g| g.name.as_str())
            .filter(|n| !n.is_empty())
        {
            None => None,
            Some(gw_name) => match self.resources.get_gateway(atespace, gw_name).await {
                Ok(gw) => Some(gw),
                Err(StoreError::NotFound) => {
                    return Err(Status::failed_precondition(format!(
                        "gateway {gw_name:?} not found in atespace {atespace:?}"
                    )));
                }
                Err(e) => {
                    return Err(Status::internal(format!(
                        "getting gateway {gw_name:?}: {e}"
                    )));
                }
            },
        };
        let effective = crate::profile::effective_profile(
            workspace
                .as_ref()
                .and_then(|w| w.spec.as_ref())
                .and_then(|s| s.profile.as_ref()),
            spec.profile.as_ref(),
        );
        if !effective.ate_actor_template.is_empty() {
            return Err(Status::unimplemented(
                "spec.profile.ateActorTemplate is not supported yet",
            ));
        }

        let stamp = now();
        let meta = task.metadata.as_mut().expect("metadata was defaulted");
        if meta.create_time.is_none() {
            meta.create_time = Some(stamp);
        }
        if meta.uid.is_empty() {
            meta.uid = new_uid();
        }
        meta.update_time = Some(stamp);
        let uid = meta.uid.clone();
        let spec = task.spec.get_or_insert_with(TaskSpec::default);
        if spec.session_id.is_empty() {
            spec.session_id = uid;
        }
        let commands = spec.commands.clone();
        let st = task.status.get_or_insert_with(TaskStatus::default);
        if st.attempt == 0 {
            st.attempt = 1;
        }
        st.generation += 1;
        if !commands.is_empty() {
            st.resolved_argv = commands;
        }
        // The task is Queued until its actor exists.
        st.set_state(TaskState::Queued);
        self.tasks
            .save(&task)
            .await
            .map_err(internal("saving task"))?;

        // New tasks start suspended.
        if let Some(r) = &self.reconciler
            && let Err(e) = r.suspend(&mut task, workspace, gateway).await
        {
            return Err(self.record_failure(&mut task, "provisioning", &e).await);
        }
        self.clear_stored_state(&mut task).await?;
        if self.reconciler.is_none() {
            task.status
                .get_or_insert_with(Default::default)
                .set_state(TaskState::Suspended);
        }

        // Report the actor as it is now, as GetTask would.
        self.tasks.apply_actor(&mut task).await;
        Ok(task)
    }

    /// Hands a task's state back to its actor after a create, suspend or
    /// resume succeeds: it clears a deleteTime left by an earlier failed
    /// delete (which cancels that deletion) and the stored state and status
    /// detail, so reads take them from the actor, and stores the status with
    /// a new generation.
    async fn clear_stored_state(&self, task: &mut Task) -> Result<(), Status> {
        let meta = task.metadata.get_or_insert_with(Default::default);
        if meta.atespace.is_empty() {
            meta.atespace = DEFAULT_ATESPACE.into();
        }
        let (atespace, name) = (meta.atespace.clone(), meta.name.clone());
        if meta.delete_time.is_some() {
            self.tasks
                .set_delete_time(&atespace, &name, None)
                .await
                .map_err(internal("clearing task delete_time"))?;
            meta.delete_time = None;
        }
        let st = task.status.get_or_insert_with(TaskStatus::default);
        st.set_state(TaskState::Unspecified);
        st.status_detail = None;
        st.generation += 1;
        self.tasks
            .update(&atespace, &name, task.status.as_ref())
            .await
            .map_err(internal("updating task status"))
    }

    /// Stores the status of a task that could not be provisioned or resumed:
    /// state Queued if `err` is a transient Substrate error, which a retry
    /// may fix, and Failed with the reason otherwise. Returns the error for
    /// `action`.
    async fn record_failure(&self, task: &mut Task, action: &str, err: &Status) -> Status {
        let meta = task.metadata.clone().unwrap_or_default();
        tracing::error!(
            task = %format!("{}/{}", meta.atespace, meta.name),
            error = %err.message(),
            "{action} task on substrate failed"
        );
        let st = task.status.get_or_insert_with(TaskStatus::default);
        if is_transient(err) {
            st.set_state(TaskState::Queued);
            st.status_detail = None;
        } else {
            st.set_state(TaskState::Failed);
            st.status_detail = Some(StatusDetail {
                reason: TaskStatusReason::ProvisioningFailed.into(),
                detail: err.message().to_owned(),
            });
        }
        st.generation += 1;
        if let Err(e) = self
            .tasks
            .update(&meta.atespace, &meta.name, task.status.as_ref())
            .await
        {
            tracing::warn!(error = %e, "recording task failure");
        }
        substrate_error(&format!("{action} task on substrate"), err)
    }

    pub async fn delete_task(
        &self,
        req: api::DeleteTaskRequest,
    ) -> Result<api::DeleteTaskResponse, Status> {
        let atespace = atespace_or_default(&req.atespace);
        let name = req.name;
        self.locked("task", &atespace, &name, async {
            let mut task = self
                .tasks
                .get(&atespace, &name)
                .await
                .map_err(read_error("task", &name, &atespace))?;
            // Mark the task with a delete time while actor cleanup is in
            // progress.
            let stamp = now();
            self.tasks
                .set_delete_time(&atespace, &name, Some(stamp))
                .await
                .map_err(internal("updating task delete_time"))?;
            let meta = task.metadata.get_or_insert_with(|| ObjectMeta {
                atespace: atespace.clone(),
                name: name.clone(),
                ..Default::default()
            });
            meta.delete_time = Some(stamp);
            self.tasks.delete(self.reconciler.as_deref(), &task).await?;
            Ok(api::DeleteTaskResponse {})
        })
        .await
    }

    pub async fn suspend_task(&self, req: api::SuspendTaskRequest) -> Result<Task, Status> {
        let atespace = atespace_or_default(&req.atespace);
        let name = req.name;
        self.locked("task", &atespace, &name, async {
            let mut task = self
                .tasks
                .get(&atespace, &name)
                .await
                .map_err(read_error("task", &name, &atespace))?;
            if let Some(r) = &self.reconciler {
                let ws = self.fetch_workspace(&atespace, &task).await;
                let gw = self.fetch_gateway(&atespace, &task).await;
                // Store nothing on failure: the actor keeps its state on
                // Substrate, and a stored Queued or Failed would hide it.
                r.suspend(&mut task, ws, gw)
                    .await
                    .map_err(|e| substrate_error("suspending task on substrate", &e))?;
            }
            self.clear_stored_state(&mut task).await?;
            if self.reconciler.is_none() {
                task.status
                    .get_or_insert_with(Default::default)
                    .set_state(TaskState::Suspended);
            }
            self.tasks.apply_actor(&mut task).await;
            Ok(task)
        })
        .await
    }

    pub async fn resume_task(&self, req: api::ResumeTaskRequest) -> Result<Task, Status> {
        let atespace = atespace_or_default(&req.atespace);
        let name = req.name;
        self.locked("task", &atespace, &name, async {
            let mut task = self
                .tasks
                .get(&atespace, &name)
                .await
                .map_err(read_error("task", &name, &atespace))?;
            if let Some(r) = &self.reconciler {
                let ws = self.fetch_workspace(&atespace, &task).await;
                let gw = self.fetch_gateway(&atespace, &task).await;
                if let Err(e) = r.resume(&mut task, ws, gw).await {
                    return Err(self.record_failure(&mut task, "resuming", &e).await);
                }
            }
            self.clear_stored_state(&mut task).await?;
            if self.reconciler.is_none() {
                task.status
                    .get_or_insert_with(Default::default)
                    .set_state(TaskState::Working);
            }
            self.tasks.apply_actor(&mut task).await;
            Ok(task)
        })
        .await
    }

    /// Loads the Workspace the task binds, or `None` when it binds none or the
    /// Workspace does not exist.
    async fn fetch_workspace(&self, atespace: &str, task: &Task) -> Option<Workspace> {
        let name = task.spec.as_ref()?.workspace.as_ref()?.name.as_str();
        if name.is_empty() {
            return None;
        }
        self.resources.get_workspace(atespace, name).await.ok()
    }

    /// Loads the Gateway the task binds, or `None` when it binds none or the
    /// Gateway cannot be loaded.
    async fn fetch_gateway(&self, atespace: &str, task: &Task) -> Option<Gateway> {
        let name = task.spec.as_ref()?.gateway.as_ref()?.name.as_str();
        if name.is_empty() {
            return None;
        }
        self.resources.get_gateway(atespace, name).await.ok()
    }

    // --- Workspaces ---

    pub async fn get_workspace(&self, req: api::GetWorkspaceRequest) -> Result<Workspace, Status> {
        let atespace = atespace_or_default(&req.atespace);
        self.resources
            .get_workspace(&atespace, &req.name)
            .await
            .map_err(read_error("workspace", &req.name, &atespace))
    }

    pub async fn list_workspaces(
        &self,
        req: api::ListWorkspacesRequest,
    ) -> Result<api::ListWorkspacesResponse, Status> {
        let workspaces = self
            .resources
            .list_workspaces(&req.atespace, req.limit, req.offset.max(0))
            .await
            .map_err(internal("listing workspaces"))?;
        Ok(api::ListWorkspacesResponse { workspaces })
    }

    /// Creates a workspace. Workspaces are immutable: creating one again is
    /// fine only with the same spec, and returns the stored one.
    pub async fn create_workspace(
        &self,
        req: api::CreateWorkspaceRequest,
    ) -> Result<Workspace, Status> {
        let mut ws = req
            .workspace
            .ok_or_else(|| Status::invalid_argument("workspace required"))?;
        rax_api::validate_workspace(&ws).map_err(|e| Status::invalid_argument(e.to_string()))?;
        // A workspace saved now would keep a profile or gateway binding that
        // nothing enforces yet.
        if let Some(field) = first_unimplemented_workspace_field(ws.spec.as_ref()) {
            return Err(Status::unimplemented(format!(
                "{field} is not supported yet"
            )));
        }
        ws.status = None;
        let meta = default_metadata(ws.metadata.take(), |_, _| async { None }).await;
        let (atespace, name) = (meta.atespace.clone(), meta.name.clone());
        ws.metadata = Some(meta);
        self.locked("workspace", &atespace, &name, async {
            match self.resources.get_workspace(&atespace, &name).await {
                Ok(existing) => {
                    let spec = |w: &Workspace| w.spec.clone().unwrap_or_default();
                    if spec(&existing) != spec(&ws) {
                        return Err(Status::failed_precondition(format!(
                            "workspace {atespace}/{name} already exists and is immutable"
                        )));
                    }
                    return Ok(existing);
                }
                Err(StoreError::NotFound) => {}
                Err(e) => {
                    return Err(Status::internal(format!(
                        "checking existing workspace: {e}"
                    )));
                }
            }
            self.resources
                .save_workspace(&mut ws)
                .await
                .map_err(internal("saving workspace"))?;
            Ok(ws)
        })
        .await
    }

    pub async fn delete_workspace(
        &self,
        req: api::DeleteWorkspaceRequest,
    ) -> Result<api::DeleteWorkspaceResponse, Status> {
        let atespace = atespace_or_default(&req.atespace);
        self.locked("workspace", &atespace, &req.name, async {
            self.resources
                .delete_workspace(&atespace, &req.name)
                .await
                .map_err(internal("deleting workspace"))?;
            Ok(api::DeleteWorkspaceResponse {})
        })
        .await
    }

    // --- Gateways ---

    pub async fn get_gateway(&self, req: api::GetGatewayRequest) -> Result<Gateway, Status> {
        let atespace = atespace_or_default(&req.atespace);
        self.resources
            .get_gateway(&atespace, &req.name)
            .await
            .map_err(read_error("gateway", &req.name, &atespace))
    }

    pub async fn list_gateways(
        &self,
        req: api::ListGatewaysRequest,
    ) -> Result<api::ListGatewaysResponse, Status> {
        let gateways = self
            .resources
            .list_gateways(&req.atespace)
            .await
            .map_err(internal("listing gateways"))?;
        Ok(api::ListGatewaysResponse { gateways })
    }

    pub async fn create_gateway(&self, req: api::CreateGatewayRequest) -> Result<Gateway, Status> {
        let gw = req
            .gateway
            .ok_or_else(|| Status::invalid_argument("gateway required"))?;
        self.save_gateway(gw, true).await
    }

    pub async fn update_gateway(&self, req: api::UpdateGatewayRequest) -> Result<Gateway, Status> {
        let gw = req
            .gateway
            .ok_or_else(|| Status::invalid_argument("gateway required"))?;
        self.save_gateway(gw, false).await
    }

    /// Validates and saves a gateway, which must be new when `create` is set
    /// and must exist otherwise.
    async fn save_gateway(&self, mut gw: Gateway, create: bool) -> Result<Gateway, Status> {
        rax_api::validate_gateway(&gw).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let meta = gw.metadata.as_ref();
        let atespace = atespace_or_default(meta.map_or("", |m| m.atespace.as_str()));
        let name = meta.map(|m| m.name.clone()).unwrap_or_default();
        self.locked("gateway", &atespace, &name, async {
            let existing = match self.resources.get_gateway(&atespace, &name).await {
                Ok(g) => Some(g),
                Err(StoreError::NotFound) => None,
                Err(e) => return Err(Status::internal(format!("getting gateway: {e}"))),
            };
            match (create, &existing) {
                (true, Some(_)) => {
                    return Err(Status::already_exists(format!(
                        "gateway {name:?} already exists in atespace {atespace:?}"
                    )));
                }
                (false, None) => return Err(not_found("gateway", &name, &atespace)),
                _ => {}
            }
            let prev = existing.and_then(|g| g.metadata);
            gw.metadata = Some(default_metadata(gw.metadata.take(), |_, _| async { prev }).await);
            self.resources
                .save_gateway(&mut gw)
                .await
                .map_err(internal("saving gateway"))?;
            Ok(gw)
        })
        .await
    }

    pub async fn delete_gateway(
        &self,
        req: api::DeleteGatewayRequest,
    ) -> Result<api::DeleteGatewayResponse, Status> {
        let atespace = atespace_or_default(&req.atespace);
        self.locked("gateway", &atespace, &req.name, async {
            self.resources
                .delete_gateway(&atespace, &req.name)
                .await
                .map_err(internal("deleting gateway"))?;
            Ok(api::DeleteGatewayResponse {})
        })
        .await
    }

    // --- Models ---

    pub async fn get_model(&self, req: api::GetModelRequest) -> Result<api::Model, Status> {
        let atespace = atespace_or_default(&req.atespace);
        self.resources
            .get_model(&atespace, &req.name)
            .await
            .map_err(read_error("model", &req.name, &atespace))
    }

    pub async fn list_models(
        &self,
        req: api::ListModelsRequest,
    ) -> Result<api::ListModelsResponse, Status> {
        let models = self
            .resources
            .list_models(&req.atespace)
            .await
            .map_err(internal("listing models"))?;
        Ok(api::ListModelsResponse { models })
    }

    pub async fn update_model(&self, req: api::UpdateModelRequest) -> Result<api::Model, Status> {
        let mut model = req
            .model
            .ok_or_else(|| Status::invalid_argument("model required"))?;
        rax_api::validate_model(&model).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let res = self.resources.clone();
        let meta = default_metadata(model.metadata.take(), |a, n| async move {
            res.get_model(&a, &n).await.ok().and_then(|m| m.metadata)
        })
        .await;
        let (atespace, name) = (meta.atespace.clone(), meta.name.clone());
        model.metadata = Some(meta);
        self.locked("model", &atespace, &name, async {
            self.resources
                .save_model(&mut model)
                .await
                .map_err(internal("saving model"))?;
            Ok(model)
        })
        .await
    }

    pub async fn delete_model(
        &self,
        req: api::DeleteModelRequest,
    ) -> Result<api::DeleteModelResponse, Status> {
        let atespace = atespace_or_default(&req.atespace);
        self.locked("model", &atespace, &req.name, async {
            self.resources
                .delete_model(&atespace, &req.name)
                .await
                .map_err(internal("deleting model"))?;
            Ok(api::DeleteModelResponse {})
        })
        .await
    }
}

/// A random (version 4) UUID, as metadata.uid holds.
fn new_uid() -> String {
    let mut b: [u8; 16] = rand::random();
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h = hex::encode(b);
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

/// The first task field that is accepted but not honored yet, if the spec
/// sets one.
fn first_unimplemented_task_field(spec: Option<&TaskSpec>) -> Option<&'static str> {
    let spec = spec?;
    if spec
        .profile
        .as_ref()
        .is_some_and(|p| !p.ate_actor_template.is_empty())
    {
        return Some("spec.profile.ateActorTemplate");
    }
    if spec.retry.is_some() {
        return Some("spec.retry");
    }
    None
}

/// The first workspace field that is accepted but not honored yet: profiles
/// wait for snapshot pre-warming, and a setup gateway for a setup actor.
fn first_unimplemented_workspace_field(spec: Option<&WorkspaceSpec>) -> Option<&'static str> {
    let spec = spec?;
    if spec.profile.is_some() {
        return Some("spec.profile");
    }
    if spec.setup.as_ref().is_some_and(|s| s.gateway.is_some()) {
        return Some("spec.setup.gateway");
    }
    None
}

/// Compares a spec applied again with an existing Queued task's, allowing
/// for sessionId having defaulted to metadata.uid on the first create.
fn task_spec_matches_existing(existing: &Task, incoming: Option<&TaskSpec>) -> bool {
    let mut want = incoming.cloned().unwrap_or_default();
    let uid = existing.metadata.as_ref().map_or("", |m| m.uid.as_str());
    if want.session_id.is_empty() && !uid.is_empty() {
        want.session_id = uid.to_owned();
    }
    existing.spec.clone().unwrap_or_default() == want
}

/// Maps a Substrate error to the status the caller sees: Unavailable when it
/// is worth retrying, Internal otherwise.
fn substrate_error(context: &str, e: &Status) -> Status {
    let msg = format!("{context}: {}", e.message());
    if is_transient(e) {
        Status::unavailable(msg)
    } else {
        Status::internal(msg)
    }
}

/// Defaults the atespace and keeps the creation time of the resource being
/// replaced, or stamps a new one.
async fn default_metadata<F, Fut>(meta: Option<ObjectMeta>, existing: F) -> ObjectMeta
where
    F: FnOnce(String, String) -> Fut,
    Fut: Future<Output = Option<ObjectMeta>>,
{
    let mut meta = meta.unwrap_or_default();
    if meta.atespace.is_empty() {
        meta.atespace = DEFAULT_ATESPACE.into();
    }
    if meta.create_time.is_none() {
        let prev = existing(meta.atespace.clone(), meta.name.clone()).await;
        meta.create_time = Some(prev.and_then(|p| p.create_time).unwrap_or_else(now));
    }
    meta
}

/// Serves one AX RPC in a server span, measuring it, as AX's otelgrpc stats
/// handler does for the methods it registers.
async fn rpc<Req, T>(
    method: &str,
    req: &Request<Req>,
    handler: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    rax_otelenv::rpc::server_call(&format!("ax.v1alpha1.AX/{method}"), req.metadata(), handler)
        .await
}

#[tonic::async_trait]
impl Ax for AxService {
    async fn get_task(&self, r: Request<api::GetTaskRequest>) -> Result<Response<Task>, Status> {
        rpc("GetTask", &r, self.get_task(r.get_ref().clone()))
            .await
            .map(Response::new)
    }
    async fn list_tasks(
        &self,
        r: Request<api::ListTasksRequest>,
    ) -> Result<Response<api::ListTasksResponse>, Status> {
        rpc("ListTasks", &r, self.list_tasks(r.get_ref().clone()))
            .await
            .map(Response::new)
    }
    async fn create_task(
        &self,
        r: Request<api::CreateTaskRequest>,
    ) -> Result<Response<Task>, Status> {
        rpc("CreateTask", &r, self.create_task(r.get_ref().clone()))
            .await
            .map(Response::new)
    }
    async fn delete_task(
        &self,
        r: Request<api::DeleteTaskRequest>,
    ) -> Result<Response<api::DeleteTaskResponse>, Status> {
        rpc("DeleteTask", &r, self.delete_task(r.get_ref().clone()))
            .await
            .map(Response::new)
    }
    async fn suspend_task(
        &self,
        r: Request<api::SuspendTaskRequest>,
    ) -> Result<Response<Task>, Status> {
        rpc("SuspendTask", &r, self.suspend_task(r.get_ref().clone()))
            .await
            .map(Response::new)
    }
    async fn resume_task(
        &self,
        r: Request<api::ResumeTaskRequest>,
    ) -> Result<Response<Task>, Status> {
        rpc("ResumeTask", &r, self.resume_task(r.get_ref().clone()))
            .await
            .map(Response::new)
    }
    async fn get_workspace(
        &self,
        r: Request<api::GetWorkspaceRequest>,
    ) -> Result<Response<Workspace>, Status> {
        rpc("GetWorkspace", &r, self.get_workspace(r.get_ref().clone()))
            .await
            .map(Response::new)
    }
    async fn list_workspaces(
        &self,
        r: Request<api::ListWorkspacesRequest>,
    ) -> Result<Response<api::ListWorkspacesResponse>, Status> {
        rpc(
            "ListWorkspaces",
            &r,
            self.list_workspaces(r.get_ref().clone()),
        )
        .await
        .map(Response::new)
    }
    async fn create_workspace(
        &self,
        r: Request<api::CreateWorkspaceRequest>,
    ) -> Result<Response<Workspace>, Status> {
        rpc(
            "CreateWorkspace",
            &r,
            self.create_workspace(r.get_ref().clone()),
        )
        .await
        .map(Response::new)
    }
    async fn delete_workspace(
        &self,
        r: Request<api::DeleteWorkspaceRequest>,
    ) -> Result<Response<api::DeleteWorkspaceResponse>, Status> {
        rpc(
            "DeleteWorkspace",
            &r,
            self.delete_workspace(r.get_ref().clone()),
        )
        .await
        .map(Response::new)
    }
    async fn get_gateway(
        &self,
        r: Request<api::GetGatewayRequest>,
    ) -> Result<Response<Gateway>, Status> {
        rpc("GetGateway", &r, self.get_gateway(r.get_ref().clone()))
            .await
            .map(Response::new)
    }
    async fn list_gateways(
        &self,
        r: Request<api::ListGatewaysRequest>,
    ) -> Result<Response<api::ListGatewaysResponse>, Status> {
        rpc("ListGateways", &r, self.list_gateways(r.get_ref().clone()))
            .await
            .map(Response::new)
    }
    async fn create_gateway(
        &self,
        r: Request<api::CreateGatewayRequest>,
    ) -> Result<Response<Gateway>, Status> {
        rpc(
            "CreateGateway",
            &r,
            self.create_gateway(r.get_ref().clone()),
        )
        .await
        .map(Response::new)
    }
    async fn update_gateway(
        &self,
        r: Request<api::UpdateGatewayRequest>,
    ) -> Result<Response<Gateway>, Status> {
        rpc(
            "UpdateGateway",
            &r,
            self.update_gateway(r.get_ref().clone()),
        )
        .await
        .map(Response::new)
    }
    async fn delete_gateway(
        &self,
        r: Request<api::DeleteGatewayRequest>,
    ) -> Result<Response<api::DeleteGatewayResponse>, Status> {
        rpc(
            "DeleteGateway",
            &r,
            self.delete_gateway(r.get_ref().clone()),
        )
        .await
        .map(Response::new)
    }
    async fn get_model(
        &self,
        r: Request<api::GetModelRequest>,
    ) -> Result<Response<api::Model>, Status> {
        rpc("GetModel", &r, self.get_model(r.get_ref().clone()))
            .await
            .map(Response::new)
    }
    async fn list_models(
        &self,
        r: Request<api::ListModelsRequest>,
    ) -> Result<Response<api::ListModelsResponse>, Status> {
        rpc("ListModels", &r, self.list_models(r.get_ref().clone()))
            .await
            .map(Response::new)
    }
    async fn update_model(
        &self,
        r: Request<api::UpdateModelRequest>,
    ) -> Result<Response<api::Model>, Status> {
        rpc("UpdateModel", &r, self.update_model(r.get_ref().clone()))
            .await
            .map(Response::new)
    }
    async fn delete_model(
        &self,
        r: Request<api::DeleteModelRequest>,
    ) -> Result<Response<api::DeleteModelResponse>, Status> {
        rpc("DeleteModel", &r, self.delete_model(r.get_ref().clone()))
            .await
            .map(Response::new)
    }
}
