//! Task storage and Substrate.
//!
//! The store keeps only what AX itself knows about a task. Its actor, the
//! actor's UID, and the state and Ready condition that follow from the
//! actor's state are read from Substrate whenever a task is fetched.
//!
//! The service reaches task records only through [`Tasks`], which drops the
//! actor-owned status before writing and fills it in after reading, so no
//! handler can skip that split.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use pbjson_types::Timestamp;
use rax_api::v1alpha1::{Condition, StatusDetail, Task, TaskState, TaskStatus, TaskStatusReason};
use rax_api::{condition, is_ax_task_state, reason};
use rax_store::{Result, TaskStore};
use tonic::Status;

use crate::{Actor, ActorState, Actors, Reconciler};

pub(crate) fn now() -> Timestamp {
    let d = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}

#[derive(Clone)]
pub(crate) struct Tasks {
    store: Arc<dyn TaskStore>,
    actors: Option<Arc<dyn Actors>>,
}

impl Tasks {
    pub fn new(store: Arc<dyn TaskStore>, actors: Option<Arc<dyn Actors>>) -> Self {
        Self { store, actors }
    }

    /// Returns a task with its Substrate-owned status filled in.
    pub async fn get(&self, atespace: &str, name: &str) -> Result<Task> {
        let mut task = self.store.get_task(atespace, name).await?;
        self.apply_actor(&mut task).await;
        Ok(task)
    }

    /// Returns a page of tasks with their Substrate-owned status filled in,
    /// using one ListActors call per atespace.
    pub async fn list(&self, atespace: &str, limit: i64, offset: i64) -> Result<Vec<Task>> {
        let mut tasks = self.store.list_tasks(atespace, limit, offset).await?;
        let Some(actors) = &self.actors else {
            return Ok(tasks);
        };
        let mut by_atespace: HashMap<String, Vec<&mut Task>> = HashMap::new();
        for t in &mut tasks {
            let ns = t
                .metadata
                .as_ref()
                .map(|m| m.atespace.clone())
                .unwrap_or_default();
            by_atespace.entry(ns).or_default().push(t);
        }
        for (ns, group) in by_atespace {
            match actors.list_actors(&ns).await {
                Ok(list) => {
                    let by_name: HashMap<_, _> =
                        list.iter().map(|a| (a.name.as_str(), a)).collect();
                    for t in group {
                        let absent = actors.absent_actor(task_name(t));
                        let a = by_name.get(task_name(t)).copied().or(absent.as_ref());
                        set_actor_status(t, Ok(a));
                    }
                }
                Err(e) => group.into_iter().for_each(|t| set_actor_status(t, Err(&e))),
            }
        }
        Ok(tasks)
    }

    /// Stores a task without its Substrate-owned status.
    pub async fn save(&self, task: &Task) -> Result<()> {
        let mut stored = task.clone();
        stored.status = Some(stored_status(task.status.as_ref()));
        self.store.save_task(&mut stored).await
    }

    /// Stores a task's status without its Substrate-owned parts.
    pub async fn update(
        &self,
        atespace: &str,
        name: &str,
        status: Option<&TaskStatus>,
    ) -> Result<()> {
        self.store
            .update_task_status(atespace, name, stored_status(status))
            .await
    }

    /// Sets or clears metadata.deleteTime on the task's record.
    pub async fn set_delete_time(
        &self,
        atespace: &str,
        name: &str,
        delete_time: Option<Timestamp>,
    ) -> Result<()> {
        self.store
            .set_task_delete_time(atespace, name, delete_time)
            .await
    }

    /// Returns a task as stored, without its Substrate-owned status.
    pub async fn get_stored(&self, atespace: &str, name: &str) -> Result<Task> {
        self.store.get_task(atespace, name).await
    }

    /// Removes a task's actor and templates from Substrate, then its record
    /// from the store.
    pub async fn delete(
        &self,
        reconciler: Option<&dyn Reconciler>,
        task: &Task,
    ) -> Result<(), Status> {
        let meta = task.metadata.clone().unwrap_or_default();
        let (atespace, name) = (meta.atespace.as_str(), meta.name.as_str());
        if let Some(r) = reconciler
            && let Err(e) = r.delete(task).await
        {
            return Err(match e.code() {
                tonic::Code::Unknown => Status::internal(format!(
                    "cleaning up substrate resources for task {atespace}/{name}: {}",
                    e.message()
                )),
                _ => e,
            });
        }
        self.store
            .delete_task(atespace, name)
            .await
            .map_err(|e| Status::internal(format!("deleting task record: {e}")))
    }

    /// Fills in the Substrate-owned status of `task` from its actor, when the
    /// server is connected to Substrate.
    pub async fn apply_actor(&self, task: &mut Task) {
        let Some(actors) = &self.actors else { return };
        let atespace = task
            .metadata
            .as_ref()
            .map(|m| m.atespace.clone())
            .unwrap_or_default();
        let name = task_name(task).to_owned();
        let res = actors
            .get_actor(&atespace, &name)
            .await
            .map(|a| a.or_else(|| actors.absent_actor(&name)));
        set_actor_status(task, res.as_ref().map(|a| a.as_ref()));
    }
}

fn task_name(t: &Task) -> &str {
    t.metadata.as_ref().map_or("", |m| m.name.as_str())
}

/// Overlays the actor's state on the task's status: actor name, actor UID,
/// and, unless the state is one of AX's own or deletion is in progress, the
/// state and Ready condition. `actor` is `Ok(None)` when the task has no
/// actor, and an error when the actor could not be looked up, in which case
/// Ready is reported as Unknown.
pub(crate) fn set_actor_status(task: &mut Task, actor: Result<Option<&Actor>, &Status>) {
    let terminating = task
        .metadata
        .as_ref()
        .is_some_and(|m| m.delete_time.is_some());
    let name = task_name_of(task);
    let st = task.status.get_or_insert_with(Default::default);
    st.actor.clear();
    st.actor_uid.clear();
    let ax_state = is_ax_task_state(st.state());
    let deleting =
        |st: &mut TaskStatus| set_ready(st, "False", reason::TERMINATING, "Task is being deleted");

    match actor {
        Err(e) => {
            tracing::warn!(task = name, error = %e.message(), "could not read task actor");
            if terminating {
                return deleting(st);
            }
            if !ax_state {
                st.set_state(TaskState::Unspecified);
                set_ready(st, "Unknown", reason::SUBSTRATE_UNAVAILABLE, e.message());
            }
        }
        // A task that finished provisioning always has an actor, so a missing
        // one was removed outside AX.
        Ok(None) => {
            if terminating {
                return deleting(st);
            }
            if !ax_state {
                st.set_state(TaskState::Failed);
                set_ready(
                    st,
                    "False",
                    reason::ACTOR_NOT_FOUND,
                    "The task's actor no longer exists on Substrate",
                );
            }
        }
        Ok(Some(actor)) => {
            st.actor = actor.name.clone();
            st.actor_uid = actor.uid.clone();
            if terminating {
                if !ax_state {
                    st.set_state(actor_task_state(actor.state).0);
                }
                return deleting(st);
            }
            if ax_state {
                return;
            }
            let (state, ready_reason) = actor_task_state(actor.state);
            st.set_state(state);
            match ready_reason {
                reason::TASK_RUNNING => {
                    if condition_true(st, condition::WORKSPACE_READY) {
                        set_ready(
                            st,
                            "True",
                            reason::TASK_RUNNING,
                            "Task is running and its workspace is ready",
                        );
                    } else {
                        set_ready(
                            st,
                            "False",
                            reason::WORKSPACE_INITIALIZING,
                            "Waiting for workspace setup to complete",
                        );
                    }
                }
                reason::UNKNOWN => {
                    set_ready(
                        st,
                        "Unknown",
                        reason::UNKNOWN,
                        &format!("Actor is in state {}", actor.state.name()),
                    );
                }
                // The task is not ready, and the reason says why.
                _ => {
                    let message = if actor.crash_message.is_empty() {
                        let name = actor.state.name();
                        format!(
                            "Actor is {}",
                            name.trim_start_matches("ACTOR_STATE_").to_lowercase()
                        )
                    } else {
                        actor.crash_message.clone()
                    };
                    if actor.state == ActorState::Crashed && st.status_detail.is_none() {
                        st.status_detail = Some(StatusDetail {
                            reason: TaskStatusReason::SandboxCrashed.into(),
                            detail: message.clone(),
                        });
                    }
                    set_ready(st, "False", ready_reason, &message);
                }
            }
        }
    }
}

fn task_name_of(task: &Task) -> String {
    let m = task.metadata.as_ref();
    format!(
        "{}/{}",
        m.map_or("", |m| &m.atespace),
        m.map_or("", |m| &m.name)
    )
}

/// Maps an actor's state to the task's state and the reason of its Ready
/// condition.
fn actor_task_state(state: ActorState) -> (TaskState, &'static str) {
    match state {
        ActorState::Running => (TaskState::Working, reason::TASK_RUNNING),
        ActorState::Resuming | ActorState::Reverting => (TaskState::Queued, reason::RESUMING),
        ActorState::Suspending => (TaskState::Suspending, reason::SUSPENDING),
        ActorState::Suspended => (TaskState::Suspended, reason::SUSPENDED),
        ActorState::Crashed => (TaskState::Failed, reason::FAILED),
        ActorState::Deleting => (TaskState::Unspecified, reason::TERMINATING),
        ActorState::Unspecified | ActorState::Other(_) => (TaskState::Unspecified, reason::UNKNOWN),
    }
}

fn condition_true(st: &TaskStatus, cond: &str) -> bool {
    st.conditions
        .iter()
        .find(|c| c.r#type == cond)
        .is_some_and(|c| c.status == "True")
}

/// Sets the task's Ready condition. Its update time changes only when the
/// condition does, so that repeated reads report a stable timestamp.
fn set_ready(st: &mut TaskStatus, status: &str, reason: &str, message: &str) {
    if let Some(c) = st
        .conditions
        .iter_mut()
        .find(|c| c.r#type == condition::READY)
    {
        if c.status != status || c.reason != reason || c.message != message {
            c.status = status.into();
            c.reason = reason.into();
            c.message = message.into();
            c.update_time = Some(now());
        }
        return;
    }
    st.conditions.push(Condition {
        r#type: condition::READY.into(),
        status: status.into(),
        reason: reason.into(),
        message: message.into(),
        update_time: Some(now()),
    });
}

/// Returns the part of a task's status that AX owns and stores. The actor,
/// its UID, and the state, status detail and Ready condition that follow from
/// the actor's live state are dropped.
pub(crate) fn stored_status(st: Option<&TaskStatus>) -> TaskStatus {
    let mut out = st.cloned().unwrap_or_default();
    out.actor.clear();
    out.actor_uid.clear();
    if !is_ax_task_state(out.state()) {
        out.set_state(TaskState::Unspecified);
        out.status_detail = None;
        out.conditions.retain(|c| c.r#type != condition::READY);
    }
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use rax_api::v1alpha1::ObjectMeta;

    pub fn actor(name: &str, state: ActorState) -> Actor {
        Actor {
            name: name.into(),
            uid: String::new(),
            state,
            worker_ip: String::new(),
            crash_message: String::new(),
        }
    }

    fn stored_task(name: &str, state: TaskState, conditions: Vec<Condition>) -> Task {
        let mut status = TaskStatus {
            conditions,
            ..Default::default()
        };
        status.set_state(state);
        Task {
            metadata: Some(ObjectMeta {
                name: name.into(),
                atespace: "default".into(),
                ..Default::default()
            }),
            status: Some(status),
            ..Default::default()
        }
    }

    fn cond(t: &str, status: &str, reason: &str) -> Condition {
        Condition {
            r#type: t.into(),
            status: status.into(),
            reason: reason.into(),
            ..Default::default()
        }
    }

    pub fn ready(task: &Task) -> Option<&Condition> {
        task.status
            .as_ref()?
            .conditions
            .iter()
            .find(|c| c.r#type == "Ready")
    }

    #[test]
    fn actor_status() {
        let ws_ready = || vec![cond("WorkspaceReady", "True", "")];
        let mut crashed = actor("crashed", ActorState::Crashed);
        crashed.crash_message = "OOMKilled".into();
        use ActorState::*;
        use TaskState as T;
        /// The stored task and its actor, then the state, actor and Ready
        /// status and reason the task should be reported with.
        type Case<'a> = (Task, Option<Actor>, TaskState, &'a str, &'a str, &'a str);
        #[rustfmt::skip]
        let cases: Vec<Case> = vec![
            (stored_task("running", T::Unspecified, ws_ready()), Some(actor("running", Running)), T::Working, "running", "True", reason::TASK_RUNNING),
            (stored_task("setting-up", T::Unspecified, vec![]), Some(actor("setting-up", Running)), T::Working, "setting-up", "False", reason::WORKSPACE_INITIALIZING),
            (stored_task("suspended", T::Unspecified, ws_ready()), Some(actor("suspended", Suspended)), T::Suspended, "suspended", "False", reason::SUSPENDED),
            (stored_task("crashed", T::Unspecified, vec![]), Some(crashed.clone()), T::Failed, "crashed", "False", reason::FAILED),
            (stored_task("reverting", T::Unspecified, vec![]), Some(actor("reverting", Reverting)), T::Queued, "reverting", "False", reason::RESUMING),
            (stored_task("suspending", T::Unspecified, vec![]), Some(actor("suspending", Suspending)), T::Suspending, "suspending", "False", reason::SUSPENDING),
            (stored_task("deleting", T::Unspecified, vec![]), Some(actor("deleting", Deleting)), T::Unspecified, "deleting", "False", reason::TERMINATING),
            (stored_task("odd", T::Unspecified, vec![]), Some(actor("odd", Unspecified)), T::Unspecified, "odd", "Unknown", reason::UNKNOWN),
            // The actor was removed outside AX.
            (stored_task("gone", T::Unspecified, vec![]), None, T::Failed, "", "False", reason::ACTOR_NOT_FOUND),
            // States that describe AX's own work are kept, and so is their Ready reason.
            (stored_task("failed", T::Failed, vec![cond("Ready", "False", reason::ACTOR_RESUME_FAILED)]), Some(actor("failed", Suspended)), T::Failed, "failed", "False", reason::ACTOR_RESUME_FAILED),
            (stored_task("not-yet", T::Queued, vec![]), None, T::Queued, "", "", ""),
        ];
        for (mut task, a, want_state, want_actor, want_ready, want_reason) in cases {
            let name = task_name(&task).to_owned();
            set_actor_status(&mut task, Ok(a.as_ref()));
            let st = task.status.as_ref().unwrap();
            assert_eq!(
                (st.state(), st.actor.as_str()),
                (want_state, want_actor),
                "{name}"
            );
            if want_ready.is_empty() {
                continue;
            }
            let c = ready(&task).unwrap();
            assert_eq!(
                (c.status.as_str(), c.reason.as_str()),
                (want_ready, want_reason),
                "{name}"
            );
        }

        // A crash is reported in the Ready message and the status detail.
        let mut task = stored_task("crashed", T::Unspecified, vec![]);
        set_actor_status(&mut task, Ok(Some(&crashed)));
        assert_eq!(ready(&task).unwrap().message, "OOMKilled");
        let detail = task.status.as_ref().unwrap().status_detail.clone().unwrap();
        assert_eq!(detail.reason(), TaskStatusReason::SandboxCrashed);
        assert_eq!(detail.detail, "OOMKilled");

        let mut task = stored_task("suspended", T::Unspecified, vec![]);
        set_actor_status(&mut task, Ok(Some(&actor("suspended", Suspended))));
        assert_eq!(ready(&task).unwrap().message, "Actor is suspended");
        let first = ready(&task).unwrap().update_time;
        std::thread::sleep(std::time::Duration::from_millis(2));
        set_actor_status(&mut task, Ok(Some(&actor("suspended", Suspended))));
        assert_eq!(
            ready(&task).unwrap().update_time,
            first,
            "Ready update time changed on an unchanged read"
        );
    }

    #[test]
    fn substrate_error_reports_unknown() {
        let err = Status::permission_denied("denied");
        let mut task = stored_task("a", TaskState::Unspecified, vec![]);
        let mut terminating = stored_task("b", TaskState::Unspecified, vec![]);
        terminating.metadata.as_mut().unwrap().delete_time = Some(now());
        set_actor_status(&mut task, Err(&err));
        set_actor_status(&mut terminating, Err(&err));
        assert_eq!(
            task.status.as_ref().unwrap().state(),
            TaskState::Unspecified
        );
        let c = ready(&task).unwrap();
        assert_eq!(
            (c.status.as_str(), c.reason.as_str()),
            ("Unknown", reason::SUBSTRATE_UNAVAILABLE)
        );
        assert_eq!(rax_api::format_task_state(&terminating), "Terminating");
        let c = ready(&terminating).unwrap();
        assert_eq!(
            (c.status.as_str(), c.reason.as_str()),
            ("False", reason::TERMINATING)
        );
    }

    #[test]
    fn actor_uid_is_read_never_kept() {
        let mut current = actor("a", ActorState::Running);
        current.uid = "uid-new".into();
        let err = Status::unavailable("unavailable");
        for (name, a, want) in [
            ("current actor", Ok(Some(&current)), "uid-new"),
            (
                "actor without UID",
                Ok(Some(&actor("a", ActorState::Running))),
                "",
            ),
            ("missing actor", Ok(None), ""),
            ("unreachable substrate", Err(&err), ""),
        ] {
            let mut task = stored_task("a", TaskState::Unspecified, vec![]);
            task.status.as_mut().unwrap().actor_uid = "uid-old".into();
            set_actor_status(&mut task, a);
            assert_eq!(task.status.unwrap().actor_uid, want, "{name}");
        }
    }

    #[test]
    fn stored_status_keeps_only_ax_state() {
        let mut input = TaskStatus {
            attempt: 1,
            actor: "actor".into(),
            actor_uid: "uid".into(),
            status_detail: Some(StatusDetail::default()),
            conditions: vec![cond("Ready", "", ""), cond("WorkspaceReady", "", "")],
            ..Default::default()
        };
        input.set_state(TaskState::Working);
        let got = stored_status(Some(&input));
        assert_eq!(got.attempt, 1);
        assert_eq!(got.state(), TaskState::Unspecified);
        assert!(got.actor.is_empty() && got.actor_uid.is_empty());
        assert!(got.status_detail.is_none());
        assert_eq!(got.conditions.len(), 1);
        assert_eq!(got.conditions[0].r#type, "WorkspaceReady");

        let mut input = TaskStatus {
            status_detail: Some(StatusDetail::default()),
            conditions: vec![cond("Ready", "", "")],
            ..Default::default()
        };
        input.set_state(TaskState::Failed);
        let got = stored_status(Some(&input));
        assert_eq!(got.state(), TaskState::Failed);
        assert!(got.status_detail.is_some());
        assert_eq!(got.conditions.len(), 1);
        assert_eq!(stored_status(None), TaskStatus::default());
    }
}
