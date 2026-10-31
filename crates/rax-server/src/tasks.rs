//! Task storage and Substrate.
//!
//! The store keeps only what AX itself knows about a task. Its actor, the
//! worker it runs on, and the phase and Ready condition that follow from the
//! actor's state are read from Substrate whenever a task is fetched.
//!
//! The service reaches task records only through [`Tasks`], which drops the
//! actor-owned status before writing and fills it in after reading, so no
//! handler can skip that split.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use pbjson_types::Timestamp;
use rax_api::v1alpha1::{Condition, Task, TaskStatus};
use rax_api::{condition, phase, reason};
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

    /// Removes a task's actor and templates from Substrate, then its record
    /// from the store.
    pub async fn delete(
        &self,
        reconciler: Option<&dyn Reconciler>,
        atespace: &str,
        name: &str,
    ) -> Result<(), Status> {
        if let Some(r) = reconciler
            && let Err(e) = r.delete(atespace, name).await
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

/// Overlays the actor's state on the task's status: actor, workerIP, and,
/// unless the phase is one of AX's own, the phase and Ready condition.
/// `actor` is `Ok(None)` when the task has no actor, and an error when the
/// actor could not be looked up, in which case the task is reported with
/// phase Unknown.
pub(crate) fn set_actor_status(task: &mut Task, actor: Result<Option<&Actor>, &Status>) {
    let st = task.status.get_or_insert_with(Default::default);
    st.actor.clear();
    st.worker_ip.clear();
    let ax_phase = phase::is_ax_phase(&st.phase);

    match actor {
        Err(e) => {
            tracing::warn!(task = task_name_of(task), error = %e.message(), "could not read task actor");
            if !ax_phase {
                let st = task.status.as_mut().unwrap();
                st.phase = phase::UNKNOWN.into();
                set_ready(st, "Unknown", reason::SUBSTRATE_UNAVAILABLE, e.message());
            }
        }
        // A task that finished provisioning always has an actor, so a missing
        // one was removed outside AX.
        Ok(None) => {
            if !ax_phase {
                st.phase = phase::FAILED.into();
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
            st.worker_ip = actor.worker_ip.clone();
            if ax_phase {
                return;
            }
            st.phase = actor_phase(actor.state).into();
            match st.phase.as_str() {
                phase::RUNNING => {
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
                phase::UNKNOWN => {
                    set_ready(
                        st,
                        "Unknown",
                        phase::UNKNOWN,
                        &format!("Actor is in state {}", actor.state.name()),
                    );
                }
                // The task is not ready, and its phase says why.
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
                    let p = st.phase.clone();
                    set_ready(st, "False", &p, &message);
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

/// Maps an actor's state to the task's phase.
fn actor_phase(state: ActorState) -> &'static str {
    match state {
        ActorState::Running => phase::RUNNING,
        ActorState::Resuming | ActorState::Reverting => phase::RESUMING,
        ActorState::Suspending => phase::SUSPENDING,
        ActorState::Suspended => phase::SUSPENDED,
        ActorState::Crashed => phase::FAILED,
        ActorState::Deleting => phase::TERMINATING,
        ActorState::Unspecified | ActorState::Other(_) => phase::UNKNOWN,
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

/// Returns the part of a task's status that AX owns and stores. The actor, its
/// worker, and the phase and Ready condition that follow from the actor's
/// state are dropped.
pub(crate) fn stored_status(st: Option<&TaskStatus>) -> TaskStatus {
    let mut out = st.cloned().unwrap_or_default();
    out.actor.clear();
    out.worker_ip.clear();
    if !phase::is_ax_phase(&out.phase) {
        out.phase.clear();
        out.conditions.retain(|c| c.r#type != condition::READY);
    }
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use rax_api::v1alpha1::ObjectMeta;

    pub fn actor(name: &str, state: ActorState, worker_ip: &str) -> Actor {
        Actor {
            name: name.into(),
            state,
            worker_ip: worker_ip.into(),
            crash_message: String::new(),
        }
    }

    fn stored_task(name: &str, phase: &str, conditions: Vec<Condition>) -> Task {
        Task {
            metadata: Some(ObjectMeta {
                name: name.into(),
                atespace: "default".into(),
                create_time: None,
            }),
            status: Some(TaskStatus {
                phase: phase.into(),
                conditions,
                ..Default::default()
            }),
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
        let mut crashed = actor("crashed", ActorState::Crashed, "");
        crashed.crash_message = "OOMKilled".into();
        use ActorState::*;
        /// The stored task and its actor, then the phase, worker IP, actor and
        /// Ready status and reason the task should be reported with.
        type Case<'a> = (
            Task,
            Option<Actor>,
            &'a str,
            &'a str,
            &'a str,
            &'a str,
            &'a str,
        );
        #[rustfmt::skip]
        let cases: Vec<Case> = vec![
            (stored_task("running", "", ws_ready()), Some(actor("running", Running, "10.0.0.5")), "Running", "10.0.0.5", "running", "True", reason::TASK_RUNNING),
            (stored_task("setting-up", "", vec![]), Some(actor("setting-up", Running, "10.0.0.6")), "Running", "10.0.0.6", "setting-up", "False", reason::WORKSPACE_INITIALIZING),
            (stored_task("suspended", "", ws_ready()), Some(actor("suspended", Suspended, "")), "Suspended", "", "suspended", "False", phase::SUSPENDED),
            (stored_task("crashed", "", vec![]), Some(crashed.clone()), phase::FAILED, "", "crashed", "False", phase::FAILED),
            (stored_task("reverting", "", vec![]), Some(actor("reverting", Reverting, "")), phase::RESUMING, "", "reverting", "False", phase::RESUMING),
            (stored_task("suspending", "", vec![]), Some(actor("suspending", Suspending, "")), phase::SUSPENDING, "", "suspending", "False", phase::SUSPENDING),
            (stored_task("deleting", "", vec![]), Some(actor("deleting", Deleting, "")), phase::TERMINATING, "", "deleting", "False", phase::TERMINATING),
            (stored_task("odd", "", vec![]), Some(actor("odd", Unspecified, "")), phase::UNKNOWN, "", "odd", "Unknown", phase::UNKNOWN),
            // The actor was removed outside AX.
            (stored_task("gone", "", vec![]), None, phase::FAILED, "", "", "False", reason::ACTOR_NOT_FOUND),
            // Phases that describe AX's own work are kept, and so is their Ready reason.
            (stored_task("failed", phase::FAILED, vec![cond("Ready", "False", reason::ACTOR_RESUME_FAILED)]), Some(actor("failed", Suspended, "")), phase::FAILED, "", "failed", "False", reason::ACTOR_RESUME_FAILED),
            (stored_task("not-yet", phase::PENDING, vec![]), None, phase::PENDING, "", "", "", ""),
        ];
        for (mut task, a, want_phase, want_ip, want_actor, want_ready, want_reason) in cases {
            let name = task_name(&task).to_owned();
            set_actor_status(&mut task, Ok(a.as_ref()));
            let st = task.status.as_ref().unwrap();
            assert_eq!(
                (st.phase.as_str(), st.worker_ip.as_str(), st.actor.as_str()),
                (want_phase, want_ip, want_actor),
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

        let mut task = stored_task("crashed", "", vec![]);
        set_actor_status(&mut task, Ok(Some(&crashed)));
        assert_eq!(ready(&task).unwrap().message, "OOMKilled");

        let mut task = stored_task("suspended", "", vec![]);
        set_actor_status(&mut task, Ok(Some(&actor("suspended", Suspended, ""))));
        assert_eq!(ready(&task).unwrap().message, "Actor is suspended");
        let first = ready(&task).unwrap().update_time;
        std::thread::sleep(std::time::Duration::from_millis(2));
        set_actor_status(&mut task, Ok(Some(&actor("suspended", Suspended, ""))));
        assert_eq!(
            ready(&task).unwrap().update_time,
            first,
            "Ready update time changed on an unchanged read"
        );
    }

    #[test]
    fn substrate_error_reports_unknown() {
        let err = Status::permission_denied("denied");
        let mut task = stored_task("a", "", vec![]);
        let mut terminating = stored_task("b", phase::TERMINATING, vec![]);
        set_actor_status(&mut task, Err(&err));
        set_actor_status(&mut terminating, Err(&err));
        assert_eq!(task.status.as_ref().unwrap().phase, "Unknown");
        let c = ready(&task).unwrap();
        assert_eq!(
            (c.status.as_str(), c.reason.as_str()),
            ("Unknown", reason::SUBSTRATE_UNAVAILABLE)
        );
        assert_eq!(terminating.status.unwrap().phase, phase::TERMINATING);
    }

    #[test]
    fn stored_status_keeps_only_ax_state() {
        let input = TaskStatus {
            id: "id".into(),
            phase: "Running".into(),
            actor: "actor".into(),
            worker_ip: "10.0.0.1".into(),
            conditions: vec![cond("Ready", "", ""), cond("WorkspaceReady", "", "")],
        };
        let got = stored_status(Some(&input));
        assert_eq!(got.id, "id");
        assert!(got.phase.is_empty() && got.actor.is_empty() && got.worker_ip.is_empty());
        assert_eq!(got.conditions.len(), 1);
        assert_eq!(got.conditions[0].r#type, "WorkspaceReady");

        let input = TaskStatus {
            phase: phase::FAILED.into(),
            conditions: vec![cond("Ready", "", "")],
            ..Default::default()
        };
        let got = stored_status(Some(&input));
        assert_eq!(got.phase, phase::FAILED);
        assert_eq!(got.conditions.len(), 1);
        assert_eq!(stored_status(None), TaskStatus::default());
    }
}
