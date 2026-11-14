//! The RAX control plane: the `ax.v1alpha1.AX` gRPC service over a resource
//! store, with HTTP health checks on the same port.
//!
//! A task's actor on Agent Substrate is the source of truth for whether the
//! task is running, where, and whether it is healthy. The server reads that
//! through [`Actors`] and suspends, resumes and deletes actors through a
//! [`Reconciler`]; both are
//! implemented on top of the Substrate client.

pub mod axattr;
pub mod controller;
mod http;
pub mod pods;
pub mod profile;
mod service;
mod tasks;
pub mod telemetry;

use async_trait::async_trait;
use rax_api::v1alpha1::{Gateway, Task, Workspace};
use tonic::{Code, Status};

pub use http::router;
pub use service::{AxService, ServerOptions};

/// The state of a task's actor on Agent Substrate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorState {
    Unspecified,
    Running,
    Resuming,
    Reverting,
    Suspending,
    Suspended,
    Crashed,
    Deleting,
    /// A state this version of RAX does not know, by its protobuf number.
    Other(i32),
}

impl ActorState {
    /// The state's protobuf enum name, such as `ACTOR_STATE_RUNNING`.
    pub fn name(self) -> String {
        match self {
            Self::Unspecified => "ACTOR_STATE_UNSPECIFIED".into(),
            Self::Running => "ACTOR_STATE_RUNNING".into(),
            Self::Resuming => "ACTOR_STATE_RESUMING".into(),
            Self::Reverting => "ACTOR_STATE_REVERTING".into(),
            Self::Suspending => "ACTOR_STATE_SUSPENDING".into(),
            Self::Suspended => "ACTOR_STATE_SUSPENDED".into(),
            Self::Crashed => "ACTOR_STATE_CRASHED".into(),
            Self::Deleting => "ACTOR_STATE_DELETING".into(),
            Self::Other(n) => n.to_string(),
        }
    }
}

/// What the server reads from a task's actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Actor {
    pub name: String,
    /// The actor's Substrate UID (`ate.actor.uid` in telemetry), or empty
    /// when it has none.
    pub uid: String,
    pub state: ActorState,
    /// The pod IP of the worker hosting the actor, or empty when it has none
    /// (for example while it is suspended).
    pub worker_ip: String,
    /// Why the actor crashed, when it did.
    pub crash_message: String,
}

/// Reads actors from Agent Substrate.
#[async_trait]
pub trait Actors: Send + Sync {
    /// Returns the actor, or `None` when it does not exist.
    async fn get_actor(&self, atespace: &str, name: &str) -> Result<Option<Actor>, Status>;
    async fn list_actors(&self, atespace: &str) -> Result<Vec<Actor>, Status>;
    /// The actor to report for a task that has none. `None`, the default,
    /// reports the task as failed: on Substrate a provisioned task always has
    /// an actor.
    fn absent_actor(&self, _name: &str) -> Option<Actor> {
        None
    }
}

/// Coordinates sandbox and actor lifecycles on Agent Substrate, as AX's
/// `controller.TaskReconciler` does.
///
/// [`suspend`](Self::suspend) and [`resume`](Self::resume) first make sure the
/// task's atespace, template and actor exist, and that the actor of a task
/// bound to a Gateway has its rules. They record on `task` only what AX
/// stores in a task's status: the template it runs from, its resolved argv,
/// the `WorkspaceReady` condition, and on failure the reason in a False
/// `Ready` condition. The caller sets the state, and the server reads the
/// rest of the status from the actor.
#[async_trait]
pub trait Reconciler: Send + Sync {
    /// Provisions the task's actor and suspends it, then binds its gateway,
    /// so the actor is suspended even when binding fails. A failure to
    /// suspend the actor itself is returned without changing the task's
    /// status.
    async fn suspend(
        &self,
        task: &mut Task,
        workspace: Option<Workspace>,
        gateway: Option<Gateway>,
    ) -> Result<(), Status>;
    /// Provisions the task's actor, binds its gateway, resumes the actor, and
    /// waits a little for its workspace to be set up, recording the result in
    /// `WorkspaceReady`.
    async fn resume(
        &self,
        task: &mut Task,
        workspace: Option<Workspace>,
        gateway: Option<Gateway>,
    ) -> Result<(), Status>;
    /// Removes the task's actor from Substrate, and the templates made for
    /// it unless it runs from a template of its own.
    async fn delete(&self, task: &Task) -> Result<(), Status>;
}

/// Reports whether a Substrate error is worth retrying: Substrate was
/// unreachable or too slow, rather than refusing the request.
pub fn is_transient(status: &Status) -> bool {
    matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded)
}
