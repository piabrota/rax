//! The OpenTelemetry attribute names AX puts on its spans, logs and events,
//! so every emitter spells them the same way and RAX's records join AX's.
//! Attributes that identify an actor reuse Agent Substrate's names, so AX
//! and Substrate records join on them.
//!
//! Keep these attributes off metrics:
//! - The identity attributes (task ID and name, atespace, actor UID) have one
//!   value per task or per user-chosen atespace, so their set of values is
//!   unbounded. Agent Substrate's no-actor-identity rule also keeps
//!   `ate.atespace` and `ate.actor.uid` off its metrics.
//! - The exit code and the signal have a bounded set of values, but they
//!   describe one task's command, so they are recorded only beside the task
//!   identity, on spans, logs and events.

use opentelemetry::Key;

/// The task's `status.id`, unique across the task's lifetime.
pub const TASK_ID: Key = Key::from_static_str("ax.task.id");
/// The task's `metadata.name`.
pub const TASK_NAME: Key = Key::from_static_str("ax.task.name");
/// The task's `status.phase`, for example `Running`.
pub const TASK_PHASE: Key = Key::from_static_str("ax.task.phase");
/// Where the task's actor is in its lifecycle: queued, resuming, running,
/// suspending, suspended, reverting, crashed, deleting, or deleted.
pub const TASK_RESIDENCY: Key = Key::from_static_str("ax.task.residency");
/// How the task's command ended: succeeded, failed, or cancelled.
pub const TASK_OUTCOME: Key = Key::from_static_str("ax.task.outcome");
/// What decided the outcome: sandbox, orchestrator, or external.
pub const TASK_OUTCOME_SOURCE: Key = Key::from_static_str("ax.task.outcome_source");
/// The exit code of the task's command.
pub const TASK_EXIT_CODE: Key = Key::from_static_str("ax.task.exit_code");
/// The signal that ended the task's command.
pub const TASK_SIGNAL: Key = Key::from_static_str("ax.task.signal");

/// The atespace of the task and its actor, named as Substrate names it.
pub const ATESPACE: Key = Key::from_static_str("ate.atespace");
/// The UID of the task's actor, named as Substrate names it.
pub const ACTOR_UID: Key = Key::from_static_str("ate.actor.uid");
