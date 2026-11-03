//! A client for the Agent Substrate control API, as AX uses it: atespaces,
//! actor templates, and the lifecycle of the actor behind each task.

mod client;
pub mod probe;
mod retry;
mod template;
#[cfg(feature = "testing")]
pub mod testing;
pub mod tls;

pub mod ateapi {
    //! Types and services generated from Substrate's `api/ateapi/v1/ateapi.proto`
    //! (package `substrate.ateapi.v1`).
    #![allow(clippy::all)]
    tonic::include_proto!("substrate.ateapi.v1");
}

pub use client::{Client, ClientOptions, DEFAULT_TARGET, TokenInterceptor, worker_ip};
pub use retry::RetryPolicy;
pub use template::{
    DEFAULT_CPU_MILLICORES, DEFAULT_GUEST_COMMAND, DEFAULT_MEMORY_BYTES, build_actor_template,
    resource_limits,
};

use tonic::{Code, Status};

/// A failed Substrate call: what AX was doing, and the status Substrate (or
/// the connection to it) returned.
#[derive(Debug, Clone)]
pub struct Error {
    context: String,
    status: Status,
}

impl Error {
    pub fn new(context: impl Into<String>, status: Status) -> Self {
        Self {
            context: context.into(),
            status,
        }
    }

    pub fn code(&self) -> Code {
        self.status.code()
    }

    /// The message Substrate wrote, without AX's context. This is what users
    /// see on a task's conditions.
    pub fn message(&self) -> &str {
        self.status.message()
    }

    /// Reports whether the call is worth retrying: Substrate was unreachable or
    /// too slow, rather than refusing the request.
    pub fn is_transient(&self) -> bool {
        matches!(self.code(), Code::Unavailable | Code::DeadlineExceeded)
    }

    /// Adds outer context, as in "ensuring actor: creating actor a/b: ...".
    pub fn context(self, outer: &str) -> Self {
        let context = if self.context.is_empty() {
            outer.to_owned()
        } else {
            format!("{outer}: {}", self.context)
        };
        Self {
            context,
            status: self.status,
        }
    }

    /// A gRPC status with the same code and the full message, for returning
    /// to AX's own callers.
    pub fn to_status(&self) -> Status {
        Status::new(self.code(), self.to_string())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.context.is_empty() {
            write!(f, "{}", self.message())
        } else {
            write!(f, "{}: {}", self.context, self.message())
        }
    }
}

impl std::error::Error for Error {}

impl From<Status> for Error {
    fn from(status: Status) -> Self {
        Self::new("", status)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
