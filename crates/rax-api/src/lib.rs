//! The AX `ax.io/v1alpha1` API: generated protobuf types and gRPC service,
//! YAML manifest encoding, and the validation the control plane applies before
//! saving a resource.

mod resources;
mod validate;
mod yaml;

pub mod v1alpha1 {
    //! Types and service generated from AX's protos in `proto/pkg/apis/v1alpha1`.
    #![allow(clippy::all)]
    tonic::include_proto!("ax.v1alpha1");
    include!(concat!(env!("OUT_DIR"), "/ax.v1alpha1.serde.rs"));

    /// Encoded `FileDescriptorSet` for AX's protos and their imports.
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        include_bytes!(concat!(env!("OUT_DIR"), "/ax_descriptor.bin"));
}

pub mod substrate {
    //! The messages of Agent Substrate's control API (`substrate.ateapi.v1`)
    //! that AX's protos import: a Gateway's egress rules are Substrate's
    //! `EgressRule`.
    pub mod ateapi {
        pub mod v1 {
            #![allow(clippy::all)]
            include!(concat!(
                env!("OUT_DIR"),
                "/substrate/substrate.ateapi.v1.rs"
            ));
            include!(concat!(
                env!("OUT_DIR"),
                "/substrate/substrate.ateapi.v1.serde.rs"
            ));
        }
    }
}

pub use resources::{
    MAX_CPU_LIMIT_CORES, parse_cpu_millicores, parse_memory_bytes, validate_resources,
};
pub use validate::{
    DEFAULT_WORKSPACE_PATH, MAX_ENV_VALUE_SIZE, MAX_NAME_LENGTH, MAX_TASK_CONFIG_SIZE,
    MAX_TASK_YAML_SIZE, ValidationError, validate_gateway, validate_model, validate_name,
    validate_object_meta, validate_task, validate_task_config, validate_task_size,
    validate_workspace,
};
pub use yaml::{
    Manifest, YamlError, from_json_lenient, from_yaml, from_yaml_lenient, to_json, to_yaml,
};

pub const API_VERSION: &str = "ax.io/v1alpha1";
pub const KIND_TASK: &str = "Task";
pub const KIND_WORKSPACE: &str = "Workspace";
pub const KIND_MODEL: &str = "Model";
pub const KIND_GATEWAY: &str = "Gateway";

pub const DEFAULT_TASK_IMAGE: &str = "gcr.io/ax-substrate/ate-images/ax-task-runner";
pub const DEFAULT_SYSTEM_NAMESPACE: &str = "ax-system";

/// Task phases.
///
/// `PENDING`, `FAILED` and `TERMINATING` describe the control plane's own work
/// and are the only phases stored. The rest follow from the state of the
/// task's actor on Agent Substrate and are read from it whenever a task is
/// fetched.
pub mod phase {
    /// The actor has not been provisioned yet, for example because Substrate
    /// was unavailable. Retrying can still succeed.
    pub const PENDING: &str = "Pending";
    /// AX could not provision or operate the task, or its actor crashed or
    /// disappeared.
    pub const FAILED: &str = "Failed";
    /// Deletion was requested and the actor is being torn down.
    pub const TERMINATING: &str = "Terminating";
    pub const RUNNING: &str = "Running";
    pub const RESUMING: &str = "Resuming";
    pub const SUSPENDING: &str = "Suspending";
    pub const SUSPENDED: &str = "Suspended";
    /// The actor could not be read, or is in a state AX does not recognize.
    pub const UNKNOWN: &str = "Unknown";

    /// Reports whether `phase` describes AX's own work on a task rather than
    /// the state of its actor. Only these phases are stored.
    pub fn is_ax_phase(phase: &str) -> bool {
        matches!(phase, PENDING | FAILED | TERMINATING)
    }
}

/// Condition types reported on Task status.
pub mod condition {
    /// The actor is running and its workspace has finished setting up.
    pub const READY: &str = "Ready";
    /// The workspace inside the actor has finished setting up.
    pub const WORKSPACE_READY: &str = "WorkspaceReady";
}

/// Condition reasons. A Ready condition that is False because the actor is not
/// running (for example while it is suspended) uses the task's phase instead.
pub mod reason {
    pub const TASK_RUNNING: &str = "TaskRunning";
    pub const WORKSPACE_INITIALIZING: &str = "WorkspaceInitializing";
    pub const SUBSTRATE_UNAVAILABLE: &str = "SubstrateUnavailable";
    pub const ACTOR_NOT_FOUND: &str = "ActorNotFound";
    pub const ATESPACE_CREATION_FAILED: &str = "AtespaceCreationFailed";
    pub const INVALID_RESOURCES: &str = "InvalidResources";
    pub const TEMPLATE_CREATION_FAILED: &str = "TemplateCreationFailed";
    pub const ACTOR_CREATION_FAILED: &str = "ActorCreationFailed";
    pub const ACTOR_RESUME_FAILED: &str = "ActorResumeFailed";
    pub const SETUP_COMPLETE: &str = "SetupComplete";
    pub const INITIALIZING: &str = "Initializing";
}

impl v1alpha1::TaskSpec {
    /// The task command's working directory: the bound workspace's path when
    /// set, otherwise [`DEFAULT_WORKSPACE_PATH`].
    pub fn workspace_path(&self) -> &str {
        match self.workspace.as_ref().map(|w| w.path.as_str()) {
            Some(p) if !p.is_empty() => p,
            _ => DEFAULT_WORKSPACE_PATH,
        }
    }
}
