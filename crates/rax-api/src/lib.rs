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

pub mod google {
    //! googleapis' `google.rpc.Status`, which a task's outcome carries.
    pub mod rpc {
        #![allow(clippy::all)]
        include!(concat!(env!("OUT_DIR"), "/substrate/google.rpc.rs"));
        include!(concat!(env!("OUT_DIR"), "/substrate/google.rpc.serde.rs"));
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

pub const DEFAULT_ATESPACE: &str = "default";

/// The file in a task's sandbox that holds the CA certificates to trust for
/// outbound TLS, including the egress gateway's.
pub const EGRESS_TRUST_BUNDLE_PATH: &str = "/run/ate/egress-trust-bundle/trust-bundle.pem";

/// Reports whether `state` is one of the control plane's own stored task
/// states (queued for provisioning or retry, or terminal) rather than one
/// read from the task's actor on Agent Substrate. Only these are stored.
pub fn is_ax_task_state(state: v1alpha1::TaskState) -> bool {
    use v1alpha1::TaskState as S;
    matches!(state, S::Queued | S::Completed | S::Failed | S::Cancelled)
}

/// A task's state as the CLI shows it. A task whose deletion is in progress
/// (metadata.deleteTime set) is "Terminating".
pub fn format_task_state(task: &v1alpha1::Task) -> &'static str {
    use v1alpha1::TaskState as S;
    if task
        .metadata
        .as_ref()
        .is_some_and(|m| m.delete_time.is_some())
    {
        return reason::TERMINATING;
    }
    let state = task.status.as_ref().map_or(S::Unspecified, |s| s.state());
    match state {
        S::Idle => "Idle",
        S::Working => "Working",
        S::RequiresAction => "RequiresAction",
        S::Completed => "Completed",
        S::Failed => "Failed",
        S::Cancelled => "Cancelled",
        S::Queued => "Queued",
        S::Suspended => "Suspended",
        S::Suspending => "Suspending",
        S::Unspecified => reason::UNKNOWN,
    }
}

/// Condition types reported on Task status.
pub mod condition {
    /// The actor is running and its workspace has finished setting up.
    pub const READY: &str = "Ready";
    /// The workspace inside the actor has finished setting up.
    pub const WORKSPACE_READY: &str = "WorkspaceReady";
}

/// Condition reasons reported on Ready and WorkspaceReady conditions.
pub mod reason {
    pub const TASK_RUNNING: &str = "TaskRunning";
    pub const WORKSPACE_INITIALIZING: &str = "WorkspaceInitializing";
    pub const SUBSTRATE_UNAVAILABLE: &str = "SubstrateUnavailable";
    pub const ACTOR_NOT_FOUND: &str = "ActorNotFound";
    pub const ATESPACE_CREATION_FAILED: &str = "AtespaceCreationFailed";
    pub const INVALID_RESOURCES: &str = "InvalidResources";
    pub const TEMPLATE_CREATION_FAILED: &str = "TemplateCreationFailed";
    pub const ACTOR_CREATION_FAILED: &str = "ActorCreationFailed";
    pub const GATEWAY_BINDING_FAILED: &str = "GatewayBindingFailed";
    pub const ACTOR_RESUME_FAILED: &str = "ActorResumeFailed";
    // Reported on Ready when the actor is not running.
    pub const RESUMING: &str = "Resuming";
    pub const SUSPENDING: &str = "Suspending";
    pub const SUSPENDED: &str = "Suspended";
    pub const FAILED: &str = "Failed";
    pub const TERMINATING: &str = "Terminating";
    pub const UNKNOWN: &str = "Unknown";
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
