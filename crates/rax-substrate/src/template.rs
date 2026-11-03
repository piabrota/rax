//! Actor templates for task actors.

use std::collections::BTreeMap;

use rax_api::v1alpha1::ResourcesSpec;
use rax_api::{DEFAULT_TASK_IMAGE, parse_cpu_millicores, parse_memory_bytes};

use crate::ateapi::{
    ActorTemplate, Container, DurableDirVolumeSource, EnvVar, HttpGetAction, ResourceMetadata,
    ResourceRequirements, Resources, SnapshotConfig, SnapshotFidelity, Volume, VolumeMount,
    WakeupProbe,
};

/// The command the guest container runs when a template names none.
pub const DEFAULT_GUEST_COMMAND: &str = "/usr/local/bin/ax-task-runner";
/// The CPU limit (1 core) for a task that sets none.
pub const DEFAULT_CPU_MILLICORES: i64 = 1000;
/// The memory limit (512 MiB) for a task that sets none.
pub const DEFAULT_MEMORY_BYTES: i64 = 512 << 20;

/// Translates a task's resource limits into a template's resources. Substrate
/// sizes a sandbox by limits alone, so requests are ignored (validation
/// rejects them). Returns `None` when no limit is set, so the template gets
/// the default limits; a limit that does not parse keeps its default.
pub fn resource_limits(reqs: Option<&ResourcesSpec>) -> Option<ResourceRequirements> {
    let limits = reqs.and_then(|r| r.limits.as_ref())?;
    if limits.cpu.is_empty() && limits.memory.is_empty() {
        return None;
    }
    let mut out = Resources {
        cpu_millicores: DEFAULT_CPU_MILLICORES,
        memory_bytes: DEFAULT_MEMORY_BYTES,
    };
    if let Ok(cpu) = parse_cpu_millicores(&limits.cpu) {
        out.cpu_millicores = cpu;
    }
    if let Ok(mem) = parse_memory_bytes(&limits.memory) {
        out.memory_bytes = mem;
    }
    Some(ResourceRequirements { limits: Some(out) })
}

/// Builds the template every task actor runs from: one `guest` container
/// with a `/readyz` wakeup probe and a durable `/workspace` volume,
/// snapshotted at volume fidelity. Empty arguments take their defaults, and
/// missing resource limits take [`DEFAULT_CPU_MILLICORES`] and
/// [`DEFAULT_MEMORY_BYTES`].
///
/// `snapshots_bucket` sets the template's own snapshot storage location.
/// Substrate rejects a non-empty one (`storage_location: Forbidden`) unless
/// the cluster enables the `PerTemplateSnapshotLocation` preview feature
/// gate; leaving it empty uses Substrate's cluster-wide snapshot location.
pub fn build_actor_template(
    atespace: &str,
    name: &str,
    image: &str,
    env: &BTreeMap<String, String>,
    command: &[String],
    snapshots_bucket: &str,
    resources: Option<&ResourceRequirements>,
) -> ActorTemplate {
    let atespace = if atespace.is_empty() {
        "default"
    } else {
        atespace
    };
    let image = if image.is_empty() {
        DEFAULT_TASK_IMAGE
    } else {
        image
    };
    let command = if command.is_empty() {
        vec![DEFAULT_GUEST_COMMAND.to_owned()]
    } else {
        command.to_vec()
    };
    let limits = resources.and_then(|r| r.limits.as_ref());
    let cpu_millicores = limits
        .map(|l| l.cpu_millicores)
        .filter(|&c| c > 0)
        .unwrap_or(DEFAULT_CPU_MILLICORES);
    let memory_bytes = limits
        .map(|l| l.memory_bytes)
        .filter(|&m| m > 0)
        .unwrap_or(DEFAULT_MEMORY_BYTES);

    ActorTemplate {
        metadata: Some(ResourceMetadata {
            atespace: atespace.to_owned(),
            name: name.to_owned(),
            ..Default::default()
        }),
        containers: vec![Container {
            name: "guest".to_owned(),
            image: image.to_owned(),
            command,
            env: env
                .iter()
                .map(|(name, value)| EnvVar {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
            wakeup_probe: Some(WakeupProbe {
                http_get: Some(HttpGetAction {
                    path: "/readyz".to_owned(),
                    port: 80,
                }),
                ..Default::default()
            }),
            volume_mounts: vec![VolumeMount {
                name: "workspace".to_owned(),
                mount_path: "/workspace".to_owned(),
            }],
            ..Default::default()
        }],
        volumes: vec![Volume {
            name: "workspace".to_owned(),
            durable_dir: Some(DurableDirVolumeSource {}),
            ..Default::default()
        }],
        snapshot_config: Some(SnapshotConfig {
            storage_location: snapshots_bucket.to_owned(),
            preferred_fidelity: SnapshotFidelity::Volumes as i32,
            ..Default::default()
        }),
        resources: Some(ResourceRequirements {
            limits: Some(Resources {
                cpu_millicores,
                memory_bytes,
            }),
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rax_api::v1alpha1::ResourceList;

    #[test]
    fn builds_the_task_template() {
        let env = BTreeMap::from([("FOO".to_owned(), "BAR".to_owned())]);
        let tmpl = build_actor_template("", "test-tmpl", "", &env, &[], "", None);
        let meta = tmpl.metadata.as_ref().unwrap();
        assert_eq!(meta.atespace, "default");
        assert_eq!(meta.name, "test-tmpl");
        assert_eq!(tmpl.containers.len(), 1);
        let c = &tmpl.containers[0];
        assert_eq!(c.name, "guest");
        assert_eq!(c.image, DEFAULT_TASK_IMAGE);
        assert_eq!(c.command, [DEFAULT_GUEST_COMMAND]);
        assert_eq!(
            c.env,
            [EnvVar {
                name: "FOO".into(),
                value: "BAR".into()
            }]
        );
        let probe = c.wakeup_probe.as_ref().unwrap().http_get.as_ref().unwrap();
        assert_eq!((probe.path.as_str(), probe.port), ("/readyz", 80));
        assert_eq!(c.volume_mounts[0].mount_path, "/workspace");
        assert!(tmpl.volumes[0].durable_dir.is_some());
        let snap = tmpl.snapshot_config.as_ref().unwrap();
        // Empty by default, so Substrate 1.0 uses its cluster-wide location.
        assert_eq!(snap.storage_location, "");
        assert_eq!(snap.preferred_fidelity, SnapshotFidelity::Volumes as i32);

        // An explicit bucket is the template's location.
        let tmpl = build_actor_template(
            "",
            "test-tmpl",
            "",
            &BTreeMap::new(),
            &[],
            "gs://ate-snapshots/ax/",
            None,
        );
        assert_eq!(
            tmpl.snapshot_config.unwrap().storage_location,
            "gs://ate-snapshots/ax/"
        );
    }

    fn limits(cpu: i64, memory: i64) -> ResourceRequirements {
        ResourceRequirements {
            limits: Some(Resources {
                cpu_millicores: cpu,
                memory_bytes: memory,
            }),
        }
    }

    #[test]
    fn translates_resource_limits() {
        let spec = |cpu: &str, memory: &str| ResourcesSpec {
            limits: Some(ResourceList {
                cpu: cpu.into(),
                memory: memory.into(),
            }),
            ..Default::default()
        };
        assert_eq!(resource_limits(None), None);
        assert_eq!(resource_limits(Some(&ResourcesSpec::default())), None);
        assert_eq!(resource_limits(Some(&spec("", ""))), None);
        assert_eq!(
            resource_limits(Some(&spec("2", ""))),
            Some(limits(2000, DEFAULT_MEMORY_BYTES))
        );
        assert_eq!(
            resource_limits(Some(&spec("", "4Gi"))),
            Some(limits(DEFAULT_CPU_MILLICORES, 4 << 30))
        );
        assert_eq!(
            resource_limits(Some(&spec("2", "4Gi"))),
            Some(limits(2000, 4 << 30))
        );
    }

    #[test]
    fn template_carries_limits_or_defaults() {
        let env = BTreeMap::new();
        let want = limits(2000, 4 << 30);
        let image = "ghcr.io/my-org/agent@sha256:abc";
        let tmpl = build_actor_template("default", "t", image, &env, &[], "", Some(&want));
        assert_eq!(tmpl.resources, Some(want));
        let tmpl = build_actor_template("default", "t", image, &env, &[], "", None);
        assert_eq!(
            tmpl.resources,
            Some(limits(DEFAULT_CPU_MILLICORES, DEFAULT_MEMORY_BYTES))
        );
    }
}
