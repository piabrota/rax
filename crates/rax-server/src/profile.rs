//! Compute profiles: which profile a task runs with, the shared ActorTemplate
//! a profile maps to, and the names of workspace snapshot tags, as AX's
//! `controller` package has them.
//!
//! A Profile maps 1:1 to a shared Substrate ActorTemplate (`ax-<hash>`) in an
//! atespace. AX-managed templates synthesized from a Profile snapshot at
//! root filesystem fidelity, so that changes workspace setup makes to the
//! root filesystem persist across snapshots. Only [`effective_profile`] is
//! used by the reconciler yet; the rest is in place for pre-warmed workspace
//! snapshots.

use rax_api::v1alpha1::Profile;
use rax_api::{
    DEFAULT_ATESPACE, DEFAULT_TASK_IMAGE, MAX_NAME_LENGTH, ValidationError, parse_cpu_millicores,
    parse_memory_bytes,
};
use rax_substrate::ateapi::{ActorTemplate, ResourceRequirements, Resources, SnapshotFidelity};
use rax_substrate::{DEFAULT_CPU_MILLICORES, DEFAULT_MEMORY_BYTES, build_actor_template};
use sha2::{Digest, Sha256};

/// The snapshot fidelity of templates synthesized from a Profile.
pub const DEFAULT_PROFILE_SNAPSHOT_FIDELITY: SnapshotFidelity = SnapshotFidelity::Rootfs;

/// The profile a task runs with. Precedence is whole-profile, never merged
/// field by field: a task profile with any field set is used as it is (its
/// unset fields fall back to server defaults, not to the workspace's), and
/// otherwise the bound workspace's profile is, when set. With neither, the
/// result is empty and server defaults apply.
pub fn effective_profile(workspace: Option<&Profile>, task: Option<&Profile>) -> Profile {
    if !is_profile_empty(task) {
        return task.cloned().unwrap_or_default();
    }
    if !is_profile_empty(workspace) {
        return workspace.cloned().unwrap_or_default();
    }
    Profile::default()
}

fn is_profile_empty(p: Option<&Profile>) -> bool {
    let Some(p) = p else { return true };
    let limits = p.resources.as_ref().and_then(|r| r.limits.as_ref());
    p.ate_actor_template.is_empty()
        && p.image.is_empty()
        && limits.is_none_or(|l| l.cpu.is_empty() && l.memory.is_empty())
}

/// Resolves a profile to the name of its shared ActorTemplate and, for an
/// AX-managed one, the template to create. A profile naming an existing
/// `ateActorTemplate` resolves to that name and no template.
///
/// The name is `ax-` and 16 hex digits of a digest of the image, the runner
/// image, the CPU and memory limits (omitted ones at their defaults, so they
/// hash like explicit defaults) and the snapshot fidelity.
pub fn resolve_profile_template(
    atespace: &str,
    p: &Profile,
    runner_image: &str,
) -> Result<(String, Option<ActorTemplate>), ValidationError> {
    let atespace = if atespace.is_empty() {
        DEFAULT_ATESPACE
    } else {
        atespace
    };
    if !p.ate_actor_template.is_empty() {
        return Ok((p.ate_actor_template.clone(), None));
    }
    let image = if p.image.is_empty() {
        DEFAULT_TASK_IMAGE
    } else {
        p.image.as_str()
    };
    let limits = p.resources.as_ref().and_then(|r| r.limits.as_ref());
    let cpu_millicores = match limits.map(|l| l.cpu.as_str()).filter(|c| !c.is_empty()) {
        Some(cpu) => parse_cpu_millicores(cpu).map_err(|e| e.prefixed("profile cpu limit"))?,
        None => DEFAULT_CPU_MILLICORES,
    };
    let memory_bytes = match limits.map(|l| l.memory.as_str()).filter(|m| !m.is_empty()) {
        Some(mem) => parse_memory_bytes(mem).map_err(|e| e.prefixed("profile memory limit"))?,
        None => DEFAULT_MEMORY_BYTES,
    };
    let fidelity = DEFAULT_PROFILE_SNAPSHOT_FIDELITY;
    let mut h = Sha256::new();
    h.update(
        format!(
            "{image}\0{runner_image}\0{cpu_millicores}\0{memory_bytes}\0{}",
            fidelity.as_str_name()
        )
        .as_bytes(),
    );
    let name = format!("ax-{}", hex::encode(&h.finalize()[..8]));

    let resources = ResourceRequirements {
        limits: Some(Resources {
            cpu_millicores,
            memory_bytes,
        }),
    };
    let mut tmpl = build_actor_template(
        atespace,
        &name,
        image,
        &Default::default(),
        &[],
        "",
        Some(&resources),
    );
    if let Some(snap) = tmpl.snapshot_config.as_mut() {
        snap.preferred_fidelity = fidelity as i32;
    }
    Ok((name, Some(tmpl)))
}

/// The most digits a `u64` revision has.
const MAX_U64_DIGITS: usize = 20;
/// How much of the workspace name a tag keeps: room is left for "ws-", the
/// separators, an 8-digit workspace hash, a 16-digit template hash and "-r"
/// with up to 20 revision digits within [`MAX_NAME_LENGTH`].
const MAX_TAG_WORKSPACE_PREFIX: usize =
    MAX_NAME_LENGTH - "ws-".len() - 1 - 8 - 1 - 16 - "-r".len() - MAX_U64_DIGITS;

/// Names the snapshot Tag of a workspace prepared with a template, at a
/// revision: `ws-<prefix>-<hash(workspace)>-<hash(template)>-r<revision>`.
/// Every tag of one workspace shares the part up to the template hash, and
/// the name is always a valid resource name.
pub fn workspace_tag_name(workspace: &str, template: &str, revision: u64) -> String {
    let ws_sum = Sha256::digest(workspace.as_bytes());
    let tpl_sum = Sha256::digest(template.as_bytes());
    let cut = workspace
        .char_indices()
        .nth(MAX_TAG_WORKSPACE_PREFIX)
        .map_or(workspace.len(), |(i, _)| i);
    let prefix = workspace[..cut].trim_end_matches('-');
    format!(
        "ws-{prefix}-{}-{}-r{revision}",
        hex::encode(&ws_sum[..4]),
        hex::encode(&tpl_sum[..8])
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rax_api::v1alpha1::{ResourceList, ResourcesSpec};
    use rax_api::validate_name;

    fn limits(cpu: &str, memory: &str) -> Option<ResourcesSpec> {
        Some(ResourcesSpec {
            limits: Some(ResourceList {
                cpu: cpu.into(),
                memory: memory.into(),
            }),
        })
    }

    fn image(i: &str) -> Profile {
        Profile {
            image: i.into(),
            ..Default::default()
        }
    }

    fn template(t: &str) -> Profile {
        Profile {
            ate_actor_template: t.into(),
            ..Default::default()
        }
    }

    #[test]
    fn effective_profiles_are_never_merged() {
        assert_eq!(effective_profile(None, None), Profile::default());
        let ws = Profile {
            resources: limits("4", "8Gi"),
            ..image("ws-img")
        };
        // An empty task profile, set or not, falls back to the workspace's.
        let empty_task = Profile {
            resources: limits("", ""),
            ..Default::default()
        };
        assert_eq!(effective_profile(Some(&ws), Some(&empty_task)), ws);
        assert_eq!(effective_profile(Some(&ws), None), ws);
        assert_eq!(
            effective_profile(Some(&template("gpu-base")), None),
            template("gpu-base")
        );
        // A task profile with any field set is used as it is.
        let resources_only = Profile {
            resources: limits("", "16Gi"),
            ..Default::default()
        };
        assert_eq!(
            effective_profile(Some(&ws), Some(&resources_only)),
            resources_only
        );
        assert_eq!(
            effective_profile(Some(&ws), Some(&image("task-img"))),
            image("task-img")
        );
        assert_eq!(
            effective_profile(Some(&template("gpu-base")), Some(&resources_only)),
            resources_only
        );
        assert_eq!(
            effective_profile(Some(&ws), Some(&template("tpu-base"))),
            template("tpu-base")
        );
    }

    #[test]
    fn profile_templates_are_named_by_what_they_run() {
        let p1 = Profile {
            resources: limits("1", "1Gi"),
            ..image("img@sha256:abc")
        };
        let p2 = Profile {
            resources: limits("1000m", "1024Mi"),
            ..image("img@sha256:abc")
        };
        let (n1, t1) = resolve_profile_template("default", &p1, "runner@sha256:r1").unwrap();
        let (n2, _) = resolve_profile_template("default", &p2, "runner@sha256:r1").unwrap();
        assert_eq!(n1, n2, "equivalent profiles");
        assert!(n1.starts_with("ax-") && n1.len() == 19, "{n1}");
        validate_name(&n1).unwrap();
        let t1 = t1.unwrap();
        assert_eq!(
            t1.snapshot_config.unwrap().preferred_fidelity,
            SnapshotFidelity::Rootfs as i32
        );
        assert_eq!(t1.metadata.unwrap().name, n1);

        // Omitted limits hash like Substrate's defaults (1000m, 512Mi).
        let (omitted, _) =
            resolve_profile_template("default", &image("img@sha256:abc"), "runner@sha256:r1")
                .unwrap();
        let explicit = Profile {
            resources: limits("1000m", "512Mi"),
            ..image("img@sha256:abc")
        };
        let (explicit, _) =
            resolve_profile_template("default", &explicit, "runner@sha256:r1").unwrap();
        assert_eq!(omitted, explicit);

        let (other_runner, _) =
            resolve_profile_template("default", &p1, "runner@sha256:r2").unwrap();
        assert_ne!(other_runner, n1);

        let (direct, tmpl) =
            resolve_profile_template("default", &template("gpu-base"), "runner@sha256:r1").unwrap();
        assert_eq!(direct, "gpu-base");
        assert!(tmpl.is_none());

        let bad = Profile {
            resources: limits("two", ""),
            ..Default::default()
        };
        let err = resolve_profile_template("", &bad, "r").unwrap_err();
        assert!(err.to_string().starts_with("profile cpu limit"), "{err}");
    }

    #[test]
    fn workspace_tag_names() {
        let (tpl1, tpl2) = ("ax-3f9c1a2b5d7e9f01", "ax-9999999999999999");
        let short1 = workspace_tag_name("my-repo-env", tpl1, 1);
        let short2 = workspace_tag_name("my-repo-env", tpl2, 1);
        let refreshed = workspace_tag_name("my-repo-env", tpl1, 12);
        validate_name(&short1).unwrap();
        assert!(short1.ends_with("-r1") && refreshed.ends_with("-r12"));
        // Tags of one workspace share the owner prefix "ws-<prefix>-<hash>-".
        let owner = |tag: &str| {
            let rev = tag.rfind('-').unwrap();
            let tpl = tag[..rev].rfind('-').unwrap();
            tag[..=tpl].to_owned()
        };
        assert_eq!(owner(&short1), owner(&short2));
        assert_eq!(owner(&short1), owner(&refreshed));

        let long1 = format!("{}1", "a".repeat(62));
        let long2 = format!("{}2", "a".repeat(62));
        let tag1 = workspace_tag_name(&long1, tpl1, 1);
        let tag2 = workspace_tag_name(&long2, tpl1, u64::MAX);
        validate_name(&tag1).unwrap();
        validate_name(&tag2).unwrap();
        assert_ne!(owner(&tag1), owner(&tag2));
        // A prefix that ends in '-' is trimmed, so the name stays valid.
        validate_name(&workspace_tag_name("abcdefghijk-xyz", tpl1, 3)).unwrap();
    }
}
