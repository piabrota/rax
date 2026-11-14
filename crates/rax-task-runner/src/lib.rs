//! What runs inside every RAX task container, a port of AX's task-runner.
//!
//! Given a Task and its Workspace, [`runner::run`] starts the metadata and
//! guest server, sets up the workspace on its first run, and then starts the
//! task's command as a supervised child. It stays up as the container's main
//! process until it is told to stop, so the sandbox remains inspectable after
//! the command finishes. The `rax-task-runner` binary is a thin wrapper
//! around it.

pub mod metadata;
pub mod runner;
pub mod workspace;

use rax_api::{Manifest, from_yaml_lenient};

/// Decodes a spec handed to the runner, from `file` when it is set and
/// otherwise from the environment variable `env_var`. Neither being set gives
/// `None`, and blank YAML an empty resource. Unknown fields are ignored, so a
/// runner image keeps working with a newer control plane.
pub fn load_spec<T: Manifest>(
    file: Option<&std::path::Path>,
    env_var: &str,
) -> Result<Option<T>, String> {
    let raw = match file {
        Some(path) => {
            std::fs::read_to_string(path).map_err(|e| format!("reading {}: {e}", path.display()))?
        }
        None => match std::env::var(env_var) {
            Ok(v) if !v.is_empty() => v,
            _ => return Ok(None),
        },
    };
    from_yaml_lenient(&raw)
        .map(Some)
        .map_err(|e| format!("parsing {}: {e}", T::MESSAGE))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rax_api::v1alpha1::{Task, Workspace};

    fn write(content: &str) -> tempfile::NamedTempFile {
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(f.path(), content).unwrap();
        f
    }

    #[test]
    fn ignores_unknown_fields() {
        let f =
            write("metadata:\n  name: t\nspec:\n  profile:\n    image: img\n  futureField: true\n");
        let task: Task = load_spec(Some(f.path()), "UNUSED").unwrap().unwrap();
        assert_eq!(task.metadata.unwrap().name, "t");
        assert_eq!(task.spec.unwrap().profile.unwrap().image, "img");

        let f = write(
            "metadata:\n  name: ws\nspec:\n  setup:\n    files:\n    - path: a\n      mode: 0644\n",
        );
        let ws: Workspace = load_spec(Some(f.path()), "UNUSED").unwrap().unwrap();
        assert_eq!(ws.spec.unwrap().setup.unwrap().files[0].path, "a");
    }

    #[test]
    fn empty_specs() {
        let f = write("");
        let task: Task = load_spec(Some(f.path()), "UNUSED").unwrap().unwrap();
        assert!(task.metadata.is_none());
        assert!(
            load_spec::<Task>(None, "RAX_TEST_SURELY_UNSET_VAR")
                .unwrap()
                .is_none()
        );
        assert!(load_spec::<Task>(Some(std::path::Path::new("/nonexistent")), "X").is_err());
    }
}
