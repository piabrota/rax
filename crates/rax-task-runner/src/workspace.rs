//! Workspace setup on a task's first run: inlined files, git checkouts and
//! the skills directory, with a marker recording that it is done.

use std::path::{Path, PathBuf};
use std::time::Duration;

use rax_api::DEFAULT_WORKSPACE_PATH;
use rax_api::v1alpha1::{File, GitRepo, SkillsConfig, Workspace};
use tokio::process::Command;

/// The default directory for AX's own state and logs inside the container.
pub const DEFAULT_AX_DIR: &str = "/ax";
/// The base name of the marker written after a successful first run.
pub const INITIALIZED_MARKER_FILENAME: &str = "initialized";
/// The directory under the workspace that holds the marker.
const MARKER_DIR_NAME: &str = ".ax";

const DEFAULT_BRANCH: &str = "main";
const DEFAULT_REPO_DIR_NAME: &str = "repo";
const GIT_SUCCESS_LOG: &str = "git-success.log";
const GIT_ERROR_LOG: &str = "git-error.log";
const GIT_RETRIES: u32 = 5;
const GIT_RETRY_DELAY: Duration = Duration::from_secs(2);

/// What a setup did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SetupResult {
    pub workspace_path: PathBuf,
    pub cloned_repos: Vec<String>,
    pub skills_mounted: String,
    pub is_maiden_run: bool,
}

/// Prepares the workspace at `target` on its first run: writes the inlined
/// files, clones the declared repositories and creates the skills path. A
/// marker on the workspace volume (see [`workspace_marker_path`]) records a
/// finished first run, so later calls do nothing.
///
/// Git failures are logged and recorded under `ax_dir` but don't fail setup;
/// the marker is withheld so the next start retries the clone. Failing to
/// create the workspace directory is an error, and so is a marker path that
/// is a symbolic link.
pub async fn setup_workspace(
    ws: Option<&Workspace>,
    target: &str,
    ax_dir: &Path,
) -> std::io::Result<SetupResult> {
    let target = if target.is_empty() {
        DEFAULT_WORKSPACE_PATH
    } else {
        target
    };
    // git scopes itself to this path, so it must be absolute.
    let target = std::path::absolute(target).unwrap_or_else(|_| PathBuf::from(target));
    let mut res = SetupResult {
        workspace_path: target.clone(),
        ..Default::default()
    };

    std::fs::create_dir_all(&target).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("creating workspace directory {}: {e}", target.display()),
        )
    })?;
    if let Err(e) = std::fs::create_dir_all(ax_dir) {
        tracing::warn!(dir = %ax_dir.display(), error = %e, "creating ax state directory");
    }

    let marker = workspace_marker_path(&target);
    if marker_linked(&marker) {
        return Err(std::io::Error::other(format!(
            "marker path {} is a symbolic link; refusing to trust or write it",
            marker.display()
        )));
    }
    if marker.exists() {
        tracing::info!(path = %target.display(), "workspace already initialized; skipping maiden run setup");
        return Ok(res);
    }
    // A marker from before the marker moved to the workspace volume lives
    // under `ax_dir`. Migrate it, so a container that still has `ax_dir`
    // does not rerun setup over the agent's edits.
    if let Ok(data) = std::fs::read(ax_dir.join(marker_name(&target.to_string_lossy())))
        && legacy_owned_by(&data, ws)
    {
        tracing::info!(path = %target.display(), "migrating legacy workspace marker");
        write_marker(&workspace_marker_path(&target), ws);
        return Ok(res);
    }

    tracing::info!(path = %target.display(), "executing workspace maiden run setup");
    res.is_maiden_run = true;

    let mut git_ok = true;
    if let Some(spec) = ws.and_then(|w| w.spec.as_ref()) {
        write_files(&spec.files, &target);
        (res.cloned_repos, git_ok) = clone_repos(&spec.git, &target, ax_dir).await;
        res.skills_mounted = setup_skills(spec.skills.as_ref());
    }

    if !git_ok {
        tracing::warn!(path = %target.display(), "maiden run workspace setup completed with errors; marker omitted to allow retry");
        return Ok(res);
    }
    // A repository cloned into the workspace root moves the marker into its
    // .git directory, so look the path up again.
    write_marker(&workspace_marker_path(&target), ws);
    tracing::info!(path = %target.display(), "maiden run workspace setup completed successfully");
    Ok(res)
}

/// Fetches each repository into the workspace, returning the ones that
/// succeeded and whether all did.
async fn clone_repos(repos: &[GitRepo], target: &Path, ax_dir: &Path) -> (Vec<String>, bool) {
    let mut cloned = Vec::new();
    let mut ok = true;
    for repo in repos.iter().filter(|r| !r.repo.is_empty()) {
        let dest = clone_destination(repo, target);
        if let Err(e) = std::fs::create_dir_all(&dest) {
            tracing::warn!(dir = %dest.display(), error = %e, "failed to create destination dir");
        }
        let branch = if repo.branch.is_empty() {
            DEFAULT_BRANCH
        } else {
            &repo.branch
        };
        tracing::info!(repo = %repo.repo, branch, dir = %dest.display(), depth = repo.depth, "initializing and fetching git repo");

        let mut out = Vec::new();
        let mut result = Err(String::new());
        for attempt in 1..=GIT_RETRIES {
            (out, result) = fetch_repo(&dest, &repo.repo, branch, repo.depth).await;
            match &result {
                Ok(()) => break,
                Err(e) if attempt < GIT_RETRIES => {
                    tracing::warn!(attempt, max = GIT_RETRIES, error = %e, output = %String::from_utf8_lossy(&out), "git operation failed, will retry");
                    tokio::time::sleep(GIT_RETRY_DELAY).await;
                }
                Err(_) => {}
            }
        }
        match result {
            Ok(()) => {
                cloned.push(repo.repo.clone());
                write_state_file(ax_dir, GIT_SUCCESS_LOG, &out);
            }
            Err(e) => {
                ok = false;
                let output = String::from_utf8_lossy(&out);
                tracing::warn!(repo = %repo.repo, error = %e, %output, "git fetch error (continuing setup)");
                write_state_file(
                    ax_dir,
                    GIT_ERROR_LOG,
                    format!("error: {e}\noutput: {output}\n").as_bytes(),
                );
            }
        }
    }
    (cloned, ok)
}

/// Where a repository is checked out.
///
/// An explicit `dir` wins: `.` means the workspace root, an absolute path is
/// used as is, and anything else is relative to the workspace. Otherwise the
/// directory is named after the repository's `name`, unless that is empty or
/// a placeholder such as `origin`, in which case it comes from the URL.
pub fn clone_destination(repo: &GitRepo, target: &Path) -> PathBuf {
    if !repo.dir.is_empty() {
        return match repo.dir.as_str() {
            "." => target.to_owned(),
            d if Path::new(d).is_absolute() => PathBuf::from(d),
            d => target.join(d),
        };
    }
    let mut name = repo.name.clone();
    if name.is_empty() || name == DEFAULT_REPO_DIR_NAME || name == "origin" {
        let derived = repo_dir_name(&repo.repo);
        if !derived.is_empty() {
            name = derived;
        } else if name.is_empty() {
            name = DEFAULT_REPO_DIR_NAME.into();
        }
    }
    target.join(name)
}

/// Initializes `dir` as a repository pointed at `url`, fetches `branch`
/// (shallowly when `depth` > 0) and checks it out. It is idempotent, so it
/// can be retried on a half-initialized directory. Arguments go straight to
/// git, never through a shell.
async fn fetch_repo(
    dir: &Path,
    url: &str,
    branch: &str,
    depth: i32,
) -> (Vec<u8>, Result<(), String>) {
    let mut combined = Vec::new();
    let mut run = async |args: &[&str]| -> Result<(), String> {
        let out = Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .await
            .map_err(|e| format!("git {}: {e}", args[0]))?;
        combined.extend_from_slice(&out.stdout);
        combined.extend_from_slice(&out.stderr);
        if out.status.success() {
            Ok(())
        } else {
            Err(format!("git {}: {}", args[0], out.status))
        }
    };
    let result = async {
        run(&["init"]).await?;
        // A retry finds the remote already there, so update it instead.
        if run(&["remote", "add", "origin", url]).await.is_err() {
            run(&["remote", "set-url", "origin", url]).await?;
        }
        let depth_arg = format!("--depth={depth}");
        let mut fetch = vec!["fetch"];
        if depth > 0 {
            fetch.push(&depth_arg);
        }
        fetch.extend(["origin", branch]);
        run(&fetch).await?;
        run(&["checkout", "-f", "FETCH_HEAD"]).await
    }
    .await;
    (combined, result)
}

fn setup_skills(skills: Option<&SkillsConfig>) -> String {
    let Some(path) = skills.map(|s| &s.path).filter(|p| !p.is_empty()) else {
        return String::new();
    };
    if let Err(e) = std::fs::create_dir_all(path) {
        tracing::warn!(path, error = %e, "creating skills dir");
    }
    path.clone()
}

/// The first-run marker's file name for a workspace mounted at `path`. It is
/// derived from the path, so a workspace mounted elsewhere tracks its setup
/// separately.
pub fn marker_name(path: &str) -> String {
    let path = if path.is_empty() {
        DEFAULT_WORKSPACE_PATH
    } else {
        path
    };
    let cleaned = rax_guest::filesystem::clean(Path::new(path));
    let mut clean = cleaned
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/")
        .trim_matches('/')
        .to_owned();
    if clean.is_empty() {
        clean = "root".into();
    }
    format!("{INITIALIZED_MARKER_FILENAME}-{}", clean.replace('/', "-"))
}

/// Where the first-run marker lives: on the workspace volume, so it survives
/// a volumes-only resume, which does not restore `ax_dir`. For a git
/// repository it sits in the git directory, which `git add -A` and `git clean
/// -fdx` never touch; otherwise it sits in `.ax`.
pub fn workspace_marker_path(target: &Path) -> PathBuf {
    let git_dir = target.join(".git");
    if std::fs::symlink_metadata(&git_dir).is_ok_and(|m| m.is_dir()) {
        return git_dir
            .join(MARKER_DIR_NAME)
            .join(INITIALIZED_MARKER_FILENAME);
    }
    target
        .join(MARKER_DIR_NAME)
        .join(INITIALIZED_MARKER_FILENAME)
}

/// Whether the marker's directory or the marker itself is a symbolic link.
/// The agent can create one, and reading or writing through it would leave
/// the workspace.
fn marker_linked(path: &Path) -> bool {
    [path.parent().unwrap_or(path), path]
        .iter()
        .any(|p| std::fs::symlink_metadata(p).is_ok_and(|m| m.file_type().is_symlink()))
}

/// Whether a legacy marker was written for this workspace. Two paths can
/// share a legacy name (`/a/b` and `/a-b`), so the recorded name is checked
/// too.
fn legacy_owned_by(data: &[u8], ws: Option<&Workspace>) -> bool {
    data.starts_with(format!("workspace: {}\n", workspace_name(ws)).as_bytes())
}

/// The workspace's name as markers record it.
fn workspace_name(ws: Option<&Workspace>) -> &str {
    ws.and_then(|w| w.metadata.as_ref())
        .map(|m| m.name.as_str())
        .filter(|n| !n.is_empty())
        .unwrap_or("unknown")
}

fn write_marker(path: &Path, ws: Option<&Workspace>) {
    let name = workspace_name(ws);
    if let Some(dir) = path.parent()
        && let Err(e) = std::fs::create_dir_all(dir)
    {
        tracing::warn!(path = %path.display(), error = %e, "failed to create initialized marker directory");
        return;
    }
    let now = chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now())
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let content = format!("workspace: {name}\ninitialized_at: {now}\n");
    if let Err(e) = std::fs::write(path, content) {
        tracing::warn!(path = %path.display(), error = %e, "failed to write initialized marker file");
    }
}

fn write_state_file(ax_dir: &Path, name: &str, data: &[u8]) {
    let path = ax_dir.join(name);
    if let Err(e) = std::fs::write(&path, data) {
        tracing::warn!(path = %path.display(), error = %e, "failed to write state file");
    }
}

/// The repository name in a git URL or path: `chalk` for
/// `https://github.com/chalk/chalk.git`.
pub fn repo_dir_name(url: &str) -> String {
    let trimmed = url.trim();
    let trimmed = trimmed.strip_suffix('/').unwrap_or(trimmed);
    let trimmed = trimmed.strip_suffix(".git").unwrap_or(trimmed);
    match trimmed.rfind(['/', ':']) {
        Some(i) => trimmed[i + 1..].to_owned(),
        None => trimmed.to_owned(),
    }
}

fn write_files(files: &[File], target: &Path) {
    for f in files.iter().filter(|f| !f.path.is_empty()) {
        let dest = if Path::new(&f.path).is_absolute() {
            PathBuf::from(&f.path)
        } else {
            target.join(&f.path)
        };
        if let Some(dir) = dest.parent()
            && let Err(e) = std::fs::create_dir_all(dir)
        {
            tracing::warn!(path = %dest.display(), error = %e, "failed to create directory for workspace file");
            continue;
        }
        if let Err(e) = std::fs::write(&dest, &f.content) {
            tracing::warn!(path = %dest.display(), error = %e, "failed to write workspace file");
            continue;
        }
        tracing::info!(path = %dest.display(), "wrote inlined workspace file");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rax_api::v1alpha1::{ObjectMeta, WorkspaceSpec};

    fn workspace(name: &str, spec: WorkspaceSpec) -> Workspace {
        Workspace {
            metadata: Some(ObjectMeta {
                name: name.into(),
                ..Default::default()
            }),
            spec: Some(spec),
            ..Default::default()
        }
    }

    fn git(dir: &Path, script: &str) {
        let out = std::process::Command::new("sh")
            .args(["-c", script])
            .current_dir(dir)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    const INIT: &str = "git init -q && git symbolic-ref HEAD refs/heads/main && git config user.email test@ax.io && git config user.name AX";

    #[tokio::test]
    async fn maiden_run_writes_marker_once() {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("ax-state");
        let ws = workspace(
            "test-ws",
            WorkspaceSpec {
                skills: Some(SkillsConfig {
                    path: tmp.path().join("skills").display().to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        let target = tmp.path().to_str().unwrap();
        let first = setup_workspace(Some(&ws), target, &state).await.unwrap();
        assert!(first.is_maiden_run);
        assert!(tmp.path().join("skills").is_dir());
        let marker = tmp.path().join(".ax").join(INITIALIZED_MARKER_FILENAME);
        let content = std::fs::read_to_string(&marker).unwrap();
        assert!(
            content.starts_with("workspace: test-ws\ninitialized_at: "),
            "{content}"
        );
        // Nothing is left under the state directory to mark the run.
        assert!(!state.join(marker_name(target)).exists());

        let second = setup_workspace(Some(&ws), target, &state).await.unwrap();
        assert!(!second.is_maiden_run);
    }

    #[tokio::test]
    async fn clones_into_subdir_with_depth() {
        let src = tempfile::tempdir().unwrap();
        git(
            src.path(),
            &format!(
                "{INIT} && for i in 1 2 3; do echo $i > f.txt && git add . && git commit -qm c$i; done"
            ),
        );
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("ax-state");
        let ws = workspace(
            "git-ws",
            WorkspaceSpec {
                git: vec![GitRepo {
                    name: "ax".into(),
                    repo: src.path().display().to_string(),
                    depth: 1,
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        let res = setup_workspace(Some(&ws), tmp.path().to_str().unwrap(), &state)
            .await
            .unwrap();
        assert_eq!(res.cloned_repos.len(), 1);
        assert!(tmp.path().join("ax/.git").exists());
        assert!(state.join(GIT_SUCCESS_LOG).exists());
        let count = std::process::Command::new("git")
            .args(["rev-list", "--count", "HEAD"])
            .current_dir(tmp.path().join("ax"))
            .output()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&count.stdout).trim(), "1");
    }

    #[tokio::test(start_paused = true)]
    async fn git_errors_withhold_the_marker() {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("ax-state");
        let ws = workspace(
            "bad",
            WorkspaceSpec {
                git: vec![GitRepo {
                    name: "invalid-repo".into(),
                    repo: tmp.path().join("missing-repo").display().to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        let target = tmp.path().to_str().unwrap();
        let res = setup_workspace(Some(&ws), target, &state).await.unwrap();
        assert!(res.cloned_repos.is_empty());
        assert!(state.join(GIT_ERROR_LOG).exists());
        assert!(
            !tmp.path()
                .join(".ax")
                .join(INITIALIZED_MARKER_FILENAME)
                .exists()
        );
    }

    #[tokio::test]
    async fn writes_inlined_files() {
        let tmp = tempfile::tempdir().unwrap();
        let abs = tmp.path().join("abs.txt");
        let ws = workspace(
            "files-ws",
            WorkspaceSpec {
                files: vec![
                    File {
                        path: "config/settings.json".into(),
                        content: r#"{"env": "production"}"#.into(),
                    },
                    File {
                        path: abs.display().to_string(),
                        content: "absolute".into(),
                    },
                ],
                ..Default::default()
            },
        );
        let ws_dir = tmp.path().join("ws");
        setup_workspace(
            Some(&ws),
            ws_dir.to_str().unwrap(),
            &tmp.path().join("state"),
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(ws_dir.join("config/settings.json")).unwrap(),
            r#"{"env": "production"}"#
        );
        assert_eq!(std::fs::read_to_string(abs).unwrap(), "absolute");
    }

    /// A repository cloned into the workspace root takes the marker into its
    /// new .git directory, where the next run looks for it.
    #[tokio::test]
    async fn marker_follows_root_clone() {
        let src = tempfile::tempdir().unwrap();
        git(
            src.path(),
            &format!("{INIT} && echo hi > README.md && git add . && git commit -qm init"),
        );
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("ax-state");
        let target = tmp.path().join("ws");
        let ws = workspace(
            "root-clone-ws",
            WorkspaceSpec {
                git: vec![GitRepo {
                    name: "root".into(),
                    repo: src.path().display().to_string(),
                    dir: ".".into(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        let target_str = target.to_str().unwrap();
        setup_workspace(Some(&ws), target_str, &state)
            .await
            .unwrap();
        assert!(
            target
                .join(".git/.ax")
                .join(INITIALIZED_MARKER_FILENAME)
                .exists()
        );
        assert_eq!(
            workspace_marker_path(&target),
            target.join(".git/.ax").join(INITIALIZED_MARKER_FILENAME)
        );
        let res = setup_workspace(Some(&ws), target_str, &state)
            .await
            .unwrap();
        assert!(!res.is_maiden_run, "the second run did not find the marker");
    }

    fn files_ws(name: &str) -> Workspace {
        workspace(
            name,
            WorkspaceSpec {
                files: vec![File {
                    path: "x.txt".into(),
                    content: "x".into(),
                }],
                ..Default::default()
            },
        )
    }

    /// A legacy marker in the state directory written for this workspace is
    /// migrated, and setup does not rerun.
    #[tokio::test]
    async fn migrates_own_legacy_marker() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("ws");
        let state = tmp.path().join("ax-state");
        std::fs::create_dir_all(&state).unwrap();
        let target_str = target.to_str().unwrap();
        std::fs::write(state.join(marker_name(target_str)), "workspace: own-ws\n").unwrap();

        let res = setup_workspace(Some(&files_ws("own-ws")), target_str, &state)
            .await
            .unwrap();
        assert!(!res.is_maiden_run);
        assert!(
            target
                .join(".ax")
                .join(INITIALIZED_MARKER_FILENAME)
                .exists(),
            "legacy marker not migrated"
        );
        assert!(
            !target.join("x.txt").exists(),
            "setup reran although the legacy marker was this workspace's"
        );
    }

    /// A legacy marker with the same name but written for another workspace
    /// does not suppress setup.
    #[tokio::test]
    async fn ignores_legacy_marker_of_other_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("ws");
        let state = tmp.path().join("ax-state");
        std::fs::create_dir_all(&state).unwrap();
        let target_str = target.to_str().unwrap();
        std::fs::write(state.join(marker_name(target_str)), "workspace: other-ws\n").unwrap();

        let res = setup_workspace(Some(&files_ws("this-ws")), target_str, &state)
            .await
            .unwrap();
        assert!(res.is_maiden_run);
        assert!(
            target.join("x.txt").exists(),
            "setup was skipped because of another workspace's legacy marker"
        );
    }

    /// A .ax directory that links outside the workspace is refused, and
    /// nothing is written through it.
    #[cfg(unix)]
    #[tokio::test]
    async fn refuses_linked_marker_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let outside = tmp.path().join("outside");
        let target = tmp.path().join("ws");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::create_dir_all(&target).unwrap();
        std::os::unix::fs::symlink(&outside, target.join(".ax")).unwrap();

        let ws = workspace("linked-ws", WorkspaceSpec::default());
        let err = setup_workspace(
            Some(&ws),
            target.to_str().unwrap(),
            &tmp.path().join("ax-state"),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("symbolic link"), "{err}");
        assert!(!outside.join(INITIALIZED_MARKER_FILENAME).exists());
    }

    /// A marker that is itself a link is refused too.
    #[cfg(unix)]
    #[tokio::test]
    async fn refuses_linked_marker() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("ws");
        std::fs::create_dir_all(target.join(".ax")).unwrap();
        let elsewhere = tmp.path().join("elsewhere");
        std::fs::write(&elsewhere, "workspace: linked-ws\n").unwrap();
        std::os::unix::fs::symlink(
            &elsewhere,
            target.join(".ax").join(INITIALIZED_MARKER_FILENAME),
        )
        .unwrap();

        let ws = workspace("linked-ws", WorkspaceSpec::default());
        assert!(
            setup_workspace(
                Some(&ws),
                target.to_str().unwrap(),
                &tmp.path().join("ax-state"),
            )
            .await
            .is_err()
        );
    }

    #[test]
    fn marker_names() {
        for (path, want) in [
            ("/workspace", "initialized-workspace"),
            ("/workspace/", "initialized-workspace"),
            ("", "initialized-workspace"),
            ("/", "initialized-root"),
            ("/workspace/tools", "initialized-workspace-tools"),
            ("/workspace/a/b", "initialized-workspace-a-b"),
            ("/srv/data", "initialized-srv-data"),
        ] {
            assert_eq!(marker_name(path), want, "{path}");
        }
    }

    #[test]
    fn destinations() {
        let t = Path::new("/workspace");
        let repo = |name: &str, url: &str, dir: &str| GitRepo {
            name: name.into(),
            repo: url.into(),
            dir: dir.into(),
            ..Default::default()
        };
        for (r, want) in [
            (repo("x", "u", "."), "/workspace"),
            (repo("x", "u", "/opt/src"), "/opt/src"),
            (repo("x", "u", "sub/dir"), "/workspace/sub/dir"),
            (repo("tools", "https://h/a/b.git", ""), "/workspace/tools"),
            (
                repo("origin", "https://github.com/chalk/chalk.git", ""),
                "/workspace/chalk",
            ),
            (
                repo("", "git@github.com:org/proj.git/", ""),
                "/workspace/proj",
            ),
            (repo("", "", ""), "/workspace/repo"),
        ] {
            assert_eq!(clone_destination(&r, t), PathBuf::from(want), "{r:?}");
        }
    }
}
