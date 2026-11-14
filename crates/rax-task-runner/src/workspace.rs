//! Workspace setup on a task's first run: git checkouts at their ref, inlined
//! files, the skills directory and setup commands, with a marker recording
//! that it is done.

use std::path::{Path, PathBuf};
use std::time::Duration;

use rax_api::DEFAULT_WORKSPACE_PATH;
use rax_api::v1alpha1::{File, GitRepo, SkillsConfig, Workspace};
use tokio::process::Command;

/// The default directory for AX's own state and logs inside the container.
pub const DEFAULT_AX_DIR: &str = "/ax";
/// The base name of the marker written after a successful first run.
pub const INITIALIZED_MARKER_FILENAME: &str = "initialized";
/// The directory under the workspace that holds the marker and the default
/// skills directory.
const MARKER_DIR_NAME: &str = ".ax";
const DEFAULT_SKILLS_SUBDIR: &str = "skills";
/// How long one `spec.setup.commands` entry may run, so that a hung command
/// fails setup instead of leaving the workspace unready forever.
pub const DEFAULT_SETUP_COMMAND_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// How much of a failed command's output its error keeps.
const MAX_COMMAND_OUTPUT_IN_ERROR: usize = 2048;

const DEFAULT_GIT_REF: &str = "main";
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
    pub commands_run: usize,
    pub is_maiden_run: bool,
}

/// Prepares the workspace at `target` according to `spec.setup` on its first
/// run:
///  1. clones the declared repositories at their ref;
///  2. writes the inlined files;
///  3. creates the skills directory;
///  4. runs the setup commands one after another.
///
/// A marker on the workspace volume (see [`workspace_marker_path`]) records a
/// finished first run, so later calls do nothing. Any failure withholds the
/// marker and is returned, so the workspace is not marked ready and the next
/// start retries setup.
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
        return Err(linked_marker(&marker));
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
        write_marker(&workspace_marker_path(&target), ws)
            .map_err(|e| wrap(e, "writing initialization marker"))?;
        return Ok(res);
    }

    tracing::info!(path = %target.display(), "executing workspace maiden run setup");
    res.is_maiden_run = true;

    if let Some(setup) = ws
        .and_then(|w| w.spec.as_ref())
        .and_then(|s| s.setup.as_ref())
    {
        clone_repos(&setup.git, &target, ax_dir, &mut res.cloned_repos).await?;
        write_files(&setup.files, &target)?;
        res.skills_mounted = setup_skills(setup.skills.as_ref(), &target)?;
        run_setup_commands(&setup.commands, &target, &mut res.commands_run).await?;
    }

    // A repository cloned into the workspace root moves the marker into its
    // .git directory, so look the path up again.
    let marker = workspace_marker_path(&target);
    if marker_linked(&marker) {
        return Err(linked_marker(&marker));
    }
    write_marker(&marker, ws).map_err(|e| {
        wrap(
            e,
            &format!("writing initialization marker {}", marker.display()),
        )
    })?;
    tracing::info!(path = %target.display(), "maiden run workspace setup completed successfully");
    Ok(res)
}

fn linked_marker(marker: &Path) -> std::io::Error {
    std::io::Error::other(format!(
        "marker path {} is a symbolic link; refusing to trust or write it",
        marker.display()
    ))
}

fn wrap(e: std::io::Error, what: &str) -> std::io::Error {
    std::io::Error::new(e.kind(), format!("{what}: {e}"))
}

/// Fetches each repository into the workspace, recording those that
/// succeeded in `cloned`. The first that fails, after retries, fails setup.
async fn clone_repos(
    repos: &[GitRepo],
    target: &Path,
    ax_dir: &Path,
    cloned: &mut Vec<String>,
) -> std::io::Result<()> {
    for repo in repos.iter().filter(|r| !r.repo.is_empty()) {
        let dest = clone_destination(repo, target)?;
        std::fs::create_dir_all(&dest).map_err(|e| {
            wrap(
                e,
                &format!("creating git checkout directory {}", dest.display()),
            )
        })?;
        let git_ref = if repo.r#ref.is_empty() {
            DEFAULT_GIT_REF
        } else {
            &repo.r#ref
        };
        tracing::info!(repo = %repo.repo, git_ref, dir = %dest.display(), depth = repo.depth, "initializing and fetching git repo");

        let mut out = Vec::new();
        let mut result = Err(String::new());
        for attempt in 1..=GIT_RETRIES {
            (out, result) = fetch_repo(&dest, &repo.repo, git_ref, repo.depth).await;
            match &result {
                Ok(()) => break,
                Err(e) if attempt < GIT_RETRIES => {
                    tracing::warn!(attempt, max = GIT_RETRIES, error = %e, output = %String::from_utf8_lossy(&out), "git operation failed, will retry");
                    tokio::time::sleep(GIT_RETRY_DELAY).await;
                }
                Err(_) => {}
            }
        }
        if let Err(e) = result {
            let output = String::from_utf8_lossy(&out);
            write_state_file(
                ax_dir,
                GIT_ERROR_LOG,
                format!("error: {e}\noutput: {output}\n").as_bytes(),
            );
            return Err(std::io::Error::other(format!(
                "cloning {} (ref {git_ref}): {e}",
                repo.repo
            )));
        }
        cloned.push(repo.repo.clone());
        write_state_file(ax_dir, GIT_SUCCESS_LOG, &out);
    }
    Ok(())
}

/// Where a repository is checked out.
///
/// An explicit `dir` wins: `.` means the workspace root, and anything else
/// must be relative to the workspace and stay within it. Otherwise the
/// directory is named after the repository's `name`, unless that is empty or
/// a placeholder such as `origin`, in which case it comes from the URL.
pub fn clone_destination(repo: &GitRepo, target: &Path) -> std::io::Result<PathBuf> {
    let mut rel = repo.dir.clone();
    if rel.is_empty() {
        let mut name = repo.name.clone();
        if name.is_empty() || name == DEFAULT_REPO_DIR_NAME || name == "origin" {
            let derived = repo_dir_name(&repo.repo);
            if !derived.is_empty() {
                name = derived;
            } else if name.is_empty() {
                name = DEFAULT_REPO_DIR_NAME.into();
            }
        }
        rel = name;
    }
    if rel == "." {
        return Ok(target.to_owned());
    }
    if !is_relative_within_root(&rel) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "git checkout path {rel:?} must stay within workspace {}",
                target.display()
            ),
        ));
    }
    Ok(target.join(rel))
}

/// Initializes `dir` as a repository pointed at `url`, fetches `git_ref` (a
/// branch, tag or commit SHA; shallowly when `depth` > 0) and checks it out.
/// It is idempotent, so it can be retried on a half-initialized directory.
/// Arguments go straight to git, never through a shell.
async fn fetch_repo(
    dir: &Path,
    url: &str,
    git_ref: &str,
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
        if run(&["remote", "add", "--", "origin", url]).await.is_err() {
            run(&["remote", "set-url", "--", "origin", url]).await?;
        }
        let depth_arg = format!("--depth={depth}");
        let mut fetch = vec!["fetch"];
        if depth > 0 {
            fetch.push(&depth_arg);
        }
        fetch.extend(["--", "origin", git_ref]);
        if let Err(e) = run(&fetch).await {
            // Servers that refuse to fetch an unadvertised, abbreviated commit
            // SHA directly need a full fetch and then a checkout of the SHA.
            if !looks_like_commit_sha(git_ref) {
                return Err(e);
            }
            run(&["fetch", "--", "origin"]).await?;
            return run(&["checkout", "-f", git_ref, "--"]).await;
        }
        run(&["checkout", "-f", "FETCH_HEAD", "--"]).await
    }
    .await;
    (combined, result)
}

fn looks_like_commit_sha(s: &str) -> bool {
    (7..=40).contains(&s.len()) && s.bytes().all(|c| c.is_ascii_hexdigit())
}

/// Creates the skills directory when one is declared or registries are
/// configured, returning its path. Without a path, registries get
/// `.ax/skills` under the workspace.
fn setup_skills(skills: Option<&SkillsConfig>, target: &Path) -> std::io::Result<String> {
    let Some(skills) = skills else {
        return Ok(String::new());
    };
    let dest = match skills.path.as_str() {
        "" if skills.registries.is_empty() => return Ok(String::new()),
        "" => target.join(MARKER_DIR_NAME).join(DEFAULT_SKILLS_SUBDIR),
        p if Path::new(p).is_absolute() => PathBuf::from(p),
        p if !is_relative_within_root(p) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("skills path {p:?} escapes workspace {}", target.display()),
            ));
        }
        p => target.join(p),
    };
    std::fs::create_dir_all(&dest)
        .map_err(|e| wrap(e, &format!("creating skills dir {}", dest.display())))?;
    if !skills.registries.is_empty() {
        // Fetching skill bundles from registries is not implemented yet, as
        // in AX.
        tracing::info!(path = %dest.display(), registries = skills.registries.len(), "prepared skills directory; registry skill download not implemented yet");
    }
    Ok(dest.to_string_lossy().into_owned())
}

/// Runs the setup commands one after another in the workspace, counting
/// those that succeeded in `ran`. The first that fails or times out fails
/// setup.
async fn run_setup_commands(
    commands: &[String],
    target: &Path,
    ran: &mut usize,
) -> std::io::Result<()> {
    for (i, cmd) in commands.iter().enumerate() {
        if cmd.trim().is_empty() {
            continue;
        }
        tracing::info!(index = i, command = %cmd, dir = %target.display(), "running workspace setup command");
        let (out, result) =
            run_setup_command_with_timeout(cmd, target, DEFAULT_SETUP_COMMAND_TIMEOUT).await;
        if let Err(e) = result {
            let tail = &out[out.len().saturating_sub(MAX_COMMAND_OUTPUT_IN_ERROR)..];
            let snippet = String::from_utf8_lossy(tail).replace('\u{FFFD}', "");
            let snippet = snippet.trim();
            let msg = if snippet.is_empty() {
                format!("running setup.commands[{i}] ({cmd:?}): {e}")
            } else {
                format!("running setup.commands[{i}] ({cmd:?}): {snippet}: {e}")
            };
            return Err(std::io::Error::other(msg));
        }
        *ran += 1;
    }
    Ok(())
}

/// Runs one setup command with `sh -c` in `dir`, returning its combined
/// output. The shell gets its own process group, so a timeout kills
/// backgrounded children that hold its output open too.
pub async fn run_setup_command_with_timeout(
    cmd: &str,
    dir: &Path,
    timeout: Duration,
) -> (Vec<u8>, Result<(), String>) {
    let child = Command::new("sh")
        .args(["-c", cmd])
        .current_dir(dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .process_group(0)
        .kill_on_drop(true)
        .spawn();
    let child = match child {
        Ok(c) => c,
        Err(e) => return (Vec::new(), Err(e.to_string())),
    };
    let pid = child.id().map_or(0, |p| p as i32);
    match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(Ok(out)) => {
            let mut combined = out.stdout;
            combined.extend_from_slice(&out.stderr);
            let result = if out.status.success() {
                Ok(())
            } else {
                Err(out.status.to_string())
            };
            (combined, result)
        }
        Ok(Err(e)) => (Vec::new(), Err(e.to_string())),
        Err(_) => {
            if pid > 0 {
                // SAFETY: kill has no memory-safety preconditions.
                unsafe { libc::kill(-pid, libc::SIGKILL) };
            }
            (
                Vec::new(),
                Err(format!("timed out after {}", humantime(timeout))),
            )
        }
    }
}

fn humantime(d: Duration) -> String {
    let s = d.as_secs();
    match (s / 60, s % 60) {
        (0, _) if d.subsec_millis() > 0 && s == 0 => format!("{}ms", d.as_millis()),
        (0, s) => format!("{s}s"),
        (m, 0) => format!("{m}m0s"),
        (m, s) => format!("{m}m{s}s"),
    }
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

fn write_marker(path: &Path, ws: Option<&Workspace>) -> std::io::Result<()> {
    let name = workspace_name(ws);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let now = chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now())
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    std::fs::write(path, format!("workspace: {name}\ninitialized_at: {now}\n"))
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
    let name = match trimmed.rfind(['/', ':']) {
        Some(i) => &trimmed[i + 1..],
        None => trimmed,
    };
    if name == "." || name == ".." {
        return String::new();
    }
    name.to_owned()
}

/// Writes the inlined files into the workspace. Each path must name a file
/// within the workspace.
fn write_files(files: &[File], target: &Path) -> std::io::Result<()> {
    for f in files.iter().filter(|f| !f.path.trim().is_empty()) {
        let rel = f.path.as_str();
        if rel.ends_with('/') || rax_guest::filesystem::clean(Path::new(rel)) == Path::new(".") {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("workspace file path {rel:?} must name a file, not a directory"),
            ));
        }
        if !is_relative_within_root(rel) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "workspace file path {rel:?} must be relative to workspace {}",
                    target.display()
                ),
            ));
        }
        let dest = target.join(rel);
        if let Some(dir) = dest.parent() {
            std::fs::create_dir_all(dir).map_err(|e| {
                wrap(
                    e,
                    &format!("creating directory for workspace file {}", dest.display()),
                )
            })?;
        }
        std::fs::write(&dest, &f.content)
            .map_err(|e| wrap(e, &format!("writing workspace file {}", dest.display())))?;
        tracing::info!(path = %dest.display(), "wrote inlined workspace file");
    }
    Ok(())
}

/// Whether `p` is a relative path that stays within the directory it is
/// relative to.
fn is_relative_within_root(p: &str) -> bool {
    if Path::new(p).is_absolute() {
        return false;
    }
    let clean = rax_guest::filesystem::clean(Path::new(p));
    let clean = clean.to_string_lossy();
    clean != ".." && !clean.starts_with("../")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rax_api::v1alpha1::{ObjectMeta, WorkspaceSetup, WorkspaceSpec};

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
                setup: Some(WorkspaceSetup {
                    skills: Some(SkillsConfig {
                        path: tmp.path().join("skills").display().to_string(),
                        ..Default::default()
                    }),
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
                setup: Some(WorkspaceSetup {
                    git: vec![GitRepo {
                        name: "ax".into(),
                        repo: src.path().display().to_string(),
                        depth: 1,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
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
                setup: Some(WorkspaceSetup {
                    git: vec![GitRepo {
                        name: "invalid-repo".into(),
                        repo: tmp.path().join("missing-repo").display().to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        let target = tmp.path().to_str().unwrap();
        let err = setup_workspace(Some(&ws), target, &state)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("missing-repo (ref main)"), "{err}");
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
        let ws = workspace(
            "files-ws",
            WorkspaceSpec {
                setup: Some(WorkspaceSetup {
                    files: vec![File {
                        path: "config/settings.json".into(),
                        content: r#"{"env": "production"}"#.into(),
                    }],
                    ..Default::default()
                }),
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
    }

    fn setup_ws(name: &str, setup: WorkspaceSetup) -> Workspace {
        workspace(
            name,
            WorkspaceSpec {
                setup: Some(setup),
                ..Default::default()
            },
        )
    }

    /// Paths that leave the workspace, and file paths naming directories,
    /// fail setup.
    #[tokio::test]
    async fn rejects_escaping_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("state");
        let file = |p: &str| WorkspaceSetup {
            files: vec![File {
                path: p.into(),
                content: "x".into(),
            }],
            ..Default::default()
        };
        let skills = |p: &str| WorkspaceSetup {
            skills: Some(SkillsConfig {
                path: p.into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let cases = [
            (file("../escape.txt"), "must be relative"),
            (file("/etc/passwd"), "must be relative"),
            (file("."), "must name a file"),
            (file("dir/"), "must name a file"),
            (skills("../skills"), "escapes workspace"),
        ];
        for (i, (setup, want)) in cases.into_iter().enumerate() {
            let target = tmp.path().join(format!("ws-{i}"));
            let err = setup_workspace(
                Some(&setup_ws("esc", setup)),
                target.to_str().unwrap(),
                &state,
            )
            .await
            .unwrap_err();
            assert!(err.to_string().contains(want), "{err}");
            assert!(!workspace_marker_path(&target).exists());
        }
        assert!(!tmp.path().join("escape.txt").exists());
    }

    /// Skill registries without a path get .ax/skills; a relative path is
    /// under the workspace.
    #[tokio::test]
    async fn skills_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("state");
        let registries = vec![rax_api::v1alpha1::SkillRegistry::default()];
        for (path, regs, want) in [
            ("", registries.clone(), Some(".ax/skills")),
            ("", vec![], None),
            ("my-skills", vec![], Some("my-skills")),
        ] {
            let target = tmp.path().join(format!("ws-{path}-{}", regs.len()));
            let setup = WorkspaceSetup {
                skills: Some(SkillsConfig {
                    path: path.into(),
                    registries: regs,
                }),
                ..Default::default()
            };
            let res = setup_workspace(
                Some(&setup_ws("skills", setup)),
                target.to_str().unwrap(),
                &state,
            )
            .await
            .unwrap();
            match want {
                Some(rel) => {
                    assert_eq!(res.skills_mounted, target.join(rel).display().to_string());
                    assert!(target.join(rel).is_dir());
                }
                None => assert_eq!(res.skills_mounted, ""),
            }
        }
    }

    #[tokio::test]
    async fn runs_setup_commands() {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("state");
        let ok = tmp.path().join("ok");
        let res = setup_workspace(
            Some(&setup_ws(
                "cmd-ok",
                WorkspaceSetup {
                    files: vec![File {
                        path: "deps.txt".into(),
                        content: "pkg-a\n".into(),
                    }],
                    commands: vec![
                        "cat deps.txt > installed.txt".into(),
                        " ".into(),
                        "echo done >> installed.txt".into(),
                    ],
                    ..Default::default()
                },
            )),
            ok.to_str().unwrap(),
            &state,
        )
        .await
        .unwrap();
        assert_eq!(res.commands_run, 2);
        assert_eq!(
            std::fs::read_to_string(ok.join("installed.txt")).unwrap(),
            "pkg-a\ndone\n"
        );

        let fail = tmp.path().join("fail");
        let err = setup_workspace(
            Some(&setup_ws(
                "cmd-fail",
                WorkspaceSetup {
                    commands: vec!["echo boom >&2 && exit 7".into()],
                    ..Default::default()
                },
            )),
            fail.to_str().unwrap(),
            &state,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("setup.commands[0]"), "{err}");
        assert!(err.to_string().contains("boom"), "{err}");
        assert!(!workspace_marker_path(&fail).exists());
    }

    /// A timed-out command is killed with its whole group, so a background
    /// child holding the output open does not hang setup.
    #[tokio::test]
    async fn setup_command_timeout_kills_the_group() {
        let tmp = tempfile::tempdir().unwrap();
        let start = std::time::Instant::now();
        let (_, result) = run_setup_command_with_timeout(
            "sleep 30 & sleep 30",
            tmp.path(),
            Duration::from_millis(200),
        )
        .await;
        assert!(result.unwrap_err().contains("timed out"));
        assert!(start.elapsed() < Duration::from_secs(10));
    }

    /// A ref can be a branch, a tag or a commit SHA, full or abbreviated.
    #[tokio::test]
    async fn checks_out_branch_tag_and_sha() {
        let src = tempfile::tempdir().unwrap();
        git(
            src.path(),
            &format!(
                "{INIT} && echo one > f.txt && git add . && git commit -qm one && git tag v1 \
                 && git checkout -qb feature && echo two > f.txt && git commit -qam two \
                 && git checkout -q main && echo three > f.txt && git commit -qam three"
            ),
        );
        let sha = |rev: &str| {
            let out = std::process::Command::new("git")
                .args(["rev-parse", rev])
                .current_dir(src.path())
                .output()
                .unwrap();
            String::from_utf8_lossy(&out.stdout).trim().to_owned()
        };
        let one = sha("v1");
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().join("state");
        for (i, (r#ref, want)) in [
            ("", "three"),
            ("feature", "two"),
            ("v1", "one"),
            (one.as_str(), "one"),
            (&one[..10], "one"),
        ]
        .into_iter()
        .enumerate()
        {
            let target = tmp.path().join(format!("ws-{i}"));
            let setup = WorkspaceSetup {
                git: vec![GitRepo {
                    name: "src".into(),
                    repo: src.path().display().to_string(),
                    r#ref: r#ref.into(),
                    ..Default::default()
                }],
                ..Default::default()
            };
            setup_workspace(
                Some(&setup_ws("refs", setup)),
                target.to_str().unwrap(),
                &state,
            )
            .await
            .unwrap_or_else(|e| panic!("ref {ref:?}: {e}"));
            assert_eq!(
                std::fs::read_to_string(target.join("src/f.txt"))
                    .unwrap()
                    .trim(),
                want,
                "ref {ref:?}"
            );
        }
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
                setup: Some(WorkspaceSetup {
                    git: vec![GitRepo {
                        name: "root".into(),
                        repo: src.path().display().to_string(),
                        dir: ".".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
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
                setup: Some(WorkspaceSetup {
                    files: vec![File {
                        path: "x.txt".into(),
                        content: "x".into(),
                    }],
                    ..Default::default()
                }),
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
            assert_eq!(
                clone_destination(&r, t).unwrap(),
                PathBuf::from(want),
                "{r:?}"
            );
        }
        for dir in ["/opt/src", "../up", "a/../../b"] {
            assert!(clone_destination(&repo("x", "u", dir), t).is_err(), "{dir}");
        }
    }
}
