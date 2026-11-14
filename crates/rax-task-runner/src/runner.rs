//! The lifecycle of a task container: start the metadata and guest server,
//! set up the workspace, then run and supervise the task's command.

use std::collections::HashMap;
use std::future::Future;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use rax_api::EGRESS_TRUST_BUNDLE_PATH;
use rax_api::v1alpha1::{Task, TaskSpec, Workspace};
use tokio::process::{Child, Command};

use crate::metadata::{Server, ServerOptions};
use crate::workspace::{DEFAULT_AX_DIR, setup_workspace};

/// Where the probe and guest server listens by default: the port Agent
/// Substrate probes for readiness.
pub const DEFAULT_PORT: u16 = 80;
/// The loopback port for the metadata endpoints by default.
pub const DEFAULT_METADATA_PORT: u16 = 8099;

/// How long a running command gets to exit after SIGTERM on shutdown before
/// it is killed.
const STOP_GRACE_PERIOD: Duration = Duration::from_secs(10);

/// The variables through which common tools find their CA certificates.
pub const CA_BUNDLE_ENV: [&str; 5] = [
    "SSL_CERT_FILE",
    "CURL_CA_BUNDLE",
    "REQUESTS_CA_BUNDLE",
    "NODE_EXTRA_CA_CERTS",
    "GIT_SSL_CAINFO",
];

/// The name under which git starts this program to ask for the credentials
/// of an HTTPS remote. The program answers with a placeholder, which is
/// swapped for the real credentials outside the sandbox. git asks only after
/// a server demands credentials, so public repositories are unaffected.
pub const GIT_ASKPASS: &str = "git-askpass";
/// What the program answers when started as [`GIT_ASKPASS`].
pub const GIT_ASKPASS_PLACEHOLDER: &str = "ax-placeholder";

/// When the program was started as [`GIT_ASKPASS`], prints the placeholder
/// credential and exits. git reads the answer from standard output, so
/// `main` calls this before it can print or fail.
pub fn answer_git_askpass() {
    let argv0 = std::env::args_os().next().unwrap_or_default();
    if Path::new(&argv0).file_name() == Some(GIT_ASKPASS.as_ref()) {
        println!("{GIT_ASKPASS_PLACEHOLDER}");
        std::process::exit(0);
    }
}

/// Links `dir/git-askpass` to the running program and returns the link, or
/// `None` when it cannot be made.
pub fn link_git_askpass(dir: &Path) -> Option<PathBuf> {
    let link = dir.join(GIT_ASKPASS);
    let made = std::env::current_exe().and_then(|exe| {
        std::fs::create_dir_all(dir)?;
        let _ = std::fs::remove_file(&link);
        std::os::unix::fs::symlink(exe, &link)
    });
    match made {
        Ok(()) => Some(link),
        Err(e) => {
            tracing::warn!(path = %link.display(), error = %e, "failed to link git-askpass");
            None
        }
    }
}

/// The variables the runner gives every task, except those the task's
/// `spec.env` sets:
///  - the CA variables, pointed at `ca_bundle` when that file exists, so
///    that TLS intercepted on its way out of the sandbox verifies;
///  - `GIT_ASKPASS`, pointed at `askpass` when there is one, and
///    `GIT_TERMINAL_PROMPT=0`, so git never waits for someone to type.
pub fn default_env(
    spec: &TaskSpec,
    ca_bundle: &Path,
    askpass: Option<&Path>,
) -> Vec<(String, String)> {
    let mut env = vec![("GIT_TERMINAL_PROMPT".to_owned(), "0".to_owned())];
    if let Some(a) = askpass {
        env.push(("GIT_ASKPASS".into(), a.to_string_lossy().into_owned()));
    }
    if ca_bundle.exists() {
        let bundle = ca_bundle.to_string_lossy().into_owned();
        env.extend(CA_BUNDLE_ENV.map(|n| (n.to_owned(), bundle.clone())));
    }
    env.retain(|(n, _)| !spec.env.iter().any(|e| &e.name == n));
    env
}

/// How the task command finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandExit {
    pub pid: i32,
    /// The exit status, or -1 when the command was killed by a signal.
    pub exit_code: i32,
    /// `None` when the command exited with status zero.
    pub error: Option<String>,
}

/// Controls one [`run`].
#[derive(Default)]
pub struct Config {
    /// Where the probe and guest server listens. Zero means [`DEFAULT_PORT`].
    pub port: u16,
    /// The loopback port for the metadata endpoints the command reads through
    /// `AX_METADATA_URL`. Zero means [`DEFAULT_METADATA_PORT`].
    pub metadata_port: u16,
    /// The task to run. `None` runs no command, with default settings.
    pub task: Option<Task>,
    /// The workspace the task binds. `None` sets up an empty directory.
    pub workspace: Option<Workspace>,
    /// Where AX keeps its state and logs. `None` means [`DEFAULT_AX_DIR`].
    pub ax_dir: Option<PathBuf>,
    /// A file of CA certificates to trust for outbound TLS. When it exists,
    /// the CA variables common tools read point at it, unless the task sets
    /// them. `None` means [`EGRESS_TRUST_BUNDLE_PATH`].
    pub ca_bundle: Option<PathBuf>,
    /// Called once the task command has exited.
    pub on_command_exit: Option<Box<dyn FnOnce(CommandExit) + Send>>,
}

/// Runs the task-runner lifecycle until `shutdown` completes.
///
/// The workspace is set up at the task's workspace path (`/workspace` unless
/// the binding says otherwise). The task's `spec.commands`, if any, then
/// runs as a child in its own process group, in that directory, with
/// `AX_METADATA_URL`, the [`default_env`] and `spec.env` added to its
/// environment; guest processes get the same variables. The server keeps
/// serving whether or not the command is still running. On shutdown a
/// running command gets SIGTERM, a grace period, then SIGKILL.
///
/// Errors are returned when workspace setup fails, before the command
/// starts, or when the runner itself cannot start, such as when a listener
/// cannot bind or the command cannot launch.
pub async fn run(cfg: Config, shutdown: impl Future<Output = ()>) -> Result<(), String> {
    let port = if cfg.port == 0 {
        DEFAULT_PORT
    } else {
        cfg.port
    };
    let metadata_port = if cfg.metadata_port == 0 {
        DEFAULT_METADATA_PORT
    } else {
        cfg.metadata_port
    };
    let spec = cfg
        .task
        .as_ref()
        .and_then(|t| t.spec.clone())
        .unwrap_or_default();
    let ws_path = spec.workspace_path().to_owned();
    let ws_name = spec
        .workspace
        .as_ref()
        .map(|w| w.name.clone())
        .unwrap_or_default();
    let ax_dir = cfg.ax_dir.unwrap_or_else(|| PathBuf::from(DEFAULT_AX_DIR));
    tracing::info!(port, workspace = %ws_name, ws_path = %ws_path, "initializing AX task runner");

    let ca_bundle = cfg
        .ca_bundle
        .clone()
        .unwrap_or_else(|| PathBuf::from(EGRESS_TRUST_BUNDLE_PATH));
    let askpass = link_git_askpass(&ax_dir);
    let mut task_env = default_env(&spec, &ca_bundle, askpass.as_deref());
    task_env.extend(spec.env.iter().map(|e| (e.name.clone(), e.value.clone())));
    let guest_env: HashMap<String, String> = task_env.iter().cloned().collect();

    let server = Server::start(
        port,
        cfg.task.clone(),
        cfg.workspace.clone(),
        ServerOptions {
            workspace_path: Some(ws_path.clone()),
            log_dir: None,
            metadata_port,
            guest_env,
        },
    )
    .await
    .map_err(|e| format!("starting metadata server: {e}"))?;
    let metadata_url = server.metadata_url();

    let result = supervise(
        &server,
        cfg.workspace.as_ref(),
        &spec.commands,
        &ws_path,
        &ws_name,
        &ax_dir,
        &metadata_url,
        &task_env,
        cfg.on_command_exit,
        shutdown,
    )
    .await;
    tracing::info!("shutting down metadata server");
    server.stop().await;
    result
}

#[allow(clippy::too_many_arguments)]
async fn supervise(
    server: &Server,
    workspace: Option<&Workspace>,
    command: &[String],
    ws_path: &str,
    ws_name: &str,
    ax_dir: &std::path::Path,
    metadata_url: &str,
    task_env: &[(String, String)],
    on_exit: Option<Box<dyn FnOnce(CommandExit) + Send>>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    setup_workspace(workspace, ws_path, ax_dir)
        .await
        .map_err(|e| format!("setting up workspace {ws_name:?}: {e}"))?;
    server.set_workspace_ready(true);
    tracing::info!(path = ws_path, "workspace maiden run setup marked ready");

    let Some((program, args)) = command.split_first() else {
        tracing::info!("no task command specified; serving metadata until stopped");
        shutdown.await;
        return Ok(());
    };

    let mut child = Command::new(program)
        .args(args)
        .current_dir(ws_path)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .env("AX_METADATA_URL", metadata_url)
        .envs(task_env.iter().cloned())
        // Its own group, so shutdown signals the command and everything it
        // started.
        .process_group(0)
        .spawn()
        .map_err(|e| format!("starting task command {command:?}: {e}"))?;
    let pid = child.id().map_or(0, |p| p as i32);
    tracing::info!(pid, ?command, "started task command");

    tokio::pin!(shutdown);
    tokio::select! {
        status = child.wait() => {
            report_exit(pid, status, on_exit);
            // Keep the sandbox up and inspectable until told to stop.
            shutdown.await;
        }
        _ = &mut shutdown => {
            let status = stop_command(&mut child, pid).await;
            report_exit(pid, status, on_exit);
        }
    }
    Ok(())
}

/// Sends SIGTERM to the command's group, then SIGKILL if it is still running
/// after the grace period.
async fn stop_command(child: &mut Child, pid: i32) -> std::io::Result<ExitStatus> {
    tracing::info!(pid, "stopping task command");
    // SAFETY: kill has no memory-safety preconditions.
    unsafe { libc::kill(-pid, libc::SIGTERM) };
    match tokio::time::timeout(STOP_GRACE_PERIOD, child.wait()).await {
        Ok(status) => status,
        Err(_) => {
            tracing::warn!(pid, grace_period = ?STOP_GRACE_PERIOD, "task command did not exit within grace period; killing");
            // SAFETY: as above.
            unsafe { libc::kill(-pid, libc::SIGKILL) };
            child.wait().await
        }
    }
}

fn report_exit(
    pid: i32,
    status: std::io::Result<ExitStatus>,
    on_exit: Option<Box<dyn FnOnce(CommandExit) + Send>>,
) {
    let exit = match status {
        Ok(s) => CommandExit {
            pid,
            exit_code: s.code().unwrap_or(-1),
            error: (!s.success()).then(|| match s.signal() {
                Some(sig) => format!("signal: {}", signal_name(sig)),
                None => format!("exit status {}", s.code().unwrap_or(-1)),
            }),
        },
        Err(e) => CommandExit {
            pid,
            exit_code: -1,
            error: Some(e.to_string()),
        },
    };
    match &exit.error {
        Some(e) => {
            tracing::error!(pid, exit_code = exit.exit_code, error = %e, "task command exited with error")
        }
        None => tracing::info!(pid, "task command completed successfully"),
    }
    if let Some(f) = on_exit {
        f(exit);
    }
}

fn signal_name(sig: i32) -> String {
    match sig {
        libc::SIGTERM => "terminated".into(),
        libc::SIGKILL => "killed".into(),
        libc::SIGINT => "interrupt".into(),
        other => format!("signal {other}"),
    }
}
