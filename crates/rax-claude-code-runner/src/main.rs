//! `rax-claude-code-runner`, a port of AX's `claude-code-runner`: the task
//! runner of the Claude Code image (`Dockerfile.claude-code`), installed
//! there as `/usr/local/bin/ax-task-runner`.
//!
//! A Task that leaves `spec.command` empty runs one headless `claude --print`
//! session built from `spec.config` (see [`rax_claude_code_runner`]); a Task
//! that sets it runs as under `rax-task-runner`. Flags and environment are
//! the same as `rax-task-runner`'s.

use std::path::PathBuf;

use clap::Parser;
use rax_api::v1alpha1::{Task, Workspace};
use rax_claude_code_runner::with_claude_command;
use rax_task_runner::load_spec;
use rax_task_runner::runner::{self, DEFAULT_METADATA_PORT, DEFAULT_PORT};
use tokio::signal::unix::{SignalKind, signal};

#[derive(Parser, Debug)]
#[command(name = "rax-claude-code-runner", version, about)]
struct Args {
    /// Port for the probe and guest server.
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
    /// Loopback-only port for the metadata endpoints.
    #[arg(long, default_value_t = DEFAULT_METADATA_PORT)]
    metadata_port: u16,
    /// Read the Task YAML from this file instead of AX_TASK_YAML.
    #[arg(long)]
    task_file: Option<PathBuf>,
    /// Read the Workspace YAML from this file instead of AX_WORKSPACE_YAML.
    #[arg(long)]
    workspace_file: Option<PathBuf>,
}

/// Go's flag package takes `-name` as well as `--name`; so does this, for
/// templates written for AX's runner.
fn go_style_args() -> Vec<String> {
    std::env::args()
        .enumerate()
        .map(|(i, a)| {
            if i > 0 && a.starts_with('-') && !a.starts_with("--") && a.len() > 2 {
                format!("-{a}")
            } else {
                a
            }
        })
        .collect()
}

fn fatal(err: impl std::fmt::Display) -> ! {
    tracing::error!(error = %err, "claude code runner failed");
    std::process::exit(1);
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_ansi(false)
        .init();
    let args = Args::parse_from(go_style_args());

    let workspace = load_spec::<Workspace>(args.workspace_file.as_deref(), "AX_WORKSPACE_YAML")
        .unwrap_or_else(|e| fatal(e))
        .filter(|w| w.metadata.is_some());
    let task = load_spec::<Task>(args.task_file.as_deref(), "AX_TASK_YAML")
        .unwrap_or_else(|e| fatal(e))
        .filter(|t| t.metadata.is_some())
        .map(|t| with_claude_command(t, workspace.as_ref()).unwrap_or_else(|e| fatal(e)));

    let shutdown = async {
        let mut term = signal(SignalKind::terminate()).expect("installing SIGTERM handler");
        let mut int = signal(SignalKind::interrupt()).expect("installing SIGINT handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
    };
    let cfg = runner::Config {
        port: args.port,
        metadata_port: args.metadata_port,
        task,
        workspace,
        ..Default::default()
    };
    if let Err(e) = runner::run(cfg, shutdown).await {
        fatal(e);
    }
}
