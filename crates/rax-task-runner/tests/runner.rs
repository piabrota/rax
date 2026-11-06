//! Ports of AX's runner and metadata server tests, plus `ax ssh`-style
//! execs through the guest client against a running runner.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rax_api::v1alpha1::{
    EnvVar, ObjectMeta, Task, TaskSpec, Workspace, WorkspaceRef, WorkspaceSpec,
};
use rax_guest::{Client, ExecOptions};
use rax_task_runner::metadata::{Server, ServerOptions};
use rax_task_runner::runner::{self, CommandExit, Config};
use rax_task_runner::workspace::{INITIALIZED_MARKER_FILENAME, workspace_marker_path};
use tokio::sync::{mpsc, oneshot};

/// A blocking HTTP/1.0 GET returning the status code and body.
fn get(addr: SocketAddr, path: &str) -> std::io::Result<(u16, String)> {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(s, "GET {path} HTTP/1.0\r\nHost: test\r\n\r\n")?;
    let mut raw = String::new();
    s.read_to_string(&mut raw)?;
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    let code = head
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    Ok((code, body.to_owned()))
}

async fn get_async(addr: SocketAddr, path: &'static str) -> std::io::Result<(u16, String)> {
    tokio::task::spawn_blocking(move || get(addr, path))
        .await
        .unwrap()
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn local(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn task(name: &str, ws_path: &Path) -> Task {
    Task {
        metadata: Some(ObjectMeta {
            name: name.into(),
            atespace: "default".into(),
            ..Default::default()
        }),
        spec: Some(TaskSpec {
            workspace: Some(WorkspaceRef {
                name: "ws".into(),
                path: ws_path.display().to_string(),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Runs the runner in the background with its state in a temp dir, free
/// ports, and the command's exit captured.
struct Harness {
    _dir: tempfile::TempDir,
    ax_dir: PathBuf,
    ws_path: PathBuf,
    port: u16,
    metadata_port: u16,
    task: Task,
    workspace: Option<Workspace>,
    stop: Option<oneshot::Sender<()>>,
    finished: Option<tokio::task::JoinHandle<Result<(), String>>>,
    exited: Option<mpsc::Receiver<CommandExit>>,
}

impl Harness {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let ws_path = dir.path().join("workspace");
        Self {
            ax_dir: dir.path().join("ax"),
            task: task("lib-task", &ws_path),
            ws_path,
            _dir: dir,
            port: free_port(),
            metadata_port: free_port(),
            workspace: None,
            stop: None,
            finished: None,
            exited: None,
        }
    }

    fn spec(&mut self) -> &mut TaskSpec {
        self.task.spec.as_mut().unwrap()
    }

    async fn start(&mut self) {
        let (exit_tx, exit_rx) = mpsc::channel(1);
        let (stop_tx, stop_rx) = oneshot::channel();
        let cfg = Config {
            port: self.port,
            metadata_port: self.metadata_port,
            task: Some(self.task.clone()),
            workspace: self.workspace.clone(),
            ax_dir: Some(self.ax_dir.clone()),
            on_command_exit: Some(Box::new(move |e| {
                let _ = exit_tx.try_send(e);
            })),
        };
        self.finished = Some(tokio::spawn(runner::run(cfg, async {
            let _ = stop_rx.await;
        })));
        self.stop = Some(stop_tx);
        self.exited = Some(exit_rx);
        self.wait_healthy().await;
    }

    async fn wait_healthy(&self) {
        for _ in 0..250 {
            if get_async(local(self.port), "/healthz").await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("runner never became reachable");
    }

    async fn wait_exit(&mut self) -> CommandExit {
        tokio::time::timeout(
            Duration::from_secs(10),
            self.exited.as_mut().unwrap().recv(),
        )
        .await
        .expect("command did not exit in time")
        .unwrap()
    }

    async fn stop(&mut self) {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(15), self.finished.take().unwrap())
            .await
            .expect("run did not return in time")
            .unwrap()
            .unwrap();
    }

    fn is_running(&self) -> bool {
        !self.finished.as_ref().unwrap().is_finished()
    }
}

#[tokio::test]
async fn starts_command_with_injected_env() {
    let mut h = Harness::new();
    let out = h.ws_path.with_file_name("out.txt");
    h.spec().env = vec![EnvVar {
        name: "DEMO_ENV".into(),
        value: "from-task".into(),
    }];
    h.spec().command = vec![
        "sh".into(),
        "-c".into(),
        format!(
            "echo \"$DEMO_ENV $AX_METADATA_URL $(pwd)\" > {}",
            out.display()
        ),
    ];
    h.start().await;
    let exit = h.wait_exit().await;
    assert_eq!((exit.exit_code, exit.error), (0, None));
    let content = std::fs::read_to_string(&out).unwrap();
    let fields: Vec<_> = content.split_whitespace().collect();
    assert_eq!(fields[0], "from-task");
    assert_eq!(fields[1], format!("http://127.0.0.1:{}", h.metadata_port));
    assert_eq!(
        std::fs::canonicalize(fields[2]).unwrap(),
        std::fs::canonicalize(&h.ws_path).unwrap()
    );
    h.stop().await;
}

#[tokio::test]
async fn reports_exit_code_and_keeps_serving() {
    let mut h = Harness::new();
    h.spec().command = vec!["sh".into(), "-c".into(), "exit 3".into()];
    h.start().await;
    let exit = h.wait_exit().await;
    assert_eq!(exit.exit_code, 3);
    assert!(exit.error.is_some());
    // The command is gone but the runner and its server are not.
    assert_eq!(get_async(local(h.port), "/healthz").await.unwrap().0, 200);
    assert!(h.is_running());
    h.stop().await;
}

#[tokio::test]
async fn stops_running_command_group_on_shutdown() {
    let mut h = Harness::new();
    h.spec().command = vec!["sh".into(), "-c".into(), "sleep 30; sleep 30".into()];
    h.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    h.stop().await;
    let exit = h.wait_exit().await;
    assert!(exit.error.is_some(), "{exit:?}");
}

#[tokio::test]
async fn without_command_waits_for_shutdown() {
    let mut h = Harness::new();
    h.start().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(h.is_running());
    h.stop().await;
}

#[tokio::test]
async fn sets_up_bound_workspace() {
    let mut h = Harness::new();
    let code = h.ws_path.with_file_name("code");
    let out = h.ws_path.with_file_name("pwd.txt");
    h.spec().workspace = Some(WorkspaceRef {
        name: "code".into(),
        path: code.display().to_string(),
    });
    h.workspace = Some(Workspace {
        metadata: Some(ObjectMeta {
            name: "code".into(),
            ..Default::default()
        }),
        spec: Some(WorkspaceSpec::default()),
        ..Default::default()
    });
    h.spec().command = vec!["sh".into(), "-c".into(), format!("pwd > {}", out.display())];
    h.start().await;
    assert_eq!(h.wait_exit().await.exit_code, 0);
    assert!(code.is_dir());
    assert!(workspace_marker_path(&code).exists());
    assert!(code.join(".ax").join(INITIALIZED_MARKER_FILENAME).exists());
    assert_eq!(
        std::fs::canonicalize(std::fs::read_to_string(&out).unwrap().trim()).unwrap(),
        std::fs::canonicalize(&code).unwrap()
    );
    assert_eq!(
        get_async(local(h.port), "/readyz").await.unwrap(),
        (200, "ok\n".into())
    );
    let (status, body) = get_async(local(h.metadata_port), "/metadata/v1alpha1/ax/workspace")
        .await
        .unwrap();
    assert_eq!(status, 200);
    assert!(body.contains("name: code"), "{body}");
    h.stop().await;
}

fn sample_task(debug: bool) -> Task {
    let mut t = task("task123", Path::new("/workspace"));
    let spec = t.spec.as_mut().unwrap();
    spec.workspace = None;
    spec.image = "ghcr.io/test/img".into();
    spec.debug = debug;
    t
}

#[tokio::test]
async fn metadata_server() {
    let ws = Workspace {
        metadata: Some(ObjectMeta {
            name: "ws-default".into(),
            atespace: "default".into(),
            ..Default::default()
        }),
        spec: Some(WorkspaceSpec::default()),
        ..Default::default()
    };
    let srv = Server::start(
        0,
        Some(sample_task(true)),
        Some(ws),
        ServerOptions::default(),
    )
    .await
    .unwrap();
    let probe = local(srv.probe_addr().port());
    assert_eq!(
        get_async(probe, "/healthz").await.unwrap(),
        (200, "ok\n".into())
    );
    assert_eq!(get_async(probe, "/readyz").await.unwrap().0, 503);
    srv.set_workspace_ready(true);
    assert_eq!(get_async(probe, "/readyz").await.unwrap().0, 200);

    let meta: SocketAddr = srv
        .metadata_url()
        .trim_start_matches("http://")
        .parse()
        .unwrap();
    assert_eq!(meta.ip().to_string(), "127.0.0.1");
    let (status, body) = get_async(meta, "/metadata/v1alpha1/ax/task").await.unwrap();
    assert_eq!(status, 200);
    let task: Task = rax_api::from_yaml(&body).unwrap();
    assert_eq!(task.metadata.unwrap().name, "task123");
    let (_, body) = get_async(meta, "/metadata/v1alpha1/ax/workspace")
        .await
        .unwrap();
    let ws: Workspace = rax_api::from_yaml(&body).unwrap();
    assert_eq!(ws.metadata.unwrap().name, "ws-default");
    // The metadata endpoints are not on the probe port.
    assert_eq!(
        get_async(probe, "/metadata/v1alpha1/ax/task")
            .await
            .unwrap()
            .0,
        404
    );

    // Guest services share the probe port.
    let client = Client::connect(&probe.to_string(), None, None).unwrap();
    let (out, captured) = capture();
    let code = client
        .exec(ExecOptions {
            command: vec![
                "sh".into(),
                "-c".into(),
                "echo hello-from-guest; echo $AX_METADATA_URL".into(),
            ],
            cwd: String::new(),
            env: Default::default(),
            stdin: None,
            stdout: out,
            stderr: Box::new(std::io::sink()),
            signals: None,
        })
        .await
        .unwrap();
    assert_eq!(code, 0);
    assert_eq!(
        String::from_utf8(captured.lock().unwrap().clone()).unwrap(),
        format!("hello-from-guest\n{}\n", srv.metadata_url())
    );
    srv.stop().await;
}

#[tokio::test]
async fn guest_services_require_debug() {
    let srv = Server::start(0, Some(sample_task(false)), None, ServerOptions::default())
        .await
        .unwrap();
    let meta: SocketAddr = srv
        .metadata_url()
        .trim_start_matches("http://")
        .parse()
        .unwrap();
    assert_eq!(
        get_async(meta, "/metadata/v1alpha1/ax/task")
            .await
            .unwrap()
            .0,
        200
    );
    assert_eq!(
        get_async(meta, "/metadata/v1alpha1/ax/workspace")
            .await
            .unwrap()
            .0,
        404
    );
    let probe = local(srv.probe_addr().port());
    let client = Client::connect(&probe.to_string(), None, None).unwrap();
    let res = client
        .exec(ExecOptions {
            command: vec!["echo".into(), "hi".into()],
            cwd: String::new(),
            env: Default::default(),
            stdin: None,
            stdout: Box::new(std::io::sink()),
            stderr: Box::new(std::io::sink()),
            signals: None,
        })
        .await;
    assert!(res.is_err(), "{res:?}");
    srv.stop().await;
}

type Captured = Arc<Mutex<Vec<u8>>>;

struct Capture(Captured);

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn capture() -> (Box<dyn Write + Send>, Captured) {
    let c = Captured::default();
    (Box::new(Capture(c.clone())), c)
}

/// Ports of AX's guest client tests: stdin, exit codes and signals, end to
/// end through the runner's server.
#[tokio::test]
async fn guest_exec() {
    let srv = Server::start(0, Some(sample_task(true)), None, ServerOptions::default())
        .await
        .unwrap();
    let client = Client::connect(&local(srv.probe_addr().port()).to_string(), None, None).unwrap();
    let opts = |command: &[&str], stdin: Option<&'static [u8]>| {
        let (out, captured) = capture();
        (
            ExecOptions {
                command: command.iter().map(|s| s.to_string()).collect(),
                cwd: String::new(),
                env: Default::default(),
                stdin: stdin.map(|s| Box::new(s) as Box<dyn tokio::io::AsyncRead + Send + Unpin>),
                stdout: out,
                stderr: Box::new(std::io::sink()),
                signals: None,
            },
            captured,
        )
    };

    // Stdin is forwarded and closed at its end.
    let (o, out) = opts(&["cat"], Some(b"piped input\n"));
    assert_eq!(client.exec(o).await.unwrap(), 0);
    assert_eq!(&*out.lock().unwrap(), b"piped input\n");

    // Without stdin the command reads an empty one.
    let (o, out) = opts(&["sh", "-c", "cat; echo done"], None);
    assert_eq!(client.exec(o).await.unwrap(), 0);
    assert_eq!(&*out.lock().unwrap(), b"done\n");

    // Exit codes come back.
    let (o, _) = opts(&["sh", "-c", "exit 17"], None);
    assert_eq!(client.exec(o).await.unwrap(), 17);

    // Signals reach the process group.
    let (mut o, out) = opts(
        &[
            "sh",
            "-c",
            "trap 'echo got-int; exit 5' INT; echo ready; while :; do sleep 0.05; done",
        ],
        None,
    );
    let (sig_tx, sig_rx) = mpsc::channel(1);
    o.signals = Some(sig_rx);
    let exec = tokio::spawn({
        let client = client.clone();
        async move { client.exec(o).await }
    });
    for _ in 0..200 {
        if out.lock().unwrap().starts_with(b"ready") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    sig_tx.send(rax_guest::proto::Signal::Int).await.unwrap();
    assert_eq!(exec.await.unwrap().unwrap(), 5);
    assert!(String::from_utf8_lossy(&out.lock().unwrap()).contains("got-int"));
    srv.stop().await;
}
