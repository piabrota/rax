//! AX's end-to-end suite (`test/e2e` in AX), replayed without Kubernetes.
//!
//! The real suite needs a kind cluster with Agent Substrate and gVisor. Here
//! the same scenarios run against a real rax-server (gRPC API, controller,
//! in-memory store) talking to an in-memory Substrate whose actors are real
//! task-runners on local ports: resuming an actor starts a runner with the
//! actor template's `AX_TASK_YAML` and `AX_WORKSPACE_YAML`, suspending or
//! deleting it stops the runner. Every step goes through the CLI binary, as
//! the real suite does, and `ssh` reaches the runner's guest services
//! directly at its worker IP.
//!
//! Differences from a cluster: workspaces live in a temp dir, so tasks that
//! bind none get one there instead of `/workspace`; suspend keeps the files
//! on disk but not processes, as VOLUMES snapshots do, though the rootfs
//! (`/tmp`) is the host's and is not reset; there is no atenet router.
//!
//! Two environment variables point the suite at other binaries, to check
//! that RAX and AX interoperate:
//!   RAX_E2E_CLI     the CLI to drive, such as AX's Go `ax` (default: `rax`)
//!   RAX_E2E_RUNNER  a task-runner binary to start for each actor, such as
//!                   AX's Go `ax-task-runner` (default: RAX's runner, in
//!                   process). AX's runner keeps state in `/ax`, so it needs
//!                   root.
//!   RAX_E2E_SERVER  a server binary to run instead of the in-process
//!                   rax-server, such as AX's Go `ax-server`, with its store
//!                   URL in RAX_E2E_STORE (default `memory://`, which only
//!                   rax-server has). Use an empty database: the scenarios
//!                   reuse resource names.

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rax_api::v1alpha1::{Task, Workspace, WorkspaceRef};
use rax_api::{from_yaml, from_yaml_lenient, to_yaml};
use rax_server::controller::{ControllerOptions, SubstrateActors, TaskReconciler};
use rax_server::{AxService, ServerOptions, router};
use rax_store::{MemoryLocker, MemoryStore};
use rax_substrate::ateapi::{Actor, ActorState, ActorTemplate};
use rax_substrate::testing::{FakeSubstrate, Lifecycle};
use rax_substrate::{Client, RetryPolicy, TokenInterceptor};
use tokio::sync::oneshot;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

enum Running {
    InProcess(Option<oneshot::Sender<()>>),
    External(std::process::Child),
}

impl Drop for Running {
    fn drop(&mut self) {
        match self {
            Running::InProcess(stop) => {
                if let Some(stop) = stop.take() {
                    let _ = stop.send(());
                }
            }
            Running::External(child) => {
                // SAFETY: kill has no memory-safety preconditions.
                unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
                let _ = child.wait();
            }
        }
    }
}

/// Starts a task-runner per resumed actor.
struct LocalRunners {
    root: PathBuf,
    external: Option<PathBuf>,
    rt: tokio::runtime::Handle,
    running: Mutex<HashMap<String, (String, Running)>>,
}

impl std::fmt::Debug for LocalRunners {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LocalRunners")
    }
}

impl Lifecycle for LocalRunners {
    fn resume(&self, actor: &Actor, template: Option<&ActorTemplate>) -> String {
        let meta = actor.metadata.clone().unwrap_or_default();
        let key = format!("{}/{}", meta.atespace, meta.name);
        if let Some((addr, _)) = self.running.lock().unwrap().get(&key) {
            return addr.clone();
        }
        let env: HashMap<String, String> = template
            .and_then(|t| t.containers.first())
            .map(|c| {
                c.env
                    .iter()
                    .map(|e| (e.name.clone(), e.value.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let mut task: Task = env
            .get("AX_TASK_YAML")
            .map(|y| from_yaml_lenient(y).expect("AX_TASK_YAML"))
            .unwrap_or_default();
        let workspace: Option<Workspace> = env
            .get("AX_WORKSPACE_YAML")
            .map(|y| from_yaml_lenient(y).expect("AX_WORKSPACE_YAML"));
        let actor_dir = self.root.join(format!("{}-{}", meta.atespace, meta.name));
        // Tasks without a workspace binding would use /workspace.
        let spec = task.spec.get_or_insert_default();
        if spec.workspace.as_ref().is_none_or(|w| w.path.is_empty()) {
            let name = spec
                .workspace
                .as_ref()
                .map(|w| w.name.clone())
                .unwrap_or_default();
            spec.workspace = Some(WorkspaceRef {
                name,
                path: actor_dir.join("workspace").display().to_string(),
            });
        }
        let ax_dir = actor_dir.join("ax");
        let (port, metadata_port) = (free_port(), free_port());

        let running = match &self.external {
            Some(bin) => {
                let mut cmd = Command::new(bin);
                cmd.args(["--port", &port.to_string()])
                    .args(["--metadata-port", &metadata_port.to_string()])
                    .env("AX_TASK_YAML", to_yaml(&task).unwrap())
                    .env("LOG_DIR", actor_dir.join("logs"));
                if let Some(ws) = &workspace {
                    cmd.env("AX_WORKSPACE_YAML", to_yaml(ws).unwrap());
                }
                Running::External(cmd.spawn().expect("starting the external runner"))
            }
            None => {
                let (stop, stopped) = oneshot::channel();
                let cfg = rax_task_runner::runner::Config {
                    port,
                    metadata_port,
                    task: Some(task),
                    workspace,
                    ax_dir: Some(ax_dir),
                    on_command_exit: None,
                };
                self.rt.spawn(rax_task_runner::runner::run(cfg, async {
                    let _ = stopped.await;
                }));
                Running::InProcess(Some(stop))
            }
        };
        let addr = format!("127.0.0.1:{port}");
        // Substrate's wakeup probe: resume returns once the runner serves.
        // An in-process runner can't start until this call returns (it sits
        // in this worker's queue), and the controller's own readiness poll
        // waits for it.
        if matches!(running, Running::External(_)) {
            let deadline = Instant::now() + Duration::from_secs(10);
            while std::net::TcpStream::connect(&addr).is_err() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        self.running
            .lock()
            .unwrap()
            .insert(key, (addr.clone(), running));
        addr
    }

    fn stop(&self, atespace: &str, name: &str) {
        let removed = self
            .running
            .lock()
            .unwrap()
            .remove(&format!("{atespace}/{name}"));
        drop(removed);
    }
}

/// A server process, stopped when dropped.
struct ServerProcess(std::process::Child);

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Stack {
    server: String,
    _external: Option<ServerProcess>,
    substrate: FakeSubstrate,
    runners: Arc<LocalRunners>,
    cli: PathBuf,
    root: tempfile::TempDir,
    home: tempfile::TempDir,
}

async fn stack() -> Stack {
    let root = tempfile::tempdir().unwrap();
    let runners = Arc::new(LocalRunners {
        root: root.path().to_owned(),
        external: std::env::var_os("RAX_E2E_RUNNER").map(PathBuf::from),
        rt: tokio::runtime::Handle::current(),
        running: Mutex::default(),
    });
    let substrate = FakeSubstrate::new();
    substrate.state().lifecycle = Some(runners.clone());
    let substrate_addr = substrate.serve().await;
    let mut external = None;
    let server = match std::env::var_os("RAX_E2E_SERVER") {
        Some(bin) => {
            let port = free_port();
            let child = Command::new(bin)
                .arg(format!("--addr=127.0.0.1:{port}"))
                .arg(format!(
                    "--store={}",
                    std::env::var("RAX_E2E_STORE").unwrap_or_else(|_| "memory://".into())
                ))
                .arg(format!("--substrate-endpoint={substrate_addr}"))
                .arg("--substrate-plaintext")
                .spawn()
                .expect("starting the external server");
            external = Some(ServerProcess(child));
            let addr = format!("127.0.0.1:{port}");
            let deadline = Instant::now() + Duration::from_secs(30);
            while std::net::TcpStream::connect(&addr).is_err() {
                assert!(
                    Instant::now() < deadline,
                    "the external server did not start"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            format!("http://{addr}")
        }
        None => {
            let channel =
                tonic::transport::Endpoint::from_shared(format!("http://{substrate_addr}"))
                    .unwrap()
                    .connect_lazy();
            let client = Client::from_channel(channel, TokenInterceptor::default())
                .with_retry_policy(RetryPolicy::fast());
            let reconciler = TaskReconciler::new(
                client.clone(),
                ControllerOptions {
                    default_template: "default-template".into(),
                    default_template_atespace: "ax-system".into(),
                    default_task_image: rax_api::DEFAULT_TASK_IMAGE.into(),
                    ..Default::default()
                },
            );
            let service = AxService::new(
                Arc::new(MemoryStore::new()),
                ServerOptions {
                    locker: Some(Arc::new(MemoryLocker::new())),
                    reconciler: Some(Arc::new(reconciler)),
                    actors: Some(Arc::new(SubstrateActors::new(client))),
                },
            );
            let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let server = format!("http://{}", lis.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(lis, router(service)).await.unwrap() });
            server
        }
    };
    Stack {
        server,
        _external: external,
        substrate,
        runners,
        cli: std::env::var_os("RAX_E2E_CLI")
            .map_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_rax")), PathBuf::from),
        root,
        home: tempfile::tempdir().unwrap(),
    }
}

impl Stack {
    /// Runs the CLI with `--server` and, when set, `-a <atespace>`, off the
    /// async runtime. Returns success and combined output.
    async fn run(&self, atespace: &str, args: &[&str]) -> (bool, i32, String) {
        let mut cmd = Command::new(&self.cli);
        cmd.args(["--server", &self.server]);
        if !atespace.is_empty() {
            cmd.args(["-a", atespace]);
        }
        cmd.args(args)
            .env("AX_HOME", self.home.path())
            .env_remove("AX_SERVER")
            .stdin(std::process::Stdio::null());
        let out = tokio::task::spawn_blocking(move || cmd.output().unwrap())
            .await
            .unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        (out.status.success(), out.status.code().unwrap_or(-1), text)
    }

    async fn must(&self, atespace: &str, args: &[&str]) -> String {
        let (ok, _, out) = self.run(atespace, args).await;
        assert!(ok, "{} {args:?} failed:\n{out}", self.cli.display());
        out
    }

    async fn apply(&self, atespace: &str, manifest: &str) -> String {
        let path = self
            .root
            .path()
            .join(format!("manifest-{}.yaml", free_port()));
        std::fs::write(&path, manifest).unwrap();
        self.must(atespace, &["apply", "-f", path.to_str().unwrap()])
            .await
    }

    async fn ssh(&self, atespace: &str, task: &str, script: &str) -> String {
        self.must(atespace, &["ssh", task, "--", "sh", "-c", script])
            .await
    }

    async fn task(&self, atespace: &str, name: &str) -> Task {
        let out = self.must(atespace, &["get", "task", name]).await;
        from_yaml(&out).unwrap_or_else(|e| panic!("{e}\n{out}"))
    }

    fn worker(&self, atespace: &str, task: &str) -> String {
        self.runners
            .running
            .lock()
            .unwrap()
            .get(&format!("{atespace}/{task}"))
            .map(|(addr, _)| addr.clone())
            .expect("no runner for the task")
    }
}

fn assert_contains(what: &str, out: &str, wants: &[&str]) {
    for want in wants {
        assert!(out.contains(want), "{what}: missing {want:?} in:\n{out}");
    }
}

fn condition(t: &Task, kind: &str) -> (String, String) {
    t.status
        .as_ref()
        .and_then(|s| s.conditions.iter().find(|c| c.r#type == kind))
        .map(|c| (c.status.clone(), c.reason.clone()))
        .unwrap_or_default()
}

fn phase(t: &Task) -> &str {
    t.status.as_ref().map_or("", |s| s.phase.as_str())
}

fn http_get(addr: &str, path: &str) -> (u16, String) {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect(addr).unwrap();
    write!(s, "GET {path} HTTP/1.0\r\n\r\n").unwrap();
    let mut raw = String::new();
    s.read_to_string(&mut raw).unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    (
        head.split_whitespace().nth(1).unwrap().parse().unwrap(),
        body.to_owned(),
    )
}

/// A bare repository with a v1.0.0 tag and a newer main, like the one AX's
/// E2E task-runner image carries.
fn sample_repo(dir: &Path) -> String {
    let src = dir.join("sample-src");
    let bare = dir.join("sample-repo.git");
    std::fs::create_dir_all(&src).unwrap();
    let script = format!(
        "set -e; git init -q; git symbolic-ref HEAD refs/heads/main; \
         git config user.name 'AX E2E'; git config user.email e2e@ax.local; \
         echo '# Sample E2E Repo' > README.md; echo \"print('hello from git repo v1')\" > app.py; \
         git add README.md app.py; git commit -qm 'Initial commit'; git tag v1.0.0; \
         echo \"print('hello from git repo main')\" > app.py; git commit -qam 'Second commit on main'; \
         git clone -q --bare . {}",
        bare.display()
    );
    let out = Command::new("sh")
        .args(["-c", &script])
        .current_dir(&src)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    bare.display().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_version() {
    let s = stack().await;
    assert_contains("version", &s.must("", &["version"]).await, &["ax version"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workspace_gateway_and_model_crud() {
    let s = stack().await;
    let repo = sample_repo(s.root.path());
    let ws = |env: &str| {
        format!(
            "apiVersion: ax.io/v1alpha1\nkind: Workspace\nmetadata:\n  name: ws-crud\n  atespace: default\nspec:\n  git:\n  - name: origin\n    repo: {repo}\n    branch: v1.0.0\n  files:\n  - path: config/settings.json\n    content: '{{\"env\":\"{env}\"}}'\n  mcp:\n    servers:\n    - name: local-mcp\n      endpoint: http://localhost:9000\n  skills:\n    path: /workspace/.agents/skills\n"
        )
    };
    let r = "workspace.ax.io/ws-crud";
    assert_contains(
        "first apply",
        &s.apply("default", &ws("e2e-v1")).await,
        &[&format!("{r} created")],
    );
    assert_contains(
        "re-apply",
        &s.apply("default", &ws("e2e-v1")).await,
        &[&format!("{r} unchanged")],
    );
    assert_contains(
        "modified",
        &s.apply("default", &ws("e2e-v2")).await,
        &[&format!("{r} configured")],
    );
    assert_contains(
        "get workspaces",
        &s.must("default", &["get", "workspaces"]).await,
        &["ws-crud"],
    );
    assert_contains(
        "get workspace",
        &s.must("default", &["get", "workspace", "ws-crud"]).await,
        &["e2e-v2", "local-mcp", "config/settings.json"],
    );
    assert_contains(
        "describe workspace",
        &s.must("default", &["describe", "workspace", "ws-crud"])
            .await,
        &["local-mcp", ".agents/skills"],
    );
    assert_contains(
        "delete",
        &s.must("default", &["delete", "workspace", "ws-crud"]).await,
        &["deleted"],
    );
    let (ok, _, out) = s.run("default", &["get", "workspace", "ws-crud"]).await;
    assert!(!ok && out.contains("NotFound"), "{out}");

    // The same lifecycle for a Gateway.
    let gateway = |host: &str| {
        format!(
            "apiVersion: ax.io/v1alpha1\nkind: Gateway\nmetadata:\n  name: gw-crud\n  atespace: default\nspec:\n  egress:\n  - https:\n      hostnames:\n      - {host}\n"
        )
    };
    let r = "gateway.ax.io/gw-crud";
    assert_contains(
        "first apply",
        &s.apply("default", &gateway("a.example.com")).await,
        &[&format!("{r} created")],
    );
    assert_contains(
        "re-apply",
        &s.apply("default", &gateway("a.example.com")).await,
        &[&format!("{r} unchanged")],
    );
    assert_contains(
        "modified",
        &s.apply("default", &gateway("b.example.com")).await,
        &[&format!("{r} configured")],
    );
    assert_contains(
        "get gateways",
        &s.must("default", &["get", "gateways"]).await,
        &["gw-crud"],
    );
    assert_contains(
        "get gateway",
        &s.must("default", &["get", "gateway", "gw-crud"]).await,
        &["b.example.com"],
    );
    assert_contains(
        "describe gateway",
        &s.must("default", &["describe", "gateway", "gw-crud"]).await,
        &["https: b.example.com"],
    );
    s.must("default", &["delete", "gateway", "gw-crud"]).await;
    let (ok, _, out) = s.run("default", &["get", "gateway", "gw-crud"]).await;
    assert!(!ok && out.contains("NotFound"), "{out}");

    let model = |m: &str| {
        format!(
            "apiVersion: ax.io/v1alpha1\nkind: Model\nmetadata:\n  name: model-crud\n  atespace: default\nspec:\n  provider: google\n  model: {m}\n  secretKey:\n    name: gemini-secret\n    key: api-key\n"
        )
    };
    let r = "model.ax.io/model-crud";
    assert_contains(
        "first apply",
        &s.apply("default", &model("gemini-2.5-pro")).await,
        &[&format!("{r} created")],
    );
    assert_contains(
        "re-apply",
        &s.apply("default", &model("gemini-2.5-pro")).await,
        &[&format!("{r} unchanged")],
    );
    assert_contains(
        "modified",
        &s.apply("default", &model("gemini-2.5-flash")).await,
        &[&format!("{r} configured")],
    );
    assert_contains(
        "get models",
        &s.must("default", &["get", "models"]).await,
        &["model-crud", "gemini-2.5-flash"],
    );
    assert_contains(
        "get model",
        &s.must("default", &["get", "model", "model-crud"]).await,
        &["gemini-2.5-flash"],
    );
    assert_contains(
        "describe model",
        &s.must("default", &["describe", "model", "model-crud"])
            .await,
        &["gemini-secret"],
    );
    s.must("default", &["delete", "model", "model-crud"]).await;
    let (ok, _, out) = s.run("default", &["get", "model", "model-crud"]).await;
    assert!(!ok && out.contains("NotFound"), "{out}");
}

fn task_manifest(atespace: &str, name: &str, debug: bool) -> String {
    format!(
        "apiVersion: ax.io/v1alpha1\nkind: Task\nmetadata:\n  name: {name}\n  atespace: {atespace}\nspec:\n  debug: {debug}\n"
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_lifecycle_and_substrate_sync() {
    let s = stack().await;
    let name = "task-life";
    let manifest = task_manifest("default", name, true);

    // A new task is Suspended, backed by an actor of the same name.
    assert_contains(
        "apply",
        &s.apply("default", &manifest).await,
        &[&format!("task.ax.io/{name} created")],
    );
    let t = s.task("default", name).await;
    assert_eq!(phase(&t), "Suspended");
    assert_eq!(t.status.as_ref().unwrap().actor, name);

    // Tasks are immutable.
    let path = s.root.path().join("dup.yaml");
    std::fs::write(&path, &manifest).unwrap();
    let (ok, _, out) = s
        .run("default", &["apply", "-f", path.to_str().unwrap()])
        .await;
    assert!(!ok);
    assert_contains(
        "duplicate apply",
        &out,
        &["already exists and is immutable"],
    );

    // Substrate has a suspended actor using a per-task template with the
    // /readyz:80 wakeup probe and a snapshot config.
    let tmpl_name = {
        let st = s.substrate.state();
        let a = &st.actors[&format!("default/{name}")];
        assert_eq!(
            a.status.as_ref().unwrap().state,
            ActorState::Suspended as i32
        );
        let tmpl_name = a.actor_template.as_ref().unwrap().name.clone();
        assert!(
            tmpl_name.starts_with(&format!("{name}-tmpl-")),
            "{tmpl_name}"
        );
        let tmpl = &st.templates[&format!("default/{tmpl_name}")];
        assert_eq!(tmpl.containers.len(), 1);
        let probe = tmpl.containers[0]
            .wakeup_probe
            .as_ref()
            .unwrap()
            .http_get
            .as_ref()
            .unwrap();
        assert_eq!((probe.path.as_str(), probe.port), ("/readyz", 80));
        // A SnapshotConfig at volume fidelity, with no template-level
        // location so Substrate 1.0 uses its cluster-wide one.
        let snap = tmpl.snapshot_config.as_ref().unwrap();
        assert_eq!(
            snap.preferred_fidelity,
            rax_substrate::ateapi::SnapshotFidelity::Volumes as i32
        );
        assert!(snap.storage_location.is_empty());
        tmpl_name
    };

    // Resuming makes it Running and Ready on the worker Substrate reports.
    assert_contains(
        "resume",
        &s.must("default", &["resume", "task", name]).await,
        &["resumed"],
    );
    let t = s.task("default", name).await;
    assert_eq!(phase(&t), "Running");
    assert_eq!(
        condition(&t, "WorkspaceReady"),
        ("True".into(), "SetupComplete".into())
    );
    assert_eq!(
        condition(&t, "Ready"),
        ("True".into(), "TaskRunning".into())
    );
    assert_eq!(
        t.status.as_ref().unwrap().worker_ip,
        s.worker("default", name)
    );

    // Deleting removes the task, the actor and its template.
    assert_contains(
        "delete",
        &s.must("default", &["delete", "task", name]).await,
        &["deleted"],
    );
    let (ok, _, out) = s.run("default", &["get", "task", name]).await;
    assert!(!ok && out.contains("NotFound"), "{out}");
    let st = s.substrate.state();
    assert!(!st.actors.contains_key(&format!("default/{name}")));
    assert!(!st.templates.contains_key(&format!("default/{tmpl_name}")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_server_and_guest_exec() {
    let s = stack().await;
    let ws_path = s.root.path().join("meta-ws");
    let manifest = format!(
        "apiVersion: ax.io/v1alpha1\nkind: Workspace\nmetadata:\n  name: ws-meta\n  atespace: default\nspec:\n  files:\n  - path: hello.txt\n    content: \"hello from workspace\"\n---\napiVersion: ax.io/v1alpha1\nkind: Task\nmetadata:\n  name: task-meta\n  atespace: default\nspec:\n  debug: true\n  workspace:\n    name: ws-meta\n    path: {ws}\n  env:\n  - name: CUSTOM_E2E_VAR\n    value: \"custom-val-123\"\n  command:\n  - sh\n  - -c\n  - \"echo cmd-ran-ok > {ws}/cmd-ran.txt\"\n",
        ws = ws_path.display()
    );
    s.apply("default", &manifest).await;
    s.must("default", &["resume", "task", "task-meta"]).await;

    // ssh sees spec.env, the metadata URL and workspace files.
    let out = s
        .ssh(
            "default",
            "task-meta",
            &format!(
                "echo ENV=$CUSTOM_E2E_VAR META=$AX_METADATA_URL FILE=$(cat {}/hello.txt)",
                ws_path.display()
            ),
        )
        .await;
    assert_contains(
        "ssh output",
        &out,
        &[
            "ENV=custom-val-123",
            "META=http://127.0.0.1:",
            "FILE=hello from workspace",
        ],
    );
    let meta_url = out
        .split_whitespace()
        .find_map(|w| w.strip_prefix("META="))
        .unwrap()
        .to_owned();

    // spec.command runs after setup.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (ok, _, out) = s
            .run(
                "default",
                &[
                    "ssh",
                    "task-meta",
                    "--",
                    "cat",
                    &format!("{}/cmd-ran.txt", ws_path.display()),
                ],
            )
            .await;
        if ok && out.trim() == "cmd-ran-ok" {
            break;
        }
        assert!(Instant::now() < deadline, "spec.command did not run: {out}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // The probe server answers health and readiness, and the loopback
    // metadata server serves the task and workspace.
    let worker = s.worker("default", "task-meta");
    for path in ["/healthz", "/readyz"] {
        let (code, body) = http_get(&worker, path);
        assert_eq!((code, body.trim()), (200, "ok"), "{path}");
    }
    let meta = meta_url.trim_start_matches("http://");
    assert_contains(
        "task metadata",
        &http_get(meta, "/metadata/v1alpha1/ax/task").1,
        &["task-meta", "custom-val-123"],
    );
    assert_contains(
        "workspace metadata",
        &http_get(meta, "/metadata/v1alpha1/ax/workspace").1,
        &["ws-meta", "hello from workspace"],
    );

    // ssh propagates a non-zero exit status.
    let (ok, code, _) = s
        .run(
            "default",
            &["ssh", "task-meta", "--", "sh", "-c", "exit 17"],
        )
        .await;
    assert!(!ok);
    assert_eq!(code, 17);

    // ssh is rejected without spec.debug.
    s.apply("default", &task_manifest("default", "task-nodebug", false))
        .await;
    s.must("default", &["resume", "task", "task-nodebug"]).await;
    let (ok, _, out) = s
        .run(
            "default",
            &["ssh", "task-nodebug", "--", "echo", "should-fail"],
        )
        .await;
    assert!(!ok);
    assert_contains("debug-disabled error", &out, &["spec.debug: true"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workspace_setup_files_and_git() {
    let s = stack().await;
    let repo = sample_repo(s.root.path());
    let primary = s.root.path().join("workspace/primary");
    let manifest = format!(
        "apiVersion: ax.io/v1alpha1\nkind: Workspace\nmetadata:\n  name: ws-git\n  atespace: default\nspec:\n  git:\n  - name: sample\n    dir: \".\"\n    repo: {repo}\n    branch: v1.0.0\n  files:\n  - path: overlay/config.yaml\n    content: \"mode: git-plus-overlay\"\n  skills:\n    path: {p}/.agents/skills\n---\napiVersion: ax.io/v1alpha1\nkind: Task\nmetadata:\n  name: task-ws\n  atespace: default\nspec:\n  debug: true\n  workspace:\n    name: ws-git\n    path: {p}\n",
        p = primary.display()
    );
    s.apply("default", &manifest).await;
    s.must("default", &["resume", "task", "task-ws"]).await;
    let out = s
        .ssh(
            "default",
            "task-ws",
            &format!(
                "set -e\npwd\ncat {p}/app.py\ncat {p}/overlay/config.yaml\ntest -d {p}/.agents/skills && echo SKILLS_DIR_OK\ntest -f {p}/.git/.ax/initialized && echo MARKERS_OK\n",
                p = primary.display()
            ),
        )
        .await;
    assert_contains(
        "workspace setup",
        &out,
        &[
            &primary.display().to_string(),
            "hello from git repo v1",
            "mode: git-plus-overlay",
            "SKILLS_DIR_OK",
            "MARKERS_OK",
        ],
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_suspend_and_resume_persistence() {
    let s = stack().await;
    let ws = s.root.path().join("persist-ws");
    let manifest = format!(
        "apiVersion: ax.io/v1alpha1\nkind: Workspace\nmetadata:\n  name: ws-persist\n  atespace: default\nspec:\n  files:\n  - path: seed.txt\n    content: \"initial-seed\"\n---\napiVersion: ax.io/v1alpha1\nkind: Task\nmetadata:\n  name: task-persist\n  atespace: default\nspec:\n  debug: true\n  workspace:\n    name: ws-persist\n    path: {}\n",
        ws.display()
    );
    s.apply("default", &manifest).await;
    s.must("default", &["resume", "task", "task-persist"]).await;
    s.ssh(
        "default",
        "task-persist",
        &format!("echo durable-data-42 > {}/durable.txt", ws.display()),
    )
    .await;

    assert_contains(
        "suspend",
        &s.must("default", &["suspend", "task", "task-persist"])
            .await,
        &["suspended"],
    );
    let t = s.task("default", "task-persist").await;
    assert_eq!(phase(&t), "Suspended");
    assert_eq!(condition(&t, "Ready"), ("False".into(), "Suspended".into()));
    assert_eq!(t.status.as_ref().unwrap().worker_ip, "");
    assert_eq!(
        s.substrate.state().actors["default/task-persist"]
            .status
            .as_ref()
            .unwrap()
            .state,
        ActorState::Suspended as i32
    );
    // The runner is gone while suspended.
    assert!(
        s.runners
            .running
            .lock()
            .unwrap()
            .get("default/task-persist")
            .is_none()
    );

    s.must("default", &["resume", "task", "task-persist"]).await;
    let out = s
        .ssh(
            "default",
            "task-persist",
            &format!("echo DURABLE=$(cat {}/durable.txt)", ws.display()),
        )
        .await;
    assert_contains("after resume", &out, &["DURABLE=durable-data-42"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn describe_and_atespace_isolation() {
    let s = stack().await;
    let ns = "team-e2e";
    s.apply(ns, &task_manifest(ns, "task-describe", true)).await;

    let (ok, _, out) = s.run("default", &["get", "task", "task-describe"]).await;
    assert!(!ok && out.contains("NotFound"), "{out}");
    assert!(
        !s.must("default", &["get", "tasks"])
            .await
            .contains("task-describe")
    );
    assert_contains(
        "get tasks in ns",
        &s.must(ns, &["get", "tasks"]).await,
        &["task-describe"],
    );

    s.must(ns, &["resume", "task", "task-describe"]).await;
    assert_contains(
        "describe task",
        &s.must(ns, &["describe", "task", "task-describe"]).await,
        &[
            "task-describe",
            ns,
            "Running",
            "WorkspaceReady",
            "TaskRunning",
        ],
    );
}
