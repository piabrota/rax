//! Ports of AX's CLI tests: the `rax` binary against a rax-server with an
//! in-memory store.

use std::process::Command;
use std::sync::Arc;

use rax_api::v1alpha1::{ObjectMeta, Task, Workspace};
use rax_server::{AxService, ServerOptions, router};
use rax_store::{MemoryStore, ResourceStore, StoreError, TaskStore};

struct Env {
    store: Arc<MemoryStore>,
    addr: String,
    home: tempfile::TempDir,
}

async fn serve(store: Arc<MemoryStore>) -> Env {
    let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let app = router(AxService::new(store.clone(), ServerOptions::default()));
    tokio::spawn(async move { axum::serve(lis, app).await.unwrap() });
    Env {
        store,
        addr,
        home: tempfile::tempdir().unwrap(),
    }
}

/// Exit status, stdout and stderr of one `rax` run.
fn rax(env: &Env, args: &[&str]) -> (i32, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_rax"))
        .args(["--server", &env.addr])
        .args(args)
        .env("AX_HOME", env.home.path())
        .env_remove("AX_SERVER")
        .output()
        .unwrap();
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn write(dir: &std::path::Path, content: &str) -> String {
    let path = dir.join(format!("m-{}.yaml", rand_suffix()));
    std::fs::write(&path, content).unwrap();
    path.display().to_string()
}

fn rand_suffix() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::SeqCst)
}

/// Manifest atespace, flags, the atespace the resource lands in, and the
/// error expected instead.
type ApplyCase = (
    &'static str,
    &'static [&'static str],
    Option<&'static str>,
    Option<&'static str>,
);

#[tokio::test(flavor = "multi_thread")]
async fn apply_atespace() {
    for kind in ["Task", "Workspace", "Gateway", "Model"] {
        let cases: [ApplyCase; 6] = [
            ("", &[], Some("default"), None),
            ("", &["-a", "team-a"], Some("team-a"), None),
            ("team-a", &[], Some("team-a"), None),
            ("team-a", &["--atespace", "team-a"], Some("team-a"), None),
            (
                "team-a",
                &["--atespace=team-b"],
                None,
                Some(r#"metadata.atespace "team-a" does not match --atespace "team-b""#),
            ),
            (
                "team-a",
                &["-a", "default"],
                None,
                Some(r#"metadata.atespace "team-a" does not match --atespace "default""#),
            ),
        ];
        for (manifest_ns, flags, want_ns, want_err) in cases {
            let env = serve(Arc::new(MemoryStore::new())).await;
            let mut manifest =
                format!("apiVersion: ax.io/v1alpha1\nkind: {kind}\nmetadata:\n  name: example\n");
            if !manifest_ns.is_empty() {
                manifest.push_str(&format!("  atespace: {manifest_ns:?}\n"));
            }
            manifest.push_str("spec: {}\n");
            let path = write(env.home.path(), &manifest);
            let mut args = vec!["apply", "-f", &path];
            args.extend_from_slice(flags);
            let ctx = format!("{kind} {flags:?}");

            let (code, out, err) = rax(&env, &args);
            if let Some(want) = want_err {
                assert_eq!(code, 1, "{ctx}");
                assert!(err.contains(want), "{ctx}: {err}");
            } else {
                assert_eq!(
                    (code, out.as_str()),
                    (
                        0,
                        format!("{}.ax.io/example created\n", kind.to_lowercase()).as_str()
                    ),
                    "{ctx}: {err}"
                );
                let (code, out, err) = rax(&env, &args);
                if kind == "Task" {
                    let want = format!(
                        "code = FailedPrecondition desc = task {}/example already exists and is immutable",
                        want_ns.unwrap()
                    );
                    assert_eq!(code, 1, "{ctx}");
                    assert!(err.contains(&want), "{ctx}: {err}");
                } else {
                    assert_eq!(
                        (code, out),
                        (
                            0,
                            format!("{}.ax.io/example unchanged\n", kind.to_lowercase())
                        ),
                        "{ctx}: {err}"
                    );
                }
            }

            for ns in ["default", "team-a", "team-b"] {
                let found = match kind {
                    "Task" => env.store.get_task(ns, "example").await.map(|t| t.metadata),
                    "Workspace" => env
                        .store
                        .get_workspace(ns, "example")
                        .await
                        .map(|t| t.metadata),
                    "Gateway" => env
                        .store
                        .get_gateway(ns, "example")
                        .await
                        .map(|t| t.metadata),
                    _ => env.store.get_model(ns, "example").await.map(|t| t.metadata),
                };
                if Some(ns) == want_ns {
                    assert_eq!(found.unwrap().unwrap().atespace, ns, "{ctx}");
                } else {
                    assert!(matches!(found, Err(StoreError::NotFound)), "{ctx} in {ns}");
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn apply_configured_and_empty_documents() {
    const FIRST: &str =
        "apiVersion: ax.io/v1alpha1\nkind: Workspace\nmetadata:\n  name: first\nspec: {}\n";
    let second = FIRST.replace("name: first", "name: second");
    let cases: Vec<(&str, String, Vec<&str>, Option<&str>)> = vec![
        ("empty file", String::new(), vec![], None),
        (
            "empty documents only",
            "---\n# empty\n---\n".into(),
            vec![],
            None,
        ),
        (
            "leading empty document",
            format!("---\n---\n{FIRST}"),
            vec!["first"],
            None,
        ),
        (
            "trailing separator",
            format!("{FIRST}---\n"),
            vec!["first"],
            None,
        ),
        (
            "middle empty documents",
            format!("{FIRST}---\n# empty\n---\n---\n{second}"),
            vec!["first", "second"],
            None,
        ),
        (
            "missing kind",
            format!("{FIRST}---\n---\n{{}}\n---\n{second}"),
            vec!["first"],
            Some("applying document 3: missing kind"),
        ),
        (
            "explicit null",
            "null\n".into(),
            vec![],
            Some("applying document 1: missing kind"),
        ),
        (
            "tagged null",
            "!!null\n".into(),
            vec![],
            Some("applying document 1: missing kind"),
        ),
        (
            "tagged quoted null",
            "!!null \"\"\n".into(),
            vec![],
            Some("applying document 1: missing kind"),
        ),
        (
            "empty string",
            "\"\"\n".into(),
            vec![],
            Some("applying document 1: reading kind"),
        ),
        (
            "invalid YAML",
            format!("{FIRST}---\n[\n"),
            vec!["first"],
            Some("decoding document 2:"),
        ),
    ];
    for (name, manifest, want_names, want_err) in cases {
        let env = serve(Arc::new(MemoryStore::new())).await;
        let path = write(env.home.path(), &manifest);
        let (code, out, err) = rax(&env, &["apply", "-f", &path]);
        match want_err {
            Some(want) => {
                assert_eq!(code, 1, "{name}");
                assert!(err.contains(want), "{name}: {err}");
            }
            None => assert_eq!(code, 0, "{name}: {err}"),
        }
        let mut want_out = String::new();
        for n in &want_names {
            env.store.get_workspace("default", n).await.unwrap();
            want_out.push_str(&format!("workspace.ax.io/{n} created\n"));
        }
        assert_eq!(out, want_out, "{name}");
    }

    // A changed spec is reported as configured.
    let env = serve(Arc::new(MemoryStore::new())).await;
    let path = write(env.home.path(), FIRST);
    assert_eq!(
        rax(&env, &["apply", "-f", &path]).1,
        "workspace.ax.io/first created\n"
    );
    let changed = write(
        env.home.path(),
        &FIRST.replace("spec: {}", "spec:\n  skills:\n    path: /s"),
    );
    assert_eq!(
        rax(&env, &["apply", "-f", &changed]).1,
        "workspace.ax.io/first configured\n"
    );
    let (code, _, err) = rax(&env, &["apply"]);
    assert_eq!(code, 1);
    assert!(err.contains("missing required flag: -f <file>"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn get_resource_aliases() {
    const NS: &str = "test-space";
    let store = Arc::new(MemoryStore::new());
    for name in ["target", "other"] {
        let meta = Some(ObjectMeta {
            name: name.into(),
            atespace: NS.into(),
            ..Default::default()
        });
        store
            .save_task(&mut Task {
                metadata: meta.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .save_workspace(&mut Workspace {
                metadata: meta.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .save_gateway(&mut rax_api::v1alpha1::Gateway {
                metadata: meta.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .save_model(&mut rax_api::v1alpha1::Model {
                metadata: meta,
                ..Default::default()
            })
            .await
            .unwrap();
    }
    let env = serve(store).await;
    for resource in ["task", "workspace", "gateway", "model"] {
        for alias in [resource.to_owned(), format!("{resource}s")] {
            // List.
            let (code, out, err) = rax(&env, &["-a", NS, "get", &alias]);
            assert_eq!(code, 0, "{alias}: {err}");
            let rows: Vec<_> = out.trim().lines().collect();
            assert_eq!(rows.len(), 3, "{alias}: {out}");
            let mut names: Vec<_> = rows[1..]
                .iter()
                .map(|r| {
                    let f: Vec<_> = r.split_whitespace().collect();
                    assert_eq!(f[1], NS, "{r}");
                    f[0]
                })
                .collect();
            names.sort();
            assert_eq!(names, ["other", "target"]);

            // Get one.
            let (code, out, _) = rax(&env, &["-a", NS, "get", &alias, "target"]);
            assert_eq!(code, 0);
            let doc: serde_yaml_ng::Value = serde_yaml_ng::from_str(&out).unwrap();
            assert_eq!(doc["metadata"]["name"], "target", "{out}");
            assert_eq!(doc["metadata"]["atespace"], NS, "{out}");

            // Missing.
            let (code, out, err) = rax(&env, &["-a", NS, "get", &alias, "missing"]);
            assert_eq!(code, 1);
            assert!(out.is_empty());
            assert!(
                err.contains(&format!(
                    "getting {resource} \"missing\": rpc error: code = NotFound"
                )),
                "{err}"
            );
        }
    }
    let (code, _, err) = rax(&env, &["get", "pods"]);
    assert_eq!((code, err.trim()), (1, r#"Error: unknown resource "pods""#));
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_and_delete() {
    let env = serve(Arc::new(MemoryStore::new())).await;
    let manifest = r#"apiVersion: ax.io/v1alpha1
kind: Workspace
metadata:
  name: ws
spec:
  git:
  - name: origin
    repo: https://example.com/r.git
  mcp:
    servers:
    - name: local-mcp
      endpoint: http://localhost:9000
  skills:
    path: /workspace/.agents/skills
---
apiVersion: ax.io/v1alpha1
kind: Gateway
metadata:
  name: gw
spec:
  egress:
  - https:
      hostnames: [api.example.com, github.com]
  - tlsPassthrough:
      hostnames: [db.example.com]
      ports: {numbers: [5432]}
---
apiVersion: ax.io/v1alpha1
kind: Model
metadata:
  name: m
spec:
  provider: google
  model: gemini-2.5-pro
  secretKey:
    name: gemini-secret
    key: api-key
  parameters:
    temperature: 0.2
    maxOutputTokens: 4096
---
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  name: t
spec:
  image: img
  command: [sh, -c, "true"]
  workspace:
    name: ws
  config:
    b: 1
    a: {x: y}
"#;
    let path = write(env.home.path(), manifest);
    let (code, out, err) = rax(&env, &["apply", "-f", &path]);
    assert_eq!(code, 0, "{err}");
    assert_eq!(
        out,
        "workspace.ax.io/ws created\ngateway.ax.io/gw created\nmodel.ax.io/m created\ntask.ax.io/t created\n"
    );

    let (_, out, _) = rax(&env, &["describe", "workspace", "ws"]);
    assert_eq!(
        out,
        "Name:         ws\nAtespace:     default\nGit Repositories:\n  - origin (https://example.com/r.git, branch: main)\nMCP Servers:\n  - local-mcp: http://localhost:9000\nSkills Path:      /workspace/.agents/skills\n"
    );
    let (_, out, _) = rax(&env, &["describe", "gateway", "gw"]);
    assert_eq!(
        out,
        "Name:         gw\nAtespace:     default\nEgress Rules:\n  - https: api.example.com, github.com\n  - tlsPassthrough: db.example.com\n"
    );
    let (_, out, _) = rax(&env, &["get", "gateways"]);
    assert_eq!(out, "NAME   ATESPACE   EGRESS-RULES\ngw     default    2\n");
    let (_, out, _) = rax(&env, &["describe", "model", "m"]);
    assert_eq!(
        out,
        "Name:               m\nAtespace:           default\nProvider:           google\nModel:              gemini-2.5-pro\nParameters:\n  maxOutputTokens: 4096\n  temperature: 0.2\nSecret Key:         gemini-secret (key: api-key)\n"
    );
    let (_, out, _) = rax(&env, &["describe", "task", "t"]);
    assert!(
        out.starts_with("Name:         t\nAtespace:     default\nPhase:        "),
        "{out}"
    );
    assert!(
        out.contains("Workspace:    ws  path=/workspace\nImage:        img\nCommand:      [sh -c true]\nConfig:\n  a:\n    x: y\n  b: 1\n"),
        "{out}"
    );

    for (kind, name) in [
        ("task", "t"),
        ("workspaces", "ws"),
        ("Gateway", "gw"),
        ("Model", "m"),
    ] {
        let (code, out, err) = rax(&env, &["delete", kind, name]);
        assert_eq!(code, 0, "{err}");
        let lower = kind.to_lowercase();
        let lower = lower.trim_end_matches('s');
        assert_eq!(out, format!("{lower}.ax.io/{name} deleted\n"));
    }
    assert!(matches!(
        env.store.get_task("default", "t").await,
        Err(StoreError::NotFound)
    ));
    let (code, _, err) = rax(&env, &["delete", "task", "t"]);
    assert_eq!(code, 1);
    assert!(err.contains("NotFound"), "{err}");
}

#[test]
fn offline_commands() {
    let home = tempfile::tempdir().unwrap();
    let run = |args: &[&str]| {
        let out = Command::new(env!("CARGO_BIN_EXE_rax"))
            .args(args)
            .env("AX_HOME", home.path())
            .env("KUBECONFIG", home.path().join("missing-kubeconfig"))
            .env("PATH", "/nonexistent")
            .env_remove("KUBECONTEXT")
            .output()
            .unwrap();
        (
            out.status.code().unwrap(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };
    let (code, out, _) = run(&["version"]);
    assert_eq!((code, out.as_str()), (0, "rax version v1alpha1\n"));
    assert!(out.contains("ax version"));
    let (code, out, _) = run(&[]);
    assert_eq!(code, 1);
    assert!(out.contains("Available Commands:"));
    let (code, _, err) = run(&["frobnicate"]);
    assert_eq!(code, 1);
    assert!(err.contains("unknown command: frobnicate"));
    let (code, out, _) = run(&["ctx"]);
    assert_eq!(code, 0);
    assert!(
        out.contains("No active Kubernetes context detected."),
        "{out}"
    );
    let (code, out, _) = run(&["tunnel", "list"]);
    assert_eq!(
        (code, out.as_str()),
        (0, "No active tunnels found in ~/.ax/tunnels.\n")
    );
    let (code, out, err) = run(&["ssh"]);
    assert_eq!(code, 1);
    assert!(out.contains("rax ssh <task-name>"));
    assert!(err.contains("missing task name"));
    let (code, _, _) = run(&["ssh", "--help"]);
    assert_eq!(code, 0);

    // A saved tunnel shows up in ctx and list, and stop removes it.
    std::fs::create_dir_all(home.path().join("tunnels")).unwrap();
    std::fs::write(
        home.path().join("tunnels/kind-ax.json"),
        r#"{"context":"kind-ax","namespace":"ax-system","service":"ax-server","port":1,"pid":0,"created_at":"2026-10-29T20:00:00Z"}"#,
    )
    .unwrap();
    let (_, out, _) = run(&["--context", "kind-ax", "ctx"]);
    assert!(out.contains("Active Kubernetes Context: kind-ax"), "{out}");
    assert!(
        out.contains("-> ax-system/svc/ax-server:8080 (PID: 0, Stale / Not responding)"),
        "{out}"
    );
    let (_, out, _) = run(&["tunnel", "list"]);
    assert!(
        out.contains("kind-ax  1           ax-system/ax-server  0    Stale   2026-"),
        "{out}"
    );
    assert!(out.contains("Stale"), "{out}");
    let (code, out, _) = run(&["tunnel", "stop", "kind-ax"]);
    assert_eq!(
        (code, out.as_str()),
        (0, "Tunnel stopped for context \"kind-ax\"\n")
    );
    let (_, out, _) = run(&["--context", "kind-ax", "ctx"]);
    assert!(out.contains("No background tunnel running"), "{out}");
}

/// `--trace` prints the trace ID and sends it, sampled, as `traceparent` on
/// every RPC; without it, the CLI sends no trace context.
#[tokio::test(flavor = "multi_thread")]
async fn trace_flag() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let app = router(AxService::new(
        Arc::new(MemoryStore::new()),
        ServerOptions::default(),
    ))
    .layer(axum::middleware::from_fn({
        let seen = seen.clone();
        move |req: axum::extract::Request, next: axum::middleware::Next| {
            let seen = seen.clone();
            async move {
                let tp = req
                    .headers()
                    .get("traceparent")
                    .map(|v| v.to_str().unwrap().to_owned())
                    .unwrap_or_default();
                seen.lock().unwrap().push(tp);
                next.run(req).await
            }
        }
    }));
    tokio::spawn(async move { axum::serve(lis, app).await.unwrap() });
    let home = tempfile::tempdir().unwrap();
    let run = |args: &[&str], envs: &[(&str, &str)]| {
        let out = Command::new(env!("CARGO_BIN_EXE_rax"))
            .args(["--server", &addr])
            .args(args)
            .env("AX_HOME", home.path())
            .env_remove("AX_SERVER")
            .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
            .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
            .env_remove("OTEL_TRACES_EXPORTER")
            .envs(envs.iter().copied())
            .output()
            .unwrap();
        (
            out.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };

    let (code, err) = run(&["get", "tasks"], &[]);
    assert_eq!(code, 0, "{err}");
    assert!(!err.contains("Trace ID"), "{err}");
    assert_eq!(seen.lock().unwrap().drain(..).collect::<Vec<_>>(), [""]);

    let (code, err) = run(&["--trace", "get", "tasks"], &[]);
    assert_eq!(code, 0, "{err}");
    let id = err
        .lines()
        .find_map(|l| l.strip_prefix("Trace ID: "))
        .unwrap_or_else(|| panic!("no trace ID in {err:?}"));
    assert_eq!(id.len(), 32, "{id}");
    let sent: Vec<_> = seen.lock().unwrap().drain(..).collect();
    assert_eq!(sent.len(), 1);
    let parts: Vec<_> = sent[0].split('-').collect();
    assert_eq!((parts[0], parts[1], parts[3]), ("00", id, "01"), "{sent:?}");
    assert!(!err.contains("Warning"), "{err}");

    // The spans go to the collector the environment names; one that is not
    // there is a warning, not a failure.
    let (code, err) = run(
        &["get", "tasks", "--trace"],
        &[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")],
    );
    assert_eq!(code, 0, "{err}");
    assert!(
        err.contains("Warning: could not export the command trace to http://127.0.0.1:1"),
        "{err}"
    );
    // A failed command still ends its trace.
    let (code, err) = run(&["--trace", "get", "task", "missing"], &[]);
    assert_eq!(code, 1);
    assert!(
        err.contains("Trace ID: ") && err.contains("NotFound"),
        "{err}"
    );
}

/// A collector that keeps the trace exports it receives.
#[derive(Default, Clone)]
struct Collector(
    Arc<
        std::sync::Mutex<
            Vec<opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest>,
        >,
    >,
);

#[tonic::async_trait]
impl opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService
    for Collector
{
    async fn export(
        &self,
        req: tonic::Request<
            opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest,
        >,
    ) -> Result<
        tonic::Response<
            opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceResponse,
        >,
        tonic::Status,
    > {
        self.0.lock().unwrap().push(req.into_inner());
        Ok(tonic::Response::new(Default::default()))
    }
}

/// With an OTLP endpoint, the command's spans are exported once, when it
/// ends: the command span, the address lookup and one client span per RPC.
#[tokio::test(flavor = "multi_thread")]
async fn trace_export() {
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    let env = serve(Arc::new(MemoryStore::new())).await;
    let collector = Collector::default();
    let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let otlp = format!("http://{}", lis.local_addr().unwrap());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(TraceServiceServer::new(collector.clone()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(lis)),
    );
    let out = Command::new(env!("CARGO_BIN_EXE_rax"))
        .args(["--server", &env.addr, "--trace", "get", "workspaces"])
        .env("AX_HOME", env.home.path())
        .env_remove("AX_SERVER")
        .env_remove("OTEL_TRACES_EXPORTER")
        .env_remove("OTEL_SERVICE_NAME")
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", &otlp)
        .output()
        .unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{err}");
    assert!(!err.contains("Warning"), "{err}");

    let exports = collector.0.lock().unwrap().clone();
    assert_eq!(exports.len(), 1, "one export per command");
    let rs = &exports[0].resource_spans[0];
    let service = rs
        .resource
        .as_ref()
        .unwrap()
        .attributes
        .iter()
        .find(|a| a.key == "service.name")
        .and_then(|a| a.value.as_ref()?.value.clone());
    assert_eq!(service, Some(Value::StringValue("rax".into())));
    let mut names: Vec<_> = rs
        .scope_spans
        .iter()
        .flat_map(|s| &s.spans)
        .map(|s| s.name.as_str())
        .collect();
    names.sort();
    assert_eq!(
        names,
        [
            "ax.v1alpha1.AX/ListWorkspaces",
            "rax get",
            "resolve ax-server address"
        ]
    );
}
