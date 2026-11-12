//! The commands.

use std::io::Write;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant, SystemTime};

use rax_api::v1alpha1::{
    self as api, Gateway, Model, ObjectMeta, Task, Workspace, ax_client::AxClient,
};
use rax_api::{
    KIND_GATEWAY, KIND_MODEL, KIND_TASK, KIND_WORKSPACE, Manifest, from_yaml, phase, to_yaml,
};
use rax_guest::client::go_status;
use rax_guest::proto::Signal;
use rax_guest::{Client as GuestClient, ExecOptions};
use serde_yaml_ng::Value as Yaml;
use tokio::sync::mpsc;
use tonic::transport::Endpoint;
use tonic::{Code, Status};

use crate::Error;
use crate::args::parse_ssh_args;
use crate::format::{format_age, go_strings, go_value, sorted, table};
use crate::router_tls::{ROUTER_SERVER_NAME, router_tls_config};
use crate::trace::{self, Traced};
use crate::tunnel;

/// How long suspend and resume may take. Snapshotting or restoring a
/// microVM can take well over a minute; the server keeps its per-task lock
/// for as long as the call is in flight.
const LIFECYCLE_RPC_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// How soon a second Ctrl-C must follow the first to end `rax ssh` locally
/// instead of being forwarded.
pub const INTERRUPT_EXIT_WINDOW: Duration = Duration::from_secs(1);

/// A client of the AX server, whose calls are traced while the command is.
pub type AxConn = AxClient<Traced>;

/// Connects to the AX server at `server`, with or without an `http://`.
pub fn client(server: &str) -> Result<AxConn, Error> {
    let target = server
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let endpoint = Endpoint::from_shared(format!("http://{target}"))
        .map_err(|e| format!("connecting to {target}: {e}"))?;
    Ok(AxClient::new(Traced::new(endpoint.connect_lazy())))
}

fn req<T>(msg: T, timeout: Duration) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.set_timeout(timeout);
    r
}

fn status_err(context: &str) -> impl Fn(Status) -> Error + '_ {
    move |s| {
        if context.is_empty() {
            go_status(&s)
        } else {
            format!("{context}: {}", go_status(&s))
        }
    }
}

fn meta_of(m: &Option<ObjectMeta>) -> (&str, &str) {
    m.as_ref()
        .map_or(("", ""), |m| (m.name.as_str(), m.atespace.as_str()))
}

/// Reads the manifest named by `-f`/`--file`, or stdin for `-`. `None` when
/// there is no such flag.
fn manifest_from_args(args: &[String]) -> Result<Option<String>, Error> {
    let Some(i) = args.iter().position(|a| a == "-f" || a == "--file") else {
        return Ok(None);
    };
    let Some(path) = args.get(i + 1) else {
        return Err("-f requires a file path (or - for stdin)".into());
    };
    let data = if path == "-" {
        std::io::read_to_string(std::io::stdin())
    } else {
        std::fs::read_to_string(path)
    };
    data.map(Some).map_err(|e| format!("reading {path}: {e}"))
}

/// Splits a YAML stream into its documents, keeping the text of each.
/// Documents holding only comments or whitespace are `None`; they are
/// skipped but still counted, as AX counts them.
pub fn split_documents(data: &str) -> Vec<Option<String>> {
    fn content(lines: &[&str]) -> Option<String> {
        let has_content = lines.iter().any(|l| {
            let t = l.trim();
            !t.is_empty() && !t.starts_with('#') && !t.starts_with('%')
        });
        has_content.then(|| lines.join("\n"))
    }
    let mut docs = Vec::new();
    let mut current: Vec<&str> = Vec::new();
    let mut explicit = false;
    for line in data.lines() {
        let is_start = line == "---" || line.starts_with("--- ") || line.starts_with("---\t");
        if is_start {
            // Text before the first marker is a document only when it has
            // content.
            if explicit || content(&current).is_some() {
                docs.push(content(&current));
            }
            current = vec![line[3..].trim_start()];
            explicit = true;
        } else if line == "..." {
            docs.push(content(&current));
            current.clear();
            explicit = false;
        } else {
            current.push(line);
        }
    }
    if explicit || content(&current).is_some() {
        docs.push(content(&current));
    }
    docs
}

/// Applies the resources in a manifest. `atespace` is the `--atespace` flag
/// when it was given: it fills in missing atespaces and must match the ones
/// set. Without it the server defaults missing atespaces.
pub async fn run_apply(server: &str, atespace: Option<&str>, args: &[String]) -> Result<(), Error> {
    let data = manifest_from_args(args)?.ok_or("missing required flag: -f <file>")?;
    let mut client = client(server)?;
    let deadline = Instant::now() + Duration::from_secs(30);
    // Manifests are parsed here and sent one resource at a time through the
    // typed RPCs; the server never sees raw YAML.
    for (i, doc) in split_documents(&data).into_iter().enumerate() {
        let index = i + 1;
        let Some(doc) = doc else { continue };
        let parsed: Yaml = if doc.trim_start().starts_with("!!null") {
            Yaml::Null
        } else {
            serde_yaml_ng::from_str(&doc).map_err(|e| format!("decoding document {index}: {e}"))?
        };
        let left = deadline.saturating_duration_since(Instant::now());
        let (kind, name, outcome) = apply_document(&mut client, &doc, &parsed, atespace, left)
            .await
            .map_err(|e| format!("applying document {index}: {e}"))?;
        println!("{}.ax.io/{name} {outcome}", kind.to_lowercase());
    }
    Ok(())
}

fn set_apply_atespace(meta: Option<&mut ObjectMeta>, atespace: Option<&str>) -> Result<(), Error> {
    let (Some(meta), Some(atespace)) = (meta, atespace) else {
        return Ok(());
    };
    if !meta.atespace.is_empty() && meta.atespace != atespace {
        return Err(format!(
            "metadata.atespace {:?} does not match --atespace {atespace:?}",
            meta.atespace
        ));
    }
    meta.atespace = atespace.into();
    Ok(())
}

/// Whether an apply created, changed or left alone a resource, from the
/// lookup of the existing one.
fn apply_outcome<S: PartialEq + Default + Clone>(
    existing: Result<Option<S>, Status>,
    new: &Option<S>,
) -> Result<&'static str, Error> {
    match existing {
        Err(s) if s.code() == Code::NotFound => Ok("created"),
        Err(s) => Err(format!("looking up existing resource: {}", go_status(&s))),
        Ok(old) => {
            if old.unwrap_or_default() == new.clone().unwrap_or_default() {
                Ok("unchanged")
            } else {
                Ok("configured")
            }
        }
    }
}

fn decode<T: Manifest>(doc: &str) -> Result<T, Error> {
    from_yaml(doc).map_err(|e| e.to_string())
}

async fn apply_document(
    client: &mut AxConn,
    doc: &str,
    parsed: &Yaml,
    atespace: Option<&str>,
    timeout: Duration,
) -> Result<(String, String, &'static str), Error> {
    let kind = match parsed {
        Yaml::Null => String::new(),
        Yaml::Mapping(m) => match m.get("kind") {
            None | Some(Yaml::Null) => String::new(),
            Some(Yaml::String(s)) => s.clone(),
            Some(Yaml::Number(n)) => n.to_string(),
            Some(Yaml::Bool(b)) => b.to_string(),
            Some(_) => return Err("reading kind: kind must be a string".into()),
        },
        other => {
            return Err(format!(
                "reading kind: cannot unmarshal {} into a resource",
                yaml_type(other)
            ));
        }
    };
    let err = status_err("");
    match kind.as_str() {
        KIND_TASK => {
            let mut task: Task = decode(doc)?;
            set_apply_atespace(task.metadata.as_mut(), atespace)?;
            let res = client
                .create_task(req(api::CreateTaskRequest { task: Some(task) }, timeout))
                .await
                .map_err(&err)?
                .into_inner();
            Ok((kind, meta_of(&res.metadata).0.into(), "created"))
        }
        KIND_WORKSPACE => {
            let mut ws: Workspace = decode(doc)?;
            set_apply_atespace(ws.metadata.as_mut(), atespace)?;
            let (name, ns) = meta_of(&ws.metadata);
            let existing = client
                .get_workspace(req(
                    api::GetWorkspaceRequest {
                        atespace: ns.into(),
                        name: name.into(),
                    },
                    timeout,
                ))
                .await
                .map(|r| r.into_inner().spec);
            let outcome = apply_outcome(existing, &ws.spec)?;
            let res = client
                .update_workspace(req(
                    api::UpdateWorkspaceRequest {
                        workspace: Some(ws),
                    },
                    timeout,
                ))
                .await
                .map_err(&err)?
                .into_inner();
            Ok((kind, meta_of(&res.metadata).0.into(), outcome))
        }
        KIND_GATEWAY => {
            let mut gw: Gateway = decode(doc)?;
            set_apply_atespace(gw.metadata.as_mut(), atespace)?;
            let (name, ns) = meta_of(&gw.metadata);
            let existing = client
                .get_gateway(req(
                    api::GetGatewayRequest {
                        atespace: ns.into(),
                        name: name.into(),
                    },
                    timeout,
                ))
                .await
                .map(|r| r.into_inner().spec);
            let outcome = apply_outcome(existing, &gw.spec)?;
            let res = if outcome == "created" {
                client
                    .create_gateway(req(
                        api::CreateGatewayRequest { gateway: Some(gw) },
                        timeout,
                    ))
                    .await
            } else {
                client
                    .update_gateway(req(
                        api::UpdateGatewayRequest { gateway: Some(gw) },
                        timeout,
                    ))
                    .await
            }
            .map_err(&err)?
            .into_inner();
            Ok((kind, meta_of(&res.metadata).0.into(), outcome))
        }
        KIND_MODEL => {
            let mut m: Model = decode(doc)?;
            set_apply_atespace(m.metadata.as_mut(), atespace)?;
            let (name, ns) = meta_of(&m.metadata);
            let existing = client
                .get_model(req(
                    api::GetModelRequest {
                        atespace: ns.into(),
                        name: name.into(),
                    },
                    timeout,
                ))
                .await
                .map(|r| r.into_inner().spec);
            let outcome = apply_outcome(existing, &m.spec)?;
            let res = client
                .update_model(req(api::UpdateModelRequest { model: Some(m) }, timeout))
                .await
                .map_err(&err)?
                .into_inner();
            Ok((kind, meta_of(&res.metadata).0.into(), outcome))
        }
        "" => Err("missing kind".into()),
        other => Err(format!(
            "unsupported kind {other:?} (expected Task, Workspace, Gateway, or Model)"
        )),
    }
}

fn yaml_type(v: &Yaml) -> &'static str {
    match v {
        Yaml::Null => "!!null",
        Yaml::Bool(_) => "!!bool",
        Yaml::Number(_) => "!!int",
        Yaml::String(_) => "!!str",
        Yaml::Sequence(_) => "!!seq",
        Yaml::Mapping(_) => "!!map",
        Yaml::Tagged(_) => "tagged value",
    }
}

fn age_of(meta: &Option<ObjectMeta>) -> String {
    meta.as_ref()
        .and_then(|m| m.create_time.as_ref())
        .and_then(|t| {
            let created = SystemTime::UNIX_EPOCH
                + Duration::new(t.seconds.max(0) as u64, t.nanos.max(0) as u32);
            SystemTime::now().duration_since(created).ok()
        })
        .map_or_else(|| "<unknown>".into(), format_age)
}

fn print_yaml<T: Manifest>(m: &T) -> Result<(), Error> {
    print!("{}", to_yaml(m).map_err(|e| e.to_string())?);
    Ok(())
}

pub async fn run_get(server: &str, atespace: &str, args: &[String]) -> Result<(), Error> {
    let Some(resource) = args.first().map(|r| r.to_lowercase()) else {
        return Err(
            "specify resource to get (e.g. 'rax get tasks' or 'rax get task <name>')".into(),
        );
    };
    let mut client = client(server)?;
    let t = Duration::from_secs(15);
    let ns = atespace.to_owned();
    let name = args.get(1).cloned();
    match (resource.as_str(), name) {
        ("tasks" | "task", None) => {
            let tasks = client
                .list_tasks(req(
                    api::ListTasksRequest {
                        atespace: ns,
                        ..Default::default()
                    },
                    t,
                ))
                .await
                .map_err(status_err("listing tasks"))?
                .into_inner()
                .tasks;
            let mut rows = vec![cells(&[
                "NAME",
                "ATESPACE",
                "PHASE",
                "ACTOR",
                "WORKER-IP",
                "AGE",
            ])];
            for task in &tasks {
                let s = task.status.clone().unwrap_or_default();
                let or_none = |v: &str| {
                    if v.is_empty() {
                        "<none>".to_owned()
                    } else {
                        v.to_owned()
                    }
                };
                let (name, ns) = meta_of(&task.metadata);
                rows.push(vec![
                    name.into(),
                    ns.into(),
                    if s.phase.is_empty() {
                        phase::PENDING.into()
                    } else {
                        s.phase.clone()
                    },
                    or_none(&s.actor),
                    or_none(&s.worker_ip),
                    age_of(&task.metadata),
                ]);
            }
            print!("{}", table(&rows, 3));
            Ok(())
        }
        ("tasks" | "task", Some(name)) => {
            let task = client
                .get_task(req(
                    api::GetTaskRequest {
                        atespace: ns,
                        name: name.clone(),
                    },
                    t,
                ))
                .await
                .map_err(status_err(&format!("getting task {name:?}")))?
                .into_inner();
            print_yaml(&task)
        }
        ("workspaces" | "workspace", None) => {
            let list = client
                .list_workspaces(req(api::ListWorkspacesRequest { atespace: ns }, t))
                .await
                .map_err(status_err("listing workspaces"))?
                .into_inner()
                .workspaces;
            let mut rows = vec![cells(&["NAME", "ATESPACE", "GIT-REPOS", "MCP-SERVERS"])];
            for ws in &list {
                let (git, mcp) = ws.spec.as_ref().map_or((0, 0), |s| {
                    (s.git.len(), s.mcp.as_ref().map_or(0, |m| m.servers.len()))
                });
                let (name, ns) = meta_of(&ws.metadata);
                rows.push(vec![
                    name.into(),
                    ns.into(),
                    git.to_string(),
                    mcp.to_string(),
                ]);
            }
            print!("{}", table(&rows, 3));
            Ok(())
        }
        ("workspaces" | "workspace", Some(name)) => {
            let ws = client
                .get_workspace(req(
                    api::GetWorkspaceRequest {
                        atespace: ns,
                        name: name.clone(),
                    },
                    t,
                ))
                .await
                .map_err(status_err(&format!("getting workspace {name:?}")))?
                .into_inner();
            print_yaml(&ws)
        }
        ("gateways" | "gateway", None) => {
            let list = client
                .list_gateways(req(api::ListGatewaysRequest { atespace: ns }, t))
                .await
                .map_err(status_err("listing gateways"))?
                .into_inner()
                .gateways;
            let mut rows = vec![cells(&["NAME", "ATESPACE", "EGRESS-RULES"])];
            for gw in &list {
                let rules = gw.spec.as_ref().map_or(0, |s| s.egress.len());
                let (name, ns) = meta_of(&gw.metadata);
                rows.push(vec![name.into(), ns.into(), rules.to_string()]);
            }
            print!("{}", table(&rows, 3));
            Ok(())
        }
        ("gateways" | "gateway", Some(name)) => {
            let gw = client
                .get_gateway(req(
                    api::GetGatewayRequest {
                        atespace: ns,
                        name: name.clone(),
                    },
                    t,
                ))
                .await
                .map_err(status_err(&format!("getting gateway {name:?}")))?
                .into_inner();
            print_yaml(&gw)
        }
        ("models" | "model", None) => {
            let list = client
                .list_models(req(api::ListModelsRequest { atespace: ns }, t))
                .await
                .map_err(status_err("listing models"))?
                .into_inner()
                .models;
            let mut rows = vec![cells(&["NAME", "ATESPACE", "PROVIDER", "MODEL"])];
            for m in &list {
                let (provider, model) =
                    m.spec.as_ref().map_or((String::new(), String::new()), |s| {
                        (s.provider.clone(), s.model.clone())
                    });
                let (name, ns) = meta_of(&m.metadata);
                rows.push(vec![name.into(), ns.into(), provider, model]);
            }
            print!("{}", table(&rows, 3));
            Ok(())
        }
        ("models" | "model", Some(name)) => {
            let m = client
                .get_model(req(
                    api::GetModelRequest {
                        atespace: ns,
                        name: name.clone(),
                    },
                    t,
                ))
                .await
                .map_err(status_err(&format!("getting model {name:?}")))?
                .into_inner();
            print_yaml(&m)
        }
        (other, _) => Err(format!("unknown resource {other:?}")),
    }
}

fn cells(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

fn struct_json(s: &Option<pbjson_types::Struct>) -> serde_json::Map<String, serde_json::Value> {
    match s.as_ref().and_then(|s| serde_json::to_value(s).ok()) {
        Some(serde_json::Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    }
}

pub async fn run_describe(server: &str, atespace: &str, args: &[String]) -> Result<(), Error> {
    if args.len() < 2 {
        return Err("usage: rax describe <task|workspace|gateway|model> <name>".into());
    }
    let kind = args[0].to_lowercase();
    let name = args[1].clone();
    let mut client = client(server)?;
    let t = Duration::from_secs(15);
    let ns = atespace.to_owned();

    if kind == "model" || kind == "models" {
        let m = client
            .get_model(req(
                api::GetModelRequest {
                    atespace: ns,
                    name: name.clone(),
                },
                t,
            ))
            .await
            .map_err(status_err(&format!("getting model {name:?}")))?
            .into_inner();
        let (n, a) = meta_of(&m.metadata);
        println!("Name:               {n}");
        println!("Atespace:           {a}");
        if let Some(spec) = &m.spec {
            println!("Provider:           {}", spec.provider);
            println!("Model:              {}", spec.model);
            let params = struct_json(&spec.parameters);
            if !params.is_empty() {
                println!("Parameters:");
                let mut keys: Vec<_> = params.keys().collect();
                keys.sort();
                for k in keys {
                    println!("  {k}: {}", go_value(&params[k]));
                }
            }
            if let Some(key) = &spec.secret_key {
                if !key.name.is_empty() && !key.key.is_empty() && key.name != key.key {
                    println!("Secret Key:         {} (key: {})", key.name, key.key);
                } else if !key.key.is_empty() {
                    println!("Secret Key:         {}", key.key);
                } else if !key.name.is_empty() {
                    println!("Secret Key:         {}", key.name);
                }
            }
        }
        return Ok(());
    }

    if kind == "gateway" || kind == "gateways" {
        let gw = client
            .get_gateway(req(
                api::GetGatewayRequest {
                    atespace: ns,
                    name: name.clone(),
                },
                t,
            ))
            .await
            .map_err(status_err(&format!("getting gateway {name:?}")))?
            .into_inner();
        let (n, a) = meta_of(&gw.metadata);
        println!("Name:         {n}");
        println!("Atespace:     {a}");
        if let Some(spec) = gw.spec.as_ref().filter(|s| !s.egress.is_empty()) {
            println!("Egress Rules:");
            for rule in &spec.egress {
                if let Some(r) = &rule.http {
                    println!("  - http: {}", r.hostnames.join(", "));
                }
                if let Some(r) = &rule.https {
                    println!("  - https: {}", r.hostnames.join(", "));
                }
                if let Some(r) = &rule.tls_passthrough {
                    println!("  - tlsPassthrough: {}", r.hostnames.join(", "));
                }
            }
        }
        return Ok(());
    }

    if kind == "workspace" || kind == "workspaces" {
        let ws = client
            .get_workspace(req(
                api::GetWorkspaceRequest {
                    atespace: ns,
                    name: name.clone(),
                },
                t,
            ))
            .await
            .map_err(status_err(&format!("getting workspace {name:?}")))?
            .into_inner();
        let (n, a) = meta_of(&ws.metadata);
        println!("Name:         {n}");
        println!("Atespace:     {a}");
        if let Some(spec) = &ws.spec {
            if !spec.git.is_empty() {
                println!("Git Repositories:");
                for g in &spec.git {
                    let branch = if g.branch.is_empty() {
                        "main"
                    } else {
                        &g.branch
                    };
                    println!("  - {} ({}, branch: {branch})", g.name, g.repo);
                }
            }
            if let Some(mcp) = &spec.mcp {
                if !mcp.registries.is_empty() {
                    println!("MCP Registries:");
                    for r in &mcp.registries {
                        println!("  - Provider: {}, Query: {:?}", r.provider, r.query);
                    }
                }
                if !mcp.servers.is_empty() {
                    println!("MCP Servers:");
                    for s in &mcp.servers {
                        if !s.endpoint.is_empty() {
                            println!("  - {}: {}", s.name, s.endpoint);
                        } else {
                            println!("  - {}: {} {}", s.name, s.command, go_strings(&s.args));
                        }
                    }
                }
            }
            if let Some(skills) = &spec.skills {
                if !skills.registries.is_empty() {
                    println!("Skill Registries:");
                    for r in &skills.registries {
                        println!("  - Provider: {}, Query: {:?}", r.provider, r.query);
                    }
                }
                if !skills.path.is_empty() {
                    println!("Skills Path:      {}", skills.path);
                }
            }
        }
        return Ok(());
    }

    let task = client
        .get_task(req(
            api::GetTaskRequest {
                atespace: ns,
                name: name.clone(),
            },
            t,
        ))
        .await
        .map_err(status_err(&format!("getting task {name:?}")))?
        .into_inner();
    let (n, a) = meta_of(&task.metadata);
    let status = task.status.clone().unwrap_or_default();
    println!("Name:         {n}");
    println!("Atespace:     {a}");
    println!("Phase:        {}", status.phase);
    println!("Actor:        {}", status.actor);
    println!("Worker IP:    {}", status.worker_ip);
    if let Some(spec) = &task.spec {
        if let Some(r) = &spec.workspace {
            println!("Workspace:    {}  path={}", r.name, spec.workspace_path());
        }
        if !spec.image.is_empty() {
            println!("Image:        {}", spec.image);
        }
        if !spec.command.is_empty() {
            println!("Command:      {}", go_strings(&spec.command));
        }
        let config = struct_json(&spec.config);
        if !config.is_empty()
            && let Ok(out) = serde_yaml_ng::to_string(&sorted(&serde_json::Value::Object(config)))
        {
            println!("Config:");
            for line in out.trim_end_matches('\n').lines() {
                println!("  {line}");
            }
        }
    }
    if !status.conditions.is_empty() {
        println!("\nConditions:");
        let mut rows = vec![cells(&["  TYPE", "STATUS", "REASON", "MESSAGE"])];
        for c in &status.conditions {
            rows.push(vec![
                format!("  {}", c.r#type),
                c.status.clone(),
                c.reason.clone(),
                c.message.clone(),
            ]);
        }
        print!("{}", table(&rows, 2));
    }
    Ok(())
}

/// Maps what a user typed (`task`, `tasks`, `Task`) to the manifest kind.
pub fn normalize_kind(kind: &str) -> Result<&'static str, Error> {
    let lower = kind.to_lowercase();
    match lower.strip_suffix('s').unwrap_or(&lower) {
        "task" => Ok(KIND_TASK),
        "workspace" => Ok(KIND_WORKSPACE),
        "gateway" => Ok(KIND_GATEWAY),
        "model" => Ok(KIND_MODEL),
        "" => Err("missing kind".into()),
        _ => Err(format!(
            "unsupported kind {kind:?} (expected task, workspace, gateway, or model)"
        )),
    }
}

pub async fn run_delete(server: &str, atespace: &str, args: &[String]) -> Result<(), Error> {
    if args.len() < 2 {
        return Err("usage: rax delete <task|workspace|gateway|model> <name>".into());
    }
    let kind = normalize_kind(&args[0])?;
    let name = args[1].clone();
    let mut client = client(server)?;
    let t = if kind == KIND_TASK {
        Duration::from_secs(120)
    } else {
        Duration::from_secs(30)
    };
    let ns = atespace.to_owned();
    let res = match kind {
        KIND_TASK => client
            .delete_task(req(
                api::DeleteTaskRequest {
                    atespace: ns,
                    name: name.clone(),
                },
                t,
            ))
            .await
            .map(|_| ()),
        KIND_WORKSPACE => client
            .delete_workspace(req(
                api::DeleteWorkspaceRequest {
                    atespace: ns,
                    name: name.clone(),
                },
                t,
            ))
            .await
            .map(|_| ()),
        KIND_GATEWAY => client
            .delete_gateway(req(
                api::DeleteGatewayRequest {
                    atespace: ns,
                    name: name.clone(),
                },
                t,
            ))
            .await
            .map(|_| ()),
        _ => client
            .delete_model(req(
                api::DeleteModelRequest {
                    atespace: ns,
                    name: name.clone(),
                },
                t,
            ))
            .await
            .map(|_| ()),
    };
    let lower = kind.to_lowercase();
    res.map_err(status_err(&format!("deleting {lower} {atespace}/{name}")))?;
    println!("{lower}.ax.io/{name} deleted");
    Ok(())
}

fn lifecycle_name(args: &[String], verb: &str) -> Result<String, Error> {
    match args {
        [name] => Ok(name.clone()),
        [first, second, ..] if first == "task" || first == "tasks" => Ok(second.clone()),
        [first, ..] => Ok(first.clone()),
        [] => Err(format!("usage: rax {verb} task <name>")),
    }
}

pub async fn run_suspend(server: &str, atespace: &str, args: &[String]) -> Result<(), Error> {
    let name = lifecycle_name(args, "suspend")?;
    client(server)?
        .suspend_task(req(
            api::SuspendTaskRequest {
                atespace: atespace.into(),
                name: name.clone(),
            },
            LIFECYCLE_RPC_TIMEOUT,
        ))
        .await
        .map_err(status_err("suspending task"))?;
    println!("task.ax.io/{name} suspended");
    Ok(())
}

pub async fn run_resume(server: &str, atespace: &str, args: &[String]) -> Result<(), Error> {
    let name = lifecycle_name(args, "resume")?;
    client(server)?
        .resume_task(req(
            api::ResumeTaskRequest {
                atespace: atespace.into(),
                name: name.clone(),
            },
            LIFECYCLE_RPC_TIMEOUT,
        ))
        .await
        .map_err(status_err("resuming task"))?;
    println!("task.ax.io/{name} resumed");
    Ok(())
}

pub fn run_context(kube_context: &str) {
    let ctx = match tunnel::current_context(kube_context) {
        Ok(c) if !c.is_empty() => c,
        _ => {
            println!("No active Kubernetes context detected.");
            println!("Fallback server: http://localhost:8080 (or set $AX_SERVER / --server)");
            return;
        }
    };
    println!("Active Kubernetes Context: {ctx}");
    match tunnel::get_tunnel(&ctx) {
        Ok(t) => {
            let status = if tunnel::is_tunnel_healthy(t.port) {
                "Healthy"
            } else {
                "Stale / Not responding"
            };
            println!(
                "AX Tunnel:                 http://127.0.0.1:{} -> {}/svc/{}:8080 (PID: {}, {status})",
                t.port, t.namespace, t.service, t.pid
            );
        }
        Err(_) => println!(
            "AX Tunnel:                 No background tunnel running (will auto-connect on next command)"
        ),
    }
}

pub fn run_tunnel(args: &[String]) -> Result<(), Error> {
    let Some(action) = args.first() else {
        return Err(
            "specify tunnel action: 'rax tunnel list' or 'rax tunnel stop [context]'".into(),
        );
    };
    match action.as_str() {
        "list" => {
            let tunnels = tunnel::list_tunnels()?;
            if tunnels.is_empty() {
                println!("No active tunnels found in ~/.ax/tunnels.");
                return Ok(());
            }
            let mut rows = vec![cells(&[
                "CONTEXT",
                "LOCAL PORT",
                "REMOTE SERVICE",
                "PID",
                "STATUS",
                "CREATED",
            ])];
            for t in tunnels {
                let status = if tunnel::is_tunnel_healthy(t.port) {
                    "Active"
                } else {
                    "Stale"
                };
                let created = chrono::DateTime::parse_from_rfc3339(&t.created_at)
                    .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or(t.created_at.clone());
                rows.push(vec![
                    t.context,
                    t.port.to_string(),
                    format!("{}/{}", t.namespace, t.service),
                    t.pid.to_string(),
                    status.into(),
                    created,
                ]);
            }
            print!("{}", table(&rows, 2));
            Ok(())
        }
        "stop" => {
            let ctx = match args.get(1) {
                Some(c) => c.clone(),
                None => tunnel::current_context("")
                    .ok()
                    .filter(|c| !c.is_empty())
                    .ok_or("no current context found to stop tunnel")?,
            };
            tunnel::stop_tunnel_by_context(&ctx)
                .map_err(|e| format!("stopping tunnel for context {ctx:?}: {e}"))?;
            println!("Tunnel stopped for context {ctx:?}");
            Ok(())
        }
        other => Err(format!(
            "unknown tunnel subcommand: {other} (available: list, stop)"
        )),
    }
}

/// Splits a worker IP into host and port; the runner's guest port is 80
/// unless the IP carries one.
pub fn guest_endpoint(worker_ip: &str) -> (String, u16) {
    if let Ok(addr) = worker_ip.parse::<SocketAddr>() {
        return (addr.ip().to_string(), addr.port());
    }
    match worker_ip.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') => match port.parse() {
            Ok(p) => (host.to_owned(), p),
            Err(_) => (host.to_owned(), 80),
        },
        _ => (worker_ip.trim_matches(['[', ']']).to_owned(), 80),
    }
}

fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Relays each interrupt to the returned channel, except that one arriving
/// within `window` of the previous one calls `exit` instead, so a single
/// Ctrl-C reaches the remote command and a repeated one still escapes.
pub fn escalate_interrupts(
    mut interrupts: mpsc::Receiver<()>,
    window: Duration,
    exit: impl Fn() + Send + 'static,
) -> mpsc::Receiver<Signal> {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let mut last: Option<Instant> = None;
        while interrupts.recv().await.is_some() {
            let now = Instant::now();
            if last.is_some_and(|l| now.duration_since(l) < window) {
                exit();
                return;
            }
            last = Some(now);
            let _ = tx.try_send(Signal::Int);
        }
    });
    rx
}

/// Connects to a task's guest services: directly when the worker is
/// reachable, else through Substrate's atenet router (mTLS on 443, falling
/// back to plaintext on 80). Returns the port-forward the connection runs
/// over, if any.
fn dial_guest(
    host: &str,
    port: u16,
    target_actor: &str,
    kube_context: &str,
    ax_namespace: &str,
) -> Result<(GuestClient, Option<tunnel::PortForward>), Error> {
    // 1. The worker directly, when it is reachable (in-cluster or a mesh).
    let endpoint = join_host_port(host, port);
    let direct = endpoint
        .to_socket_addrs()
        .ok()
        .and_then(|mut a| a.next())
        .is_some_and(|a| TcpStream::connect_timeout(&a, Duration::from_millis(500)).is_ok());
    if direct {
        let c = GuestClient::connect(&endpoint, None, None)
            .map_err(|e| format!("connecting to guest at {endpoint}: {e}"))?;
        return Ok((c, None));
    }

    // 2. Through Substrate's atenet router: mTLS on 443, else plaintext 80.
    let ax_namespace = if ax_namespace.is_empty() {
        rax_api::DEFAULT_SYSTEM_NAMESPACE
    } else {
        ax_namespace
    };
    let setup_err = match dial_router_tls(target_actor, kube_context, ax_namespace)? {
        Ok(connected) => return Ok(connected),
        Err(setup_err) => setup_err,
    };
    let pf = tunnel::port_forward(kube_context, "ate-system", "svc/atenet-router", 80)
        .map_err(|e| format!("establishing port-forward to atenet-router: {e} ({setup_err})"))?;
    let router = format!("127.0.0.1:{}", pf.port);
    let c = GuestClient::connect(&router, Some(target_actor), None)
        .map_err(|e| format!("connecting to guest via atenet-router at {router}: {e}"))?;
    Ok((c, Some(pf)))
}

/// Connects to the guest through the atenet router over mTLS on 443. The
/// outer error is a failed connection, which ends `rax ssh`; the inner one is
/// a failure to set up mTLS or the port-forward, after which the plaintext
/// route is tried.
#[allow(clippy::type_complexity)]
fn dial_router_tls(
    target_actor: &str,
    kube_context: &str,
    ax_namespace: &str,
) -> Result<Result<(GuestClient, Option<tunnel::PortForward>), String>, Error> {
    let tls = match router_tls_config(kube_context, ax_namespace, "ax-server") {
        Ok(tls) => tls,
        Err(e) => return Ok(Err(format!("mTLS setup error: {e}"))),
    };
    let pf = match tunnel::port_forward(kube_context, "ate-system", "svc/atenet-router", 443) {
        Ok(pf) => pf,
        Err(e) => return Ok(Err(format!("port 443 forward error: {e}"))),
    };
    let router = format!("127.0.0.1:{}", pf.port);
    let c = GuestClient::connect(
        &router,
        Some(target_actor),
        Some((tls, ROUTER_SERVER_NAME.to_owned())),
    )
    .map_err(|e| format!("connecting to guest via atenet-router at {router}: {e}"))?;
    Ok(Ok((c, Some(pf))))
}

fn stdin_is_terminal() -> bool {
    // SAFETY: isatty only inspects the descriptor.
    unsafe { libc::isatty(0) == 1 }
}

/// Runs a command in a task and returns its exit status.
pub async fn run_ssh(
    server: &str,
    atespace: &str,
    kube_context: &str,
    ax_namespace: &str,
    args: &[String],
) -> Result<i32, Error> {
    let parsed = parse_ssh_args(args)?;
    let task_name = parsed.task;
    let mut command = parsed.command;
    let task = client(server)?
        .get_task(req(
            api::GetTaskRequest {
                atespace: atespace.into(),
                name: task_name.clone(),
            },
            Duration::from_secs(10),
        ))
        .await
        .map_err(status_err(&format!("fetching task {task_name:?}")))?
        .into_inner();
    let Some(status) = &task.status else {
        return Err(format!("task {task_name:?} has no status available"));
    };
    if status.phase != phase::RUNNING {
        return Err(format!(
            "task {task_name:?} is in phase {:?} (must be Running to ssh)",
            status.phase
        ));
    }
    if !task.spec.as_ref().is_some_and(|s| s.debug) {
        return Err(format!(
            "task {task_name:?} does not expose guest services; set spec.debug: true and re-apply to enable rax ssh"
        ));
    }
    if status.worker_ip.is_empty() {
        return Err(format!("task {task_name:?} has no worker IP assigned"));
    }

    let (host, port) = guest_endpoint(&status.worker_ip);
    let target_actor = format!("{}/{}", meta_of(&task.metadata).1, status.actor);
    // The port-forward, if any, is kept alive for the session.
    let (guest, _forward) = dial_guest(&host, port, &target_actor, kube_context, ax_namespace)?;

    // A bare shell on a terminal runs interactively, so it prompts and Ctrl-C
    // stops what it runs rather than the shell.
    if stdin_is_terminal() && command == ["/bin/sh"] {
        command.push("-i".into());
    }

    // Ctrl-C goes to the remote command. Twice in quick succession ends the
    // session locally, so a hung connection never traps the terminal.
    let (int_tx, int_rx) = mpsc::channel(4);
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|e| e.to_string())?;
    tokio::spawn(async move {
        while sigint.recv().await.is_some() {
            if int_tx.send(()).await.is_err() {
                return;
            }
        }
    });
    let signals = escalate_interrupts(int_rx, INTERRUPT_EXIT_WINDOW, || {
        eprintln!("\nrax ssh: interrupted twice, exiting");
        if let Some(t) = trace::active() {
            t.end_within(
                Some("rax ssh: interrupted twice"),
                trace::INTERRUPT_FLUSH_TIMEOUT,
            );
        }
        std::process::exit(130);
    });

    let code = guest
        .exec(ExecOptions {
            command,
            cwd: String::new(),
            env: Default::default(),
            stdin: Some(Box::new(tokio::io::stdin())),
            stdout: Box::new(std::io::stdout()),
            stderr: Box::new(std::io::stderr()),
            signals: Some(signals),
        })
        .await?;
    let _ = std::io::stdout().flush();
    Ok(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_documents_like_ax() {
        let first =
            "apiVersion: ax.io/v1alpha1\nkind: Workspace\nmetadata:\n  name: first\nspec: {}\n";
        let count = |s: &str| {
            split_documents(s)
                .iter()
                .map(|d| d.is_some())
                .collect::<Vec<_>>()
        };
        assert_eq!(count(""), Vec::<bool>::new());
        assert_eq!(count("---\n# empty\n---\n"), [false, false]);
        assert_eq!(count(&format!("---\n---\n{first}")), [false, true]);
        assert_eq!(count(&format!("{first}---\n")), [true, false]);
        assert_eq!(
            count(&format!("{first}---\n---\n{{}}\n---\n{first}")),
            [true, false, true, true]
        );
        assert_eq!(count("--- !!null\n"), [true]);
        assert_eq!(
            split_documents("# lead\nkind: Task\n").len(),
            1,
            "comments before content belong to the first document"
        );
    }

    #[test]
    fn normalizes_kinds() {
        assert_eq!(normalize_kind("Tasks").unwrap(), KIND_TASK);
        assert_eq!(normalize_kind("workspace").unwrap(), KIND_WORKSPACE);
        assert_eq!(normalize_kind("models").unwrap(), KIND_MODEL);
        assert_eq!(normalize_kind("s").unwrap_err(), "missing kind");
        assert!(normalize_kind("pod").is_err());
    }

    #[test]
    fn guest_endpoints() {
        assert_eq!(guest_endpoint("10.0.0.5"), ("10.0.0.5".into(), 80));
        assert_eq!(guest_endpoint("10.0.0.5:8080"), ("10.0.0.5".into(), 8080));
        assert_eq!(guest_endpoint("[fd00::1]:81"), ("fd00::1".into(), 81));
        assert_eq!(guest_endpoint("fd00::1"), ("fd00::1".into(), 80));
        assert_eq!(join_host_port("fd00::1", 80), "[fd00::1]:80");
    }

    #[tokio::test]
    async fn escalates_interrupts() {
        let (tx, rx) = mpsc::channel(2);
        let exited = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = exited.clone();
        let mut out = escalate_interrupts(rx, Duration::from_secs(60), move || {
            flag.store(true, std::sync::atomic::Ordering::SeqCst)
        });
        tx.send(()).await.unwrap();
        assert_eq!(out.recv().await, Some(Signal::Int));
        assert!(!exited.load(std::sync::atomic::Ordering::SeqCst));
        tx.send(()).await.unwrap();
        // The second interrupt exits instead of being forwarded.
        assert_eq!(out.recv().await, None);
        assert!(exited.load(std::sync::atomic::Ordering::SeqCst));

        let (tx, rx) = mpsc::channel(2);
        let mut out = escalate_interrupts(rx, Duration::from_millis(10), || {
            panic!("exited on a slow second interrupt")
        });
        tx.send(()).await.unwrap();
        out.recv().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(()).await.unwrap();
        assert_eq!(out.recv().await, Some(Signal::Int));
    }
}
