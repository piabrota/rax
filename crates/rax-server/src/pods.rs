//! Tasks as plain Kubernetes pods, for clusters without Agent Substrate (a
//! small node, no gVisor). RAX's own mode, not AX's: each resume runs the
//! task once in a new pod in one namespace, with no sandbox, snapshot or
//! workspace setup, and a pod that ends is the task suspended again.
//!
//! | Task | Pod |
//! |---|---|
//! | created, or suspended | no pod running; a running one is deleted |
//! | resumed | a new pod (older ones of the task are deleted first) |
//! | Running / Resuming | the pod is Running / Pending |
//! | Suspended | the pod exited 0, or there is none |
//! | Failed | the pod exited non-zero, or cannot start (for example its image) |
//! | deleted | its pods are deleted |
//!
//! Secrets reach a task only through the server's `env_from_secrets`: a
//! task's `env` holds plain values.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use http::{Method, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use rax_api::v1alpha1::{Condition, Task, TaskSpec, TaskStatus, Workspace};
use rax_api::{DEFAULT_TASK_IMAGE, condition, reason, to_yaml, validate_resources};
use rax_substrate::tls::{self, TlsOptions};
use rustls::ClientConfig;
use serde_json::{Value, json};
use tonic::Status;
use tracing::info;

use crate::tasks::now;
use crate::{Actor, ActorState, Actors, Reconciler};

/// The labels that tie a pod to its task.
const LABEL_MANAGED: &str = "ax.io/managed-by";
const LABEL_ATESPACE: &str = "ax.io/atespace";
const LABEL_TASK: &str = "ax.io/task";
const MANAGER: &str = "rax-server";

const SERVICE_ACCOUNT_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// Container waiting reasons after which the pod will not start by itself.
const STUCK_REASONS: &[&str] = &[
    "ErrImagePull",
    "ImagePullBackOff",
    "InvalidImageName",
    "CreateContainerConfigError",
    "CreateContainerError",
];

/// A failed Kubernetes API call.
#[derive(Debug, Clone)]
pub struct KubeError {
    /// The HTTP status, or `None` when the API could not be reached.
    pub status: Option<StatusCode>,
    pub message: String,
}

impl KubeError {
    fn unreachable(e: impl std::fmt::Display) -> Self {
        Self {
            status: None,
            message: e.to_string(),
        }
    }

    pub fn is_not_found(&self) -> bool {
        self.status == Some(StatusCode::NOT_FOUND)
    }

    /// Reports whether retrying may succeed: the API was unreachable, busy,
    /// or refused for now (a quota that is full until another task ends).
    pub fn is_transient(&self) -> bool {
        match self.status {
            None => true,
            Some(s) => {
                s.is_server_error()
                    || s == StatusCode::TOO_MANY_REQUESTS
                    || s == StatusCode::CONFLICT
                    || (s == StatusCode::FORBIDDEN && self.message.contains("exceeded quota"))
            }
        }
    }

    fn to_status(&self, context: &str) -> Status {
        let msg = format!("{context}: {}", self.message);
        if self.is_transient() {
            Status::unavailable(msg)
        } else if self.is_not_found() {
            Status::not_found(msg)
        } else {
            Status::failed_precondition(msg)
        }
    }
}

impl std::fmt::Display for KubeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.status {
            Some(s) => write!(f, "{s}: {}", self.message),
            None => write!(f, "{}", self.message),
        }
    }
}

/// A minimal client for the Kubernetes API: JSON over HTTP/1.1, one
/// connection per request, the bearer token read on every request so a
/// rotated one is used.
#[derive(Clone)]
pub struct KubeClient {
    host: String,
    port: u16,
    tls: Option<Arc<ClientConfig>>,
    token_file: Option<PathBuf>,
}

impl KubeClient {
    /// The API server as a pod sees it: `KUBERNETES_SERVICE_HOST` and
    /// `_PORT`, with the mounted service account token and CA.
    pub fn in_cluster() -> io::Result<Self> {
        let var = |k: &str| {
            std::env::var(k).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("{k} is not set: not in a pod"),
                )
            })
        };
        let host = var("KUBERNETES_SERVICE_HOST")?;
        let port = var("KUBERNETES_SERVICE_PORT")?.parse().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("KUBERNETES_SERVICE_PORT: {e}"),
            )
        })?;
        let dir = PathBuf::from(SERVICE_ACCOUNT_DIR);
        let tls = tls::client_config(&TlsOptions {
            ca_file: Some(dir.join("ca.crt")),
            alpn: vec![b"http/1.1".to_vec()],
            ..Default::default()
        })?;
        Ok(Self {
            host,
            port,
            tls: Some(tls),
            token_file: Some(dir.join("token")),
        })
    }

    /// A plaintext client with no token, for tests against a fake API.
    pub fn plaintext(host: &str, port: u16) -> Self {
        Self {
            host: host.to_owned(),
            port,
            tls: None,
            token_file: None,
        }
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value, KubeError> {
        let io = tls::connect(&self.host, self.port, self.tls.as_ref().map(|c| (c, None)))
            .await
            .map_err(KubeError::unreachable)?;
        let (mut sender, conn) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(io))
                .await
                .map_err(KubeError::unreachable)?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let mut req = http::Request::builder()
            .method(method)
            .uri(path)
            .header(http::header::HOST, format!("{}:{}", self.host, self.port))
            .header(http::header::ACCEPT, "application/json");
        if let Some(file) = &self.token_file {
            // A few hundred bytes from tmpfs: no need for tokio's fs.
            let token = std::fs::read_to_string(file)
                .map_err(|e| KubeError::unreachable(format!("reading {}: {e}", file.display())))?;
            req = req.header(
                http::header::AUTHORIZATION,
                format!("Bearer {}", token.trim()),
            );
        }
        let bytes = match body {
            Some(b) => {
                req = req.header(http::header::CONTENT_TYPE, "application/json");
                Bytes::from(serde_json::to_vec(b).expect("serializing JSON"))
            }
            None => Bytes::new(),
        };
        let resp = sender
            .send_request(req.body(Full::new(bytes)).map_err(KubeError::unreachable)?)
            .await
            .map_err(KubeError::unreachable)?;
        let status = resp.status();
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(KubeError::unreachable)?
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        if status.is_success() {
            return Ok(value);
        }
        let message = value["message"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| String::from_utf8_lossy(&body).into_owned());
        Err(KubeError {
            status: Some(status),
            message,
        })
    }
}

/// Settings for the pod runner.
#[derive(Debug, Clone)]
pub struct PodOptions {
    /// The namespace every task pod runs in.
    pub namespace: String,
    /// The image for tasks that leave `spec.image` empty.
    pub default_task_image: String,
    /// Secrets whose keys become every task's environment variables.
    pub env_from_secrets: Vec<String>,
    /// The pods' service account. Empty mounts no token.
    pub service_account: String,
    /// Limits for tasks that set none, as Kubernetes quantities.
    pub default_cpu_limit: String,
    pub default_memory_limit: String,
}

impl Default for PodOptions {
    fn default() -> Self {
        Self {
            namespace: "ax-tasks".into(),
            default_task_image: DEFAULT_TASK_IMAGE.into(),
            env_from_secrets: Vec::new(),
            service_account: String::new(),
            default_cpu_limit: "500m".into(),
            default_memory_limit: "256Mi".into(),
        }
    }
}

fn atespace_of(task: &Task) -> String {
    task.metadata
        .as_ref()
        .map(|m| m.atespace.clone())
        .filter(|a| !a.is_empty())
        .unwrap_or_else(|| "default".into())
}

fn selector(atespace: &str, task: Option<&str>) -> String {
    let mut s = format!("{LABEL_MANAGED}={MANAGER},{LABEL_ATESPACE}={atespace}");
    if let Some(t) = task {
        s.push_str(&format!(",{LABEL_TASK}={t}"));
    }
    s
}

/// The pod for one run of `task`, named `<atespace>-<task>-<run>` (`run` in
/// milliseconds since the epoch).
pub fn pod_manifest(
    task: &Task,
    workspace: Option<&Workspace>,
    opts: &PodOptions,
    run: u64,
) -> Value {
    let meta = task.metadata.as_ref();
    let name = meta.map_or("", |m| m.name.as_str());
    let atespace = atespace_of(task);
    let default_spec = TaskSpec::default();
    let spec = task.spec.as_ref().unwrap_or(&default_spec);
    let image = if spec.image.is_empty() {
        &opts.default_task_image
    } else {
        &spec.image
    };

    let mut env: Vec<Value> = spec
        .env
        .iter()
        .filter(|e| !e.name.is_empty())
        .map(|e| json!({"name": e.name, "value": e.value}))
        .collect();
    let launch = Task {
        status: None,
        ..task.clone()
    };
    env.push(json!({"name": "AX_TASK_NAME", "value": name}));
    env.push(json!({"name": "AX_TASK_ATESPACE", "value": atespace}));
    if let Ok(y) = to_yaml(&launch) {
        env.push(json!({"name": "AX_TASK_YAML", "value": y}));
    }
    if let Some(ws) = workspace
        && let Ok(y) = to_yaml(ws)
    {
        env.push(json!({"name": "AX_WORKSPACE_YAML", "value": y}));
    }

    let limits = spec.resources.as_ref().and_then(|r| r.limits.as_ref());
    let pick = |v: Option<&String>, default: &String| {
        v.filter(|s| !s.is_empty()).unwrap_or(default).clone()
    };
    let cpu = pick(limits.map(|l| &l.cpu), &opts.default_cpu_limit);
    let memory = pick(limits.map(|l| &l.memory), &opts.default_memory_limit);

    let mut container = json!({
        "name": "task",
        "image": image,
        "env": env,
        "envFrom": opts.env_from_secrets.iter().map(|s| json!({"secretRef": {"name": s}})).collect::<Vec<_>>(),
        // Requests equal to limits: the quota counts what a task may use.
        "resources": {
            "limits": {"cpu": cpu, "memory": memory},
            "requests": {"cpu": cpu, "memory": memory},
        },
        "securityContext": {
            "allowPrivilegeEscalation": false,
            "runAsNonRoot": true,
            "capabilities": {"drop": ["ALL"]},
            "seccompProfile": {"type": "RuntimeDefault"},
        },
    });
    if !spec.command.is_empty() {
        container["command"] = json!(spec.command);
    }

    let mut pod_spec = json!({
        "restartPolicy": "Never",
        "automountServiceAccountToken": !opts.service_account.is_empty(),
        "containers": [container],
    });
    if !opts.service_account.is_empty() {
        pod_spec["serviceAccountName"] = json!(opts.service_account);
    }

    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": format!("{atespace}-{name}-{run}"),
            "namespace": opts.namespace,
            "labels": {
                LABEL_MANAGED: MANAGER,
                LABEL_ATESPACE: atespace,
                LABEL_TASK: name,
            },
        },
        "spec": pod_spec,
    })
}

/// The task's actor as its newest pod shows it.
pub fn pod_actor(pod: &Value) -> Actor {
    let labels = &pod["metadata"]["labels"];
    let name = labels[LABEL_TASK].as_str().unwrap_or_default().to_owned();
    let status = &pod["status"];
    let container = &status["containerStatuses"][0]["state"];
    let (state, crash_message) = match status["phase"].as_str().unwrap_or_default() {
        "Running" => (ActorState::Running, String::new()),
        "Succeeded" => (ActorState::Suspended, String::new()),
        "Failed" => {
            let t = &container["terminated"];
            let msg = match t["exitCode"].as_i64() {
                Some(code) => format!(
                    "The task's pod exited with code {code} ({})",
                    t["reason"].as_str().unwrap_or("Error")
                ),
                None => format!(
                    "The task's pod failed: {}",
                    status["message"].as_str().unwrap_or("no reason given")
                ),
            };
            (ActorState::Crashed, msg)
        }
        "Pending" => {
            let w = &container["waiting"];
            match w["reason"].as_str() {
                Some(r) if STUCK_REASONS.contains(&r) => (
                    ActorState::Crashed,
                    format!(
                        "The task's pod cannot start: {r}: {}",
                        w["message"].as_str().unwrap_or_default()
                    ),
                ),
                _ => (ActorState::Resuming, String::new()),
            }
        }
        _ => (ActorState::Unspecified, String::new()),
    };
    let state = if pod["metadata"]["deletionTimestamp"].is_string() {
        ActorState::Suspending
    } else {
        state
    };
    Actor {
        name,
        state,
        worker_ip: status["podIP"].as_str().unwrap_or_default().to_owned(),
        crash_message,
    }
}

fn is_finished(pod: &Value) -> bool {
    matches!(
        pod["status"]["phase"].as_str(),
        Some("Succeeded" | "Failed")
    )
}

/// Runs and reads tasks as pods.
pub struct PodRunner {
    client: KubeClient,
    opts: PodOptions,
}

impl PodRunner {
    pub fn new(client: KubeClient, opts: PodOptions) -> Self {
        Self { client, opts }
    }

    fn pods_path(&self) -> String {
        format!("/api/v1/namespaces/{}/pods", self.opts.namespace)
    }

    /// The task's pods (or the atespace's, without `task`), newest first.
    async fn pods(&self, atespace: &str, task: Option<&str>) -> Result<Vec<Value>, KubeError> {
        let path = format!(
            "{}?labelSelector={}",
            self.pods_path(),
            selector(atespace, task)
        );
        let list = self.client.request(Method::GET, &path, None).await?;
        let mut items = list["items"].as_array().cloned().unwrap_or_default();
        // RFC 3339 times in UTC sort as strings.
        items.sort_by(|a, b| {
            let t = |p: &Value| {
                p["metadata"]["creationTimestamp"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned()
            };
            t(b).cmp(&t(a))
        });
        Ok(items)
    }

    async fn delete_pod(&self, name: &str) -> Result<(), KubeError> {
        let path = format!("{}/{name}", self.pods_path());
        match self.client.request(Method::DELETE, &path, None).await {
            Err(e) if !e.is_not_found() => Err(e),
            _ => Ok(()),
        }
    }

    async fn delete_pods(&self, pods: &[Value]) -> Result<(), KubeError> {
        for p in pods {
            self.delete_pod(p["metadata"]["name"].as_str().unwrap_or_default())
                .await?;
        }
        Ok(())
    }
}

fn set_condition(st: &mut TaskStatus, kind: &str, status: &str, reason: &str, message: String) {
    let cond = Condition {
        r#type: kind.into(),
        status: status.into(),
        update_time: Some(now()),
        reason: reason.into(),
        message,
    };
    match st.conditions.iter_mut().find(|c| c.r#type == kind) {
        Some(c) => *c = cond,
        None => st.conditions.push(cond),
    }
}

/// Records a failed call on the task as a False Ready condition:
/// SubstrateUnavailable when retrying may work, `failed_reason` otherwise.
fn fail(task: &mut Task, err: &KubeError, context: &str, failed_reason: &str) -> Status {
    let st = task.status.get_or_insert_with(Default::default);
    let reason = if err.is_transient() {
        reason::SUBSTRATE_UNAVAILABLE
    } else {
        failed_reason
    };
    set_condition(
        st,
        condition::READY,
        "False",
        reason,
        format!("{context}: {err}"),
    );
    err.to_status(context)
}

impl PodRunner {
    /// Fills in the task's defaults and ID, checks its limits, and lists the
    /// pods it has run in.
    async fn prepare(&self, task: &mut Task, failed_reason: &str) -> Result<Vec<Value>, Status> {
        let atespace = atespace_of(task);
        task.metadata.get_or_insert_with(Default::default).atespace = atespace.clone();
        let name = task.metadata.as_ref().unwrap().name.clone();
        let spec = task.spec.get_or_insert_with(TaskSpec::default);
        if spec.image.is_empty() {
            spec.image = self.opts.default_task_image.clone();
        }
        let resources = spec.resources.clone();
        let st = task.status.get_or_insert_with(Default::default);
        if st.id.is_empty() {
            let secs = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_default();
            st.id = format!("task-{name}-{secs}");
        }
        if let Err(e) = validate_resources(resources.as_ref()) {
            set_condition(
                st,
                condition::READY,
                "False",
                reason::INVALID_RESOURCES,
                e.to_string(),
            );
            return Err(Status::invalid_argument(format!(
                "validating resources: {e}"
            )));
        }
        self.pods(&atespace, Some(&name))
            .await
            .map_err(|e| fail(task, &e, "listing the task's pods", failed_reason))
    }
}

#[async_trait]
impl Reconciler for PodRunner {
    /// Stops a run in progress and keeps finished pods for their logs.
    async fn suspend(&self, task: &mut Task, _workspace: Option<Workspace>) -> Result<(), Status> {
        let pods = self.prepare(task, reason::ACTOR_CREATION_FAILED).await?;
        let running: Vec<Value> = pods.into_iter().filter(|p| !is_finished(p)).collect();
        if !running.is_empty() {
            info!(
                atespace = atespace_of(task),
                task = task.metadata.as_ref().map(|m| m.name.as_str()),
                "deleting the task's running pod"
            );
        }
        self.delete_pods(&running)
            .await
            .map_err(|e| e.to_status("deleting the task's pod"))
    }

    /// Keeps a run in progress; otherwise a new pod replaces the finished
    /// ones.
    async fn resume(&self, task: &mut Task, workspace: Option<Workspace>) -> Result<(), Status> {
        let pods = self.prepare(task, reason::ACTOR_RESUME_FAILED).await?;
        if pods.first().is_none_or(is_finished) {
            if let Err(e) = self.delete_pods(&pods).await {
                return Err(fail(
                    task,
                    &e,
                    "deleting the task's old pods",
                    reason::ACTOR_RESUME_FAILED,
                ));
            }
            // Milliseconds, so two runs never share a pod name.
            let run = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or_default();
            let pod = pod_manifest(task, workspace.as_ref(), &self.opts, run);
            info!(
                atespace = atespace_of(task),
                task = task.metadata.as_ref().map(|m| m.name.as_str()),
                pod = pod["metadata"]["name"].as_str(),
                "creating the task's pod"
            );
            if let Err(e) = self
                .client
                .request(Method::POST, &self.pods_path(), Some(&pod))
                .await
            {
                return Err(fail(
                    task,
                    &e,
                    "creating the task's pod",
                    reason::ACTOR_RESUME_FAILED,
                ));
            }
        }
        set_condition(
            task.status.get_or_insert_with(Default::default),
            condition::WORKSPACE_READY,
            "True",
            reason::SETUP_COMPLETE,
            "The pod runner sets up no workspace".into(),
        );
        Ok(())
    }

    async fn delete(&self, atespace: &str, name: &str) -> Result<(), Status> {
        let atespace = if atespace.is_empty() {
            "default"
        } else {
            atespace
        };
        info!(atespace, task = name, "deleting the task's pods");
        let pods = self
            .pods(atespace, Some(name))
            .await
            .map_err(|e| e.to_status("listing the task's pods"))?;
        self.delete_pods(&pods)
            .await
            .map_err(|e| e.to_status("deleting the task's pods"))
    }
}

#[async_trait]
impl Actors for PodRunner {
    async fn get_actor(&self, atespace: &str, name: &str) -> Result<Option<Actor>, Status> {
        let atespace = if atespace.is_empty() {
            "default"
        } else {
            atespace
        };
        let pods = self
            .pods(atespace, Some(name))
            .await
            .map_err(|e| e.to_status("listing the task's pods"))?;
        Ok(pods
            .first()
            .map(pod_actor)
            .or_else(|| self.absent_actor(name)))
    }

    async fn list_actors(&self, atespace: &str) -> Result<Vec<Actor>, Status> {
        let atespace = if atespace.is_empty() {
            "default"
        } else {
            atespace
        };
        let pods = self
            .pods(atespace, None)
            .await
            .map_err(|e| e.to_status("listing the atespace's pods"))?;
        // Newest first, so the first pod seen for a task is its actor.
        let mut seen = std::collections::HashSet::new();
        Ok(pods
            .iter()
            .map(pod_actor)
            .filter(|a| seen.insert(a.name.clone()))
            .collect())
    }

    /// A task with no pod has not run, or its pods were cleaned up: it is
    /// suspended, not lost.
    fn absent_actor(&self, name: &str) -> Option<Actor> {
        Some(Actor {
            name: name.to_owned(),
            state: ActorState::Suspended,
            worker_ip: String::new(),
            crash_message: String::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use axum::extract::{Path, Query, State};
    use axum::routing::{delete, get};
    use axum::{Json, Router};
    use rax_api::v1alpha1::{EnvVar, ObjectMeta, ResourceList, ResourcesSpec};

    use super::*;
    use rax_api::phase;

    /// A fake Kubernetes API that keeps pods in memory: list by label,
    /// create (as Pending, with increasing creation times) and delete.
    #[derive(Clone, Default)]
    struct FakeApi {
        pods: Arc<Mutex<Vec<Value>>>,
        created: Arc<Mutex<u32>>,
    }

    impl FakeApi {
        fn set_phase(&self, name: &str, phase: &str, extra: Value) {
            let mut pods = self.pods.lock().unwrap();
            let p = pods
                .iter_mut()
                .find(|p| p["metadata"]["name"] == name)
                .expect("pod exists");
            p["status"] = json!({"phase": phase, "podIP": "10.42.0.9"});
            if let Value::Object(m) = extra {
                for (k, v) in m {
                    p["status"][k] = v;
                }
            }
        }

        fn names(&self) -> Vec<String> {
            self.pods
                .lock()
                .unwrap()
                .iter()
                .map(|p| p["metadata"]["name"].as_str().unwrap().to_owned())
                .collect()
        }
    }

    fn matches(pod: &Value, selector: &str) -> bool {
        selector.split(',').all(|kv| {
            let (k, v) = kv.split_once('=').unwrap();
            pod["metadata"]["labels"][k] == v
        })
    }

    async fn serve(api: FakeApi) -> KubeClient {
        let app = Router::new()
            .route(
                "/api/v1/namespaces/{ns}/pods",
                get(
                    |State(api): State<FakeApi>, Query(q): Query<HashMap<String, String>>| async move {
                        let sel = q.get("labelSelector").cloned().unwrap_or_default();
                        let items: Vec<Value> = api
                            .pods
                            .lock()
                            .unwrap()
                            .iter()
                            .filter(|p| matches(p, &sel))
                            .cloned()
                            .collect();
                        Json(json!({"items": items}))
                    },
                )
                .post(|State(api): State<FakeApi>, Json(mut pod): Json<Value>| async move {
                    let n = {
                        let mut c = api.created.lock().unwrap();
                        *c += 1;
                        *c
                    };
                    pod["metadata"]["creationTimestamp"] = json!(format!("2026-10-31T00:00:{n:02}Z"));
                    pod["status"] = json!({"phase": "Pending"});
                    api.pods.lock().unwrap().push(pod.clone());
                    (StatusCode::CREATED, Json(pod))
                }),
            )
            .route(
                "/api/v1/namespaces/{ns}/pods/{name}",
                delete(
                    |State(api): State<FakeApi>, Path((_ns, name)): Path<(String, String)>| async move {
                        let mut pods = api.pods.lock().unwrap();
                        let before = pods.len();
                        pods.retain(|p| p["metadata"]["name"] != name.as_str());
                        if pods.len() == before {
                            (StatusCode::NOT_FOUND, Json(json!({"message": "not found"})))
                        } else {
                            (StatusCode::OK, Json(json!({})))
                        }
                    },
                ),
            )
            .with_state(api);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        KubeClient::plaintext("127.0.0.1", port)
    }

    fn opts() -> PodOptions {
        PodOptions {
            env_from_secrets: vec!["agent-secrets".into()],
            ..Default::default()
        }
    }

    fn task(name: &str, phase: &str) -> Task {
        Task {
            metadata: Some(ObjectMeta {
                name: name.into(),
                atespace: "ops".into(),
                ..Default::default()
            }),
            spec: Some(TaskSpec {
                image: "ghcr.io/piabrota/agents:1".into(),
                command: vec!["agent".into(), "health-digest".into()],
                env: vec![EnvVar {
                    name: "MODEL".into(),
                    value: "haiku".into(),
                }],
                ..Default::default()
            }),
            status: Some(TaskStatus {
                phase: phase.into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn env<'a>(pod: &'a Value, name: &str) -> Option<&'a str> {
        pod["spec"]["containers"][0]["env"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["name"] == name)
            .and_then(|e| e["value"].as_str())
    }

    #[test]
    fn the_pod_carries_the_task() {
        let pod = pod_manifest(&task("digest", phase::RUNNING), None, &opts(), 42);
        assert_eq!(pod["metadata"]["name"], "ops-digest-42");
        assert_eq!(pod["metadata"]["namespace"], "ax-tasks");
        assert_eq!(pod["metadata"]["labels"][LABEL_TASK], "digest");
        assert_eq!(pod["metadata"]["labels"][LABEL_ATESPACE], "ops");
        let c = &pod["spec"]["containers"][0];
        assert_eq!(c["image"], "ghcr.io/piabrota/agents:1");
        assert_eq!(c["command"], json!(["agent", "health-digest"]));
        assert_eq!(env(&pod, "MODEL"), Some("haiku"));
        assert_eq!(env(&pod, "AX_TASK_NAME"), Some("digest"));
        assert!(env(&pod, "AX_TASK_YAML").unwrap().contains("health-digest"));
        assert_eq!(
            c["envFrom"],
            json!([{"secretRef": {"name": "agent-secrets"}}])
        );
        assert_eq!(
            c["resources"]["limits"],
            json!({"cpu": "500m", "memory": "256Mi"})
        );
        assert_eq!(c["resources"]["requests"], c["resources"]["limits"]);
        assert_eq!(c["securityContext"]["runAsNonRoot"], true);
        assert_eq!(pod["spec"]["restartPolicy"], "Never");
        assert_eq!(pod["spec"]["automountServiceAccountToken"], false);
        assert!(pod["spec"]["serviceAccountName"].is_null());
    }

    #[test]
    fn the_task_limits_and_image_win_over_the_defaults() {
        let mut t = task("digest", phase::RUNNING);
        let spec = t.spec.as_mut().unwrap();
        spec.image.clear();
        spec.command.clear();
        spec.resources = Some(ResourcesSpec {
            limits: Some(ResourceList {
                cpu: String::new(),
                memory: "768Mi".into(),
            }),
            ..Default::default()
        });
        let o = PodOptions {
            service_account: "agents".into(),
            ..opts()
        };
        let pod = pod_manifest(&t, None, &o, 1);
        let c = &pod["spec"]["containers"][0];
        assert_eq!(c["image"], DEFAULT_TASK_IMAGE);
        assert!(c["command"].is_null());
        assert_eq!(
            c["resources"]["limits"],
            json!({"cpu": "500m", "memory": "768Mi"})
        );
        assert_eq!(pod["spec"]["serviceAccountName"], "agents");
        assert_eq!(pod["spec"]["automountServiceAccountToken"], true);
    }

    fn pod(phase: &str, container: Value) -> Value {
        json!({
            "metadata": {"labels": {LABEL_TASK: "t"}},
            "status": {"phase": phase, "podIP": "10.42.0.9", "containerStatuses": [{"state": container}]},
        })
    }

    #[test]
    fn pod_phases_become_actor_states() {
        let a = pod_actor(&pod("Running", json!({})));
        assert_eq!(
            (a.name.as_str(), a.state, a.worker_ip.as_str()),
            ("t", ActorState::Running, "10.42.0.9")
        );
        assert_eq!(
            pod_actor(&pod("Pending", json!({}))).state,
            ActorState::Resuming
        );
        assert_eq!(
            pod_actor(&pod("Succeeded", json!({}))).state,
            ActorState::Suspended
        );

        let failed = pod_actor(&pod(
            "Failed",
            json!({"terminated": {"exitCode": 3, "reason": "Error"}}),
        ));
        assert_eq!(failed.state, ActorState::Crashed);
        assert_eq!(
            failed.crash_message,
            "The task's pod exited with code 3 (Error)"
        );

        let stuck = pod_actor(&pod(
            "Pending",
            json!({"waiting": {"reason": "ImagePullBackOff", "message": "pull access denied"}}),
        ));
        assert_eq!(stuck.state, ActorState::Crashed);
        assert!(
            stuck
                .crash_message
                .contains("ImagePullBackOff: pull access denied")
        );

        let mut deleting = pod("Running", json!({}));
        deleting["metadata"]["deletionTimestamp"] = json!("2026-10-31T00:00:00Z");
        assert_eq!(pod_actor(&deleting).state, ActorState::Suspending);
    }

    #[test]
    fn which_errors_are_worth_retrying() {
        let e = |s: Option<u16>, m: &str| KubeError {
            status: s.map(|s| StatusCode::from_u16(s).unwrap()),
            message: m.into(),
        };
        assert!(e(None, "connection refused").is_transient());
        assert!(e(Some(503), "").is_transient());
        assert!(e(Some(409), "already exists").is_transient());
        assert!(
            e(
                Some(403),
                "pods \"x\" is forbidden: exceeded quota: ax-tasks"
            )
            .is_transient()
        );
        assert!(!e(Some(403), "forbidden: User cannot create pods").is_transient());
        assert!(!e(Some(422), "invalid").is_transient());
        assert!(e(Some(404), "").is_not_found());
    }

    #[tokio::test]
    async fn a_task_runs_once_per_resume() {
        let api = FakeApi::default();
        let runner = PodRunner::new(serve(api.clone()).await, opts());

        // Created: suspended, no pod.
        runner.suspend(&mut task("digest", ""), None).await.unwrap();
        assert!(api.names().is_empty());
        let a = runner.get_actor("ops", "digest").await.unwrap().unwrap();
        assert_eq!(a.state, ActorState::Suspended);

        // Resumed: one pod; resuming again while it runs keeps it.
        let mut t = task("digest", "");
        runner.resume(&mut t, None).await.unwrap();
        let st = t.status.as_ref().unwrap();
        assert_eq!(st.phase, "", "the phase is the server's to set");
        assert!(
            st.conditions
                .iter()
                .any(|c| c.r#type == condition::WORKSPACE_READY && c.status == "True")
        );
        let first = api.names();
        assert_eq!(first.len(), 1);
        assert!(first[0].starts_with("ops-digest-"), "{first:?}");
        runner.resume(&mut task("digest", ""), None).await.unwrap();
        assert_eq!(api.names(), first);

        // It ends: the task reads as suspended, and the pod stays for its logs.
        api.set_phase(&first[0], "Succeeded", json!({}));
        let a = runner.get_actor("ops", "digest").await.unwrap().unwrap();
        assert_eq!(a.state, ActorState::Suspended);
        runner.suspend(&mut task("digest", ""), None).await.unwrap();
        assert_eq!(api.names(), first);

        // Resumed again: the old pod goes, a new one runs.
        runner.resume(&mut task("digest", ""), None).await.unwrap();
        let second = api.names();
        assert_eq!(second.len(), 1);
        assert_ne!(second, first);

        // Suspended while running: the pod is deleted.
        runner.suspend(&mut task("digest", ""), None).await.unwrap();
        assert!(api.names().is_empty());
    }

    #[tokio::test]
    async fn listing_and_deleting_follow_the_labels() {
        let api = FakeApi::default();
        let runner = PodRunner::new(serve(api.clone()).await, opts());
        runner.resume(&mut task("a", ""), None).await.unwrap();
        runner.resume(&mut task("b", ""), None).await.unwrap();
        let names = api.names();
        api.set_phase(&names[0], "Failed", json!({"containerStatuses": [{"state": {"terminated": {"exitCode": 1, "reason": "Error"}}}]}));

        let mut actors = runner.list_actors("ops").await.unwrap();
        actors.sort_by(|x, y| x.name.cmp(&y.name));
        assert_eq!(actors.len(), 2);
        assert_eq!(
            (actors[0].name.as_str(), actors[0].state),
            ("a", ActorState::Crashed)
        );
        assert_eq!(
            (actors[1].name.as_str(), actors[1].state),
            ("b", ActorState::Resuming)
        );
        assert!(runner.list_actors("other").await.unwrap().is_empty());

        runner.delete("ops", "a").await.unwrap();
        assert_eq!(api.names(), vec![names[1].clone()]);
    }

    #[tokio::test]
    async fn an_unreachable_api_is_worth_retrying() {
        // Nothing listens on port 1.
        let runner = PodRunner::new(KubeClient::plaintext("127.0.0.1", 1), opts());
        let mut t = task("digest", "");
        let err = runner.resume(&mut t, None).await.unwrap_err();
        // The server records Pending for an Unavailable error.
        assert_eq!(err.code(), tonic::Code::Unavailable);
        let st = t.status.unwrap();
        assert!(
            st.conditions
                .iter()
                .any(|c| c.r#type == condition::READY && c.reason == reason::SUBSTRATE_UNAVAILABLE)
        );
    }
}
