//! Task reconciliation on Agent Substrate: the actor behind each task, the
//! per-task actor template it runs from, and its workspace readiness.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use rax_api::v1alpha1::{Condition, Task, TaskSpec, TaskStatus, Workspace};
use rax_api::{
    DEFAULT_SYSTEM_NAMESPACE, DEFAULT_TASK_IMAGE, condition, reason, to_yaml, validate_resources,
};
use rax_substrate::ateapi;
use rax_substrate::probe::HttpProbe;
use rax_substrate::{Client, Error as SubstrateError, resource_limits, worker_ip};
use sha2::{Digest, Sha256};
use tonic::{Code, Status};
use tracing::{info, warn};

use crate::tasks::now;
use crate::{Actor, ActorState, Actors, Reconciler};

/// How many bytes of the spec digest go into a per-task template name.
const TEMPLATE_DIGEST_BYTES: usize = 4;

/// Settings for [`TaskReconciler`]. Empty strings take their defaults.
#[derive(Debug, Clone)]
pub struct ControllerOptions {
    /// The fallback template, as `name` or `atespace/name`.
    pub default_template: String,
    pub default_template_atespace: String,
    /// The image for tasks that leave `spec.image` empty.
    pub default_task_image: String,
    /// How long to wait for a freshly resumed actor to finish workspace
    /// setup before recording it as still initializing.
    pub workspace_ready_timeout: Duration,
    pub workspace_ready_poll_interval: Duration,
    /// The router to probe readiness through as well, as `host:port` or a
    /// URL, normally from `ATENET_ROUTER_ADDR`. Empty probes workers only.
    pub router_addr: String,
    pub probe: HttpProbe,
    /// How often deleting one of a task's templates is attempted.
    pub template_delete_attempts: u32,
    pub template_delete_interval: Duration,
}

impl Default for ControllerOptions {
    fn default() -> Self {
        Self {
            default_template: String::new(),
            default_template_atespace: String::new(),
            default_task_image: String::new(),
            workspace_ready_timeout: Duration::from_secs(15),
            workspace_ready_poll_interval: Duration::from_millis(500),
            router_addr: String::new(),
            probe: HttpProbe::default(),
            template_delete_attempts: 5,
            template_delete_interval: Duration::from_millis(500),
        }
    }
}

/// Runs tasks as actors on Substrate: provisions, suspends, resumes and
/// deletes them.
pub struct TaskReconciler {
    client: Client,
    opts: ControllerOptions,
}

impl TaskReconciler {
    pub fn new(client: Client, mut opts: ControllerOptions) -> Self {
        if opts.default_template.is_empty() {
            opts.default_template = "default-template".into();
        }
        if opts.default_template_atespace.is_empty() {
            opts.default_template_atespace = DEFAULT_SYSTEM_NAMESPACE.into();
        }
        if opts.default_task_image.is_empty() {
            opts.default_task_image = DEFAULT_TASK_IMAGE.into();
        }
        Self { client, opts }
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

fn condition_true(st: &TaskStatus, kind: &str) -> bool {
    st.conditions
        .iter()
        .find(|c| c.r#type == kind)
        .is_some_and(|c| c.status == "True")
}

/// Derives the per-task template name from the task name and a digest of the
/// image and container environment, so a spec change gets a new template.
pub fn task_template_name(task: &str, image: &str, env: &BTreeMap<String, String>) -> String {
    let mut h = Sha256::new();
    h.update(image.as_bytes());
    for (k, v) in env {
        h.update(format!("{k}={v};").as_bytes());
    }
    let digest = h.finalize();
    format!(
        "{task}-tmpl-{}",
        hex::encode(&digest[..TEMPLATE_DIGEST_BYTES])
    )
}

/// Reports whether `template` is one [`task_template_name`] can produce for
/// the task, for any spec.
pub fn is_task_template(task: &str, template: &str) -> bool {
    template
        .strip_prefix(task)
        .and_then(|rest| rest.strip_prefix("-tmpl-"))
        .is_some_and(|digest| {
            digest.len() == 2 * TEMPLATE_DIGEST_BYTES
                && digest
                    .bytes()
                    .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        })
}

/// Splits a worker address into host and port, defaulting the port to 80,
/// and formats it for a URL.
fn worker_authority(addr: &str) -> String {
    if let Ok(sa) = addr.parse::<std::net::SocketAddr>() {
        return sa.to_string();
    }
    if let Ok(ip) = addr.parse::<std::net::Ipv6Addr>() {
        return format!("[{ip}]:80");
    }
    match addr.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') && port.parse::<u16>().is_ok() => addr.into(),
        _ => format!("{addr}:80"),
    }
}

fn router_url(addr: &str) -> String {
    let base = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_owned()
    } else if addr.ends_with(":443") {
        format!("https://{addr}")
    } else {
        format!("http://{addr}")
    };
    format!("{}/readyz?check=workspace", base.trim_end_matches('/'))
}

/// The task's atespace and name, which is also its actor's name.
fn atespace_and_name(task: &Task) -> (String, String) {
    let meta = task.metadata.as_ref();
    (
        meta.map(|m| m.atespace.clone()).unwrap_or_default(),
        meta.map(|m| m.name.clone()).unwrap_or_default(),
    )
}

/// Records a failure on the task as a False Ready condition: reason
/// SubstrateUnavailable when Substrate may come back, `failed_reason`
/// otherwise. The phase is the caller's to set.
fn fail(
    task: &mut Task,
    err: &SubstrateError,
    outer: &str,
    failed_reason: &str,
    message: String,
) -> Status {
    let st = task.status.get_or_insert_with(Default::default);
    if err.is_transient() {
        set_condition(
            st,
            condition::READY,
            "False",
            reason::SUBSTRATE_UNAVAILABLE,
            err.to_string(),
        );
    } else {
        set_condition(st, condition::READY, "False", failed_reason, message);
    }
    err.clone().context(outer).to_status()
}

impl TaskReconciler {
    /// Polls the actor's readiness endpoint, directly and through the router,
    /// until it answers or the timeout passes.
    async fn wait_workspace_ready(&self, atespace: &str, actor: &str, worker: &str) -> bool {
        let direct = format!("http://{}/readyz?check=workspace", worker_authority(worker));
        let target = format!("{atespace}/{actor}");
        let check = || async {
            if self.opts.probe.get_ok(&direct, &[]).await {
                return true;
            }
            if self.opts.router_addr.is_empty() {
                return false;
            }
            let url = router_url(&self.opts.router_addr);
            self.opts
                .probe
                .get_ok(&url, &[("ate-target-actor", target.as_str())])
                .await
        };
        let poll = async {
            loop {
                if check().await {
                    return;
                }
                tokio::time::sleep(self.opts.workspace_ready_poll_interval).await;
            }
        };
        tokio::time::timeout(self.opts.workspace_ready_timeout, poll)
            .await
            .is_ok()
    }

    async fn delete_task_templates(
        &self,
        atespace: &str,
        task: &str,
    ) -> Result<(), SubstrateError> {
        let templates = self.client.list_actor_templates(atespace).await?;
        let mut first_err = None;
        for name in templates
            .iter()
            .filter_map(|t| t.metadata.as_ref())
            .map(|m| m.name.as_str())
            .filter(|n| is_task_template(task, n))
        {
            info!(
                atespace,
                task,
                template = name,
                "deleting Substrate actor template for task"
            );
            let mut result = Ok(());
            for attempt in 0..self.opts.template_delete_attempts.max(1) {
                if attempt > 0 {
                    tokio::time::sleep(self.opts.template_delete_interval).await;
                }
                result = self.client.delete_actor_template(atespace, name).await;
                if result.is_ok() {
                    break;
                }
            }
            if let Err(e) = result {
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }
}

impl TaskReconciler {
    /// Fills in the task's defaults and makes sure its atespace,
    /// ActorTemplate and actor exist on Substrate. It records only what AX
    /// stores in the task's status: the task ID, and on failure the reason in
    /// a False Ready condition.
    async fn ensure_substrate_resources(
        &self,
        task: &mut Task,
        workspace: Option<Workspace>,
    ) -> Result<(), Status> {
        let meta = task.metadata.get_or_insert_with(Default::default);
        if meta.atespace.is_empty() {
            meta.atespace = "default".into();
        }
        let (atespace, name) = (meta.atespace.clone(), meta.name.clone());
        let spec = task.spec.get_or_insert_with(TaskSpec::default);
        if spec.image.is_empty() {
            spec.image = self.opts.default_task_image.clone();
        }
        let image = spec.image.clone();
        task.status.get_or_insert_with(Default::default);
        info!(
            name,
            atespace, image, "ensuring Substrate resources for task"
        );

        // 1. The atespace.
        if let Err(e) = self.client.ensure_atespace(&atespace).await {
            let msg = e.to_string();
            return Err(fail(
                task,
                &e,
                "ensuring atespace",
                reason::ATESPACE_CREATION_FAILED,
                msg,
            ));
        }

        // 2. The actor is always named after the task, so the two can be
        // used interchangeably (for example in the router's
        // ate-target-actor header).
        let actor = name.clone();
        {
            let st = task.status.as_mut().unwrap();
            if st.id.is_empty() {
                let secs = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or_default();
                st.id = format!("task-{name}-{secs}");
            }
        }

        // 3. The template: the task's environment, the task itself without
        // its status (status changes must not make new templates), and the
        // bound workspace.
        let spec = task.spec.as_ref().unwrap();
        let mut env: BTreeMap<String, String> = spec
            .env
            .iter()
            .filter(|e| !e.name.is_empty())
            .map(|e| (e.name.clone(), e.value.clone()))
            .collect();
        let launch = Task {
            status: None,
            ..task.clone()
        };
        if let Ok(y) = to_yaml(&launch) {
            env.insert("AX_TASK_YAML".into(), y);
        }
        if let Some(ws) = &workspace
            && let Ok(y) = to_yaml(ws)
        {
            env.insert("AX_WORKSPACE_YAML".into(), y);
        }

        // Limits are enforced by the template, so they are checked here as
        // well as on apply: a task must never run without limits it asked for.
        if let Err(e) = validate_resources(spec.resources.as_ref()) {
            let st = task.status.as_mut().unwrap();
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
        let resources = resource_limits(spec.resources.as_ref());

        let template = task_template_name(&name, &image, &env);
        info!(image, template, "ensuring ActorTemplate for task");
        // spec.resources is in AX_TASK_YAML, so a limits change is part of
        // the digest and makes a new template.
        let tmpl = match self
            .client
            .ensure_actor_template(&atespace, &template, &image, resources.as_ref(), &env)
            .await
        {
            Ok(t) => t,
            // Falling back to the default template would run another image,
            // without the limits asked for, so the real error goes on the
            // task: just what Substrate said, such as why it rejected the
            // image.
            Err(e) => {
                let msg = e.message().to_owned();
                return Err(fail(
                    task,
                    &e,
                    "ensuring actor template",
                    reason::TEMPLATE_CREATION_FAILED,
                    msg,
                ));
            }
        };
        let (template_atespace, template) = match tmpl.metadata {
            Some(m) => (m.atespace, m.name),
            None => match self.opts.default_template.split_once('/') {
                Some((a, n)) => (a.to_owned(), n.to_owned()),
                None => (
                    self.opts.default_template_atespace.clone(),
                    self.opts.default_template.clone(),
                ),
            },
        };

        if let Err(e) = self
            .client
            .ensure_actor(&atespace, &actor, &template_atespace, &template)
            .await
        {
            let msg = e.to_string();
            return Err(fail(
                task,
                &e,
                "ensuring actor",
                reason::ACTOR_CREATION_FAILED,
                msg,
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl Reconciler for TaskReconciler {
    async fn suspend(&self, task: &mut Task, workspace: Option<Workspace>) -> Result<(), Status> {
        self.ensure_substrate_resources(task, workspace).await?;
        let (atespace, actor) = atespace_and_name(task);
        info!(actor, "suspending actor on Substrate");
        self.client
            .suspend_actor(&atespace, &actor)
            .await
            .map_err(|e| e.context("suspending actor").to_status())?;
        info!(actor, "task suspended");
        Ok(())
    }

    async fn resume(&self, task: &mut Task, workspace: Option<Workspace>) -> Result<(), Status> {
        self.ensure_substrate_resources(task, workspace).await?;
        let (atespace, actor) = atespace_and_name(task);
        info!(actor, "resuming actor on Substrate worker");
        let worker = match self.client.resume_actor(&atespace, &actor).await {
            Ok((_, ip)) => ip,
            Err(e) => {
                let msg = e.to_string();
                return Err(fail(
                    task,
                    &e,
                    "resuming actor",
                    reason::ACTOR_RESUME_FAILED,
                    msg,
                ));
            }
        };
        let st = task.status.get_or_insert_with(Default::default);

        // Workspace setup happens once per task: once WorkspaceReady is True
        // it stays so across suspends and resumes, so only poll before that.
        let already_ready = condition_true(st, condition::WORKSPACE_READY);
        let ready = already_ready
            || (!worker.is_empty() && self.wait_workspace_ready(&atespace, &actor, &worker).await);
        let st = task.status.as_mut().unwrap();
        if ready {
            if !already_ready {
                set_condition(
                    st,
                    condition::WORKSPACE_READY,
                    "True",
                    reason::SETUP_COMPLETE,
                    format!("Workspace setup completed at {worker}"),
                );
            }
        } else {
            set_condition(
                st,
                condition::WORKSPACE_READY,
                "False",
                reason::INITIALIZING,
                format!("Workspace is initializing at {worker}"),
            );
        }
        info!(actor, worker, ready, "task resumed");
        Ok(())
    }

    async fn delete(&self, atespace: &str, name: &str) -> Result<(), Status> {
        let atespace = if atespace.is_empty() {
            "default"
        } else {
            atespace
        };
        info!(atespace, task = name, "deleting Substrate actor for task");
        self.client
            .delete_actor(atespace, name)
            .await
            .map_err(|e| e.to_status())?;
        if let Err(e) = self.delete_task_templates(atespace, name).await {
            warn!(task = name, error = %e, "could not clean up actor templates for task");
        }
        Ok(())
    }
}

/// Reads task actors from Substrate.
pub struct SubstrateActors {
    client: Client,
}

impl SubstrateActors {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

fn actor_state(n: i32) -> ActorState {
    use ateapi::ActorState as S;
    match S::try_from(n) {
        Ok(S::Unspecified) => ActorState::Unspecified,
        Ok(S::Resuming) => ActorState::Resuming,
        Ok(S::Running) => ActorState::Running,
        Ok(S::Suspending) => ActorState::Suspending,
        Ok(S::Suspended) => ActorState::Suspended,
        Ok(S::Crashed) => ActorState::Crashed,
        Ok(S::Deleting) => ActorState::Deleting,
        Ok(S::Reverting) => ActorState::Reverting,
        Err(_) => ActorState::Other(n),
    }
}

fn to_actor(a: &ateapi::Actor) -> Actor {
    let status = a.status.as_ref();
    Actor {
        name: a
            .metadata
            .as_ref()
            .map(|m| m.name.clone())
            .unwrap_or_default(),
        state: actor_state(status.map_or(0, |s| s.state)),
        worker_ip: worker_ip(a),
        crash_message: status
            .and_then(|s| s.crash.as_ref())
            .map(|c| c.message.clone())
            .unwrap_or_default(),
    }
}

#[async_trait]
impl Actors for SubstrateActors {
    async fn get_actor(&self, atespace: &str, name: &str) -> Result<Option<Actor>, Status> {
        match self.client.get_actor(atespace, name).await {
            Ok(a) => Ok(Some(to_actor(&a))),
            Err(e) if e.code() == Code::NotFound => Ok(None),
            Err(e) => Err(e.to_status()),
        }
    }

    async fn list_actors(&self, atespace: &str) -> Result<Vec<Actor>, Status> {
        let actors = self
            .client
            .list_actors(atespace)
            .await
            .map_err(|e| e.to_status())?;
        Ok(actors.iter().map(to_actor).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_names_follow_the_spec() {
        let env = BTreeMap::from([("A".to_owned(), "1".to_owned())]);
        let a = task_template_name("t", "img", &env);
        assert!(is_task_template("t", &a), "{a}");
        assert_eq!(a, task_template_name("t", "img", &env));
        assert_ne!(a, task_template_name("t", "img2", &env));
        assert_ne!(a, task_template_name("t", "img", &BTreeMap::new()));
        // sha256("img" + "A=1;")[:4]
        let mut h = Sha256::new();
        h.update(b"imgA=1;");
        assert_eq!(a, format!("t-tmpl-{}", hex::encode(&h.finalize()[..4])));
    }

    #[test]
    fn template_pattern() {
        assert!(is_task_template("t", "t-tmpl-0123abcd"));
        assert!(!is_task_template("t", "t-tmpl-0123ABCD"));
        assert!(!is_task_template("t", "t-tmpl-0123abc"));
        assert!(!is_task_template("t", "t-other-tmpl-0123abcd"));
        assert!(!is_task_template("t", "tt-tmpl-0123abcd"));
        assert!(!is_task_template("t.x", "tax-tmpl-0123abcd"));
    }

    #[test]
    fn worker_and_router_addresses() {
        assert_eq!(worker_authority("10.0.0.1"), "10.0.0.1:80");
        assert_eq!(worker_authority("10.0.0.1:8080"), "10.0.0.1:8080");
        assert_eq!(worker_authority("fd00::1"), "[fd00::1]:80");
        assert_eq!(worker_authority("[fd00::1]:81"), "[fd00::1]:81");
        assert_eq!(worker_authority("worker:81"), "worker:81");
        assert_eq!(
            router_url("router:443"),
            "https://router:443/readyz?check=workspace"
        );
        assert_eq!(
            router_url("router:80"),
            "http://router:80/readyz?check=workspace"
        );
        assert_eq!(router_url("https://r/"), "https://r/readyz?check=workspace");
    }
}
