//! An in-memory Substrate control server for tests.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Endpoint, Server};
use tonic::{Request, Response, Status};

use crate::ateapi::control_server::{Control, ControlServer};
use crate::ateapi::*;
use crate::client::TokenInterceptor;
use crate::{Client, RetryPolicy};

/// The worker IP actors get when they resume, unless a test sets another.
pub const DEFAULT_WORKER_IP: &str = "10.244.1.42";

/// Runs the workload behind actors, for tests that want real task
/// containers (for example task-runner processes) instead of bare records.
pub trait Lifecycle: Send + Sync + std::fmt::Debug {
    /// Starts the actor's workload from its template and returns the worker
    /// IP to report, such as `127.0.0.1:41234`.
    fn resume(&self, actor: &Actor, template: Option<&ActorTemplate>) -> String;
    /// Stops the actor's workload, on suspend, revert and delete.
    fn stop(&self, atespace: &str, name: &str);
}

/// The fake's state. Tests read and change it between calls.
#[derive(Debug)]
pub struct State {
    /// Runs workloads on resume and stops them on suspend or delete. Without
    /// it, actors are only records and get [`State::worker_ip`].
    pub lifecycle: Option<Arc<dyn Lifecycle>>,
    pub atespaces: BTreeSet<String>,
    /// Templates by "atespace/name".
    pub templates: BTreeMap<String, ActorTemplate>,
    /// Every template created, in order.
    pub created_templates: Vec<ActorTemplate>,
    /// Actors by "atespace/name".
    pub actors: BTreeMap<String, Actor>,
    /// Egress policies by their actor's "atespace/name".
    pub egress_policies: BTreeMap<String, EgressPolicy>,
    /// Snapshot tags by "atespace/name".
    pub tags: BTreeMap<String, Tag>,
    /// Every call, as "Method atespace/name".
    pub calls: Vec<String>,
    pub worker_ip: String,
    /// How many GetActor or CreateActor calls a deleted actor stays visible
    /// as DELETING.
    /// `usize::MAX` means it never goes away.
    pub deleting_polls: usize,
    /// Actors being deleted, with the number of polls left.
    pub deleting: HashMap<String, usize>,
    /// Errors to return, per method name, before doing the call.
    pub errors: HashMap<&'static str, VecDeque<Status>>,
    /// The page size for ListActors; 0 means everything at once.
    pub list_page_size: usize,
    /// The authorization header on the latest call.
    pub last_authorization: Option<String>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            lifecycle: None,
            atespaces: BTreeSet::new(),
            templates: BTreeMap::new(),
            created_templates: Vec::new(),
            actors: BTreeMap::new(),
            egress_policies: BTreeMap::new(),
            tags: BTreeMap::new(),
            calls: Vec::new(),
            worker_ip: DEFAULT_WORKER_IP.to_owned(),
            deleting_polls: 0,
            deleting: HashMap::new(),
            errors: HashMap::new(),
            list_page_size: 0,
            last_authorization: None,
        }
    }
}

impl State {
    /// Queues errors for the next calls of `method`, such as "CreateActor".
    pub fn fail(&mut self, method: &'static str, errors: impl IntoIterator<Item = Status>) {
        self.errors.entry(method).or_default().extend(errors);
    }

    /// The calls of one method, as "atespace/name".
    pub fn calls_of(&self, method: &str) -> Vec<String> {
        let prefix = format!("{method} ");
        self.calls
            .iter()
            .filter_map(|c| c.strip_prefix(&prefix).map(str::to_owned))
            .collect()
    }

    /// Adds an actor in the given state, as if it had been created earlier.
    pub fn put_actor(&mut self, atespace: &str, name: &str, state: ActorState) {
        self.actors
            .insert(key(atespace, name), actor(atespace, name, state, None));
    }

    /// Counts a look at an actor being deleted, removing it once no looks
    /// are left.
    fn advance_deletion(&mut self, k: &str) {
        match self.deleting.get(k).copied() {
            Some(0) => {
                self.deleting.remove(k);
                self.actors.remove(k);
            }
            Some(usize::MAX) | None => {}
            Some(left) => {
                self.deleting.insert(k.to_owned(), left - 1);
            }
        }
    }

    pub fn actor_state(&self, atespace: &str, name: &str) -> Option<ActorState> {
        self.actors
            .get(&key(atespace, name))
            .and_then(|a| a.status.as_ref())
            .and_then(|s| ActorState::try_from(s.state).ok())
    }
}

fn key(atespace: &str, name: &str) -> String {
    format!("{atespace}/{name}")
}

fn ref_key(r: &Option<ObjectRef>) -> String {
    r.as_ref()
        .map(|r| key(&r.atespace, &r.name))
        .unwrap_or_default()
}

fn meta_key(m: &Option<ResourceMetadata>) -> String {
    m.as_ref()
        .map(|m| key(&m.atespace, &m.name))
        .unwrap_or_default()
}

fn actor(atespace: &str, name: &str, state: ActorState, worker_ip: Option<&str>) -> Actor {
    Actor {
        metadata: Some(ResourceMetadata {
            atespace: atespace.to_owned(),
            name: name.to_owned(),
            ..Default::default()
        }),
        status: Some(ActorStatus {
            state: state as i32,
            worker_assignment: worker_ip.map(|ip| WorkerAssignment {
                worker_pod: "worker-pod-1".to_owned(),
                worker_pod_ips: vec![ip.to_owned()],
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn set_state(a: &mut Actor, state: ActorState, worker_ip: Option<&str>) {
    let meta = a.metadata.take().unwrap_or_default();
    let template = a.actor_template.take();
    let source_tag = a.source_tag.take();
    *a = actor(&meta.atespace, &meta.name, state, worker_ip);
    a.metadata = Some(meta);
    a.actor_template = template;
    a.source_tag = source_tag;
}

fn not_found(what: &str, k: &str) -> Status {
    Status::not_found(format!("{what} {k:?} not found"))
}

/// A fake Substrate control server holding its state in memory.
#[derive(Clone, Default)]
pub struct FakeSubstrate {
    pub state: Arc<Mutex<State>>,
}

impl FakeSubstrate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Serves the fake on a local port and returns a client for it, with
    /// fast retries.
    pub async fn start(&self) -> Client {
        self.start_with_auth(TokenInterceptor::default()).await
    }

    pub async fn start_with_auth(&self, auth: TokenInterceptor) -> Client {
        let addr = self.serve().await;
        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .expect("endpoint")
            .connect_lazy();
        Client::from_channel(channel, auth).with_retry_policy(RetryPolicy::fast())
    }

    /// Serves the fake on a local port and returns its address.
    pub async fn serve(&self) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let svc = ControlServer::new(self.clone());
        tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("fake substrate");
        });
        addr
    }

    pub fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    /// Records the call and returns the next queued error for it, if any.
    fn begin<T>(
        &self,
        method: &'static str,
        k: &str,
        req: &Request<T>,
    ) -> Result<std::sync::MutexGuard<'_, State>, Status> {
        let mut s = self.state();
        s.calls.push(format!("{method} {k}"));
        s.last_authorization = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        if let Some(err) = s.errors.get_mut(method).and_then(VecDeque::pop_front) {
            return Err(err);
        }
        Ok(s)
    }
}

#[tonic::async_trait]
impl Control for FakeSubstrate {
    async fn create_atespace(
        &self,
        req: Request<CreateAtespaceRequest>,
    ) -> Result<Response<Atespace>, Status> {
        let name = req
            .get_ref()
            .atespace
            .as_ref()
            .and_then(|a| a.metadata.as_ref())
            .map(|m| m.name.clone())
            .unwrap_or_default();
        let mut s = self.begin("CreateAtespace", &name, &req)?;
        if !s.atespaces.insert(name.clone()) {
            return Err(Status::already_exists(format!(
                "atespace {name:?} already exists"
            )));
        }
        Ok(Response::new(req.into_inner().atespace.unwrap_or_default()))
    }

    async fn get_actor_template(
        &self,
        req: Request<GetActorTemplateRequest>,
    ) -> Result<Response<ActorTemplate>, Status> {
        let k = ref_key(&req.get_ref().actor_template);
        let s = self.begin("GetActorTemplate", &k, &req)?;
        s.templates
            .get(&k)
            .cloned()
            .map(Response::new)
            .ok_or_else(|| not_found("template", &k))
    }

    async fn create_actor_template(
        &self,
        req: Request<CreateActorTemplateRequest>,
    ) -> Result<Response<ActorTemplate>, Status> {
        let mut tmpl = req.get_ref().actor_template.clone().unwrap_or_default();
        let k = meta_key(&tmpl.metadata);
        let meta = tmpl.metadata.get_or_insert_with(Default::default);
        if meta.uid.is_empty() {
            meta.uid = format!("uid-{k}");
        }
        let mut s = self.begin("CreateActorTemplate", &k, &req)?;
        if s.templates.contains_key(&k) {
            return Err(Status::already_exists(format!(
                "template {k:?} already exists"
            )));
        }
        s.templates.insert(k, tmpl.clone());
        s.created_templates.push(tmpl.clone());
        Ok(Response::new(tmpl))
    }

    async fn list_actor_templates(
        &self,
        req: Request<ListActorTemplatesRequest>,
    ) -> Result<Response<ListActorTemplatesResponse>, Status> {
        let atespace = req.get_ref().atespace.clone();
        let s = self.begin("ListActorTemplates", &atespace, &req)?;
        let actor_templates = s
            .templates
            .values()
            .filter(|t| t.metadata.as_ref().is_some_and(|m| m.atespace == atespace))
            .cloned()
            .collect();
        Ok(Response::new(ListActorTemplatesResponse {
            actor_templates,
            next_page_token: String::new(),
        }))
    }

    async fn delete_actor_template(
        &self,
        req: Request<DeleteActorTemplateRequest>,
    ) -> Result<Response<ActorTemplate>, Status> {
        let k = ref_key(&req.get_ref().actor_template);
        let mut s = self.begin("DeleteActorTemplate", &k, &req)?;
        s.templates
            .remove(&k)
            .map(Response::new)
            .ok_or_else(|| not_found("template", &k))
    }

    async fn create_actor(
        &self,
        req: Request<CreateActorRequest>,
    ) -> Result<Response<Actor>, Status> {
        let mut a = req.get_ref().actor.clone().unwrap_or_default();
        let k = meta_key(&a.metadata);
        let mut s = self.begin("CreateActor", &k, &req)?;
        s.advance_deletion(&k);
        if s.actors.contains_key(&k) {
            return Err(Status::already_exists(format!(
                "actor {k:?} already exists"
            )));
        }
        if let Some(tag_ref) = &a.source_tag {
            let tag_key = key(&tag_ref.atespace, &tag_ref.name);
            let tag = s
                .tags
                .get(&tag_key)
                .ok_or_else(|| not_found("source tag", &tag_key))?;
            let tag_uid = tag
                .status
                .as_ref()
                .map(|t| t.actor_template_uid.as_str())
                .unwrap_or_default();
            let tpl_uid = s
                .templates
                .get(&ref_key(&a.actor_template))
                .and_then(|t| t.metadata.as_ref())
                .map(|m| m.uid.as_str())
                .unwrap_or_default();
            if !tag_uid.is_empty() && !tpl_uid.is_empty() && tag_uid != tpl_uid {
                return Err(Status::invalid_argument(format!(
                    "source tag template uid {tag_uid:?} does not match actor template uid {tpl_uid:?}"
                )));
            }
        }
        let meta = a.metadata.get_or_insert_with(Default::default);
        if meta.uid.is_empty() {
            meta.uid = format!("uid-{k}");
        }
        a.status = Some(ActorStatus {
            state: ActorState::Suspended as i32,
            ..Default::default()
        });
        s.actors.insert(k, a.clone());
        Ok(Response::new(a))
    }

    async fn get_actor(&self, req: Request<GetActorRequest>) -> Result<Response<Actor>, Status> {
        let k = ref_key(&req.get_ref().actor);
        let mut s = self.begin("GetActor", &k, &req)?;
        s.advance_deletion(&k);
        s.actors
            .get(&k)
            .cloned()
            .map(Response::new)
            .ok_or_else(|| not_found("actor", &k))
    }

    async fn list_actors(
        &self,
        req: Request<ListActorsRequest>,
    ) -> Result<Response<ListActorsResponse>, Status> {
        let atespace = req.get_ref().atespace.clone();
        let token = req
            .get_ref()
            .options
            .as_ref()
            .map(|o| o.page_token.clone())
            .unwrap_or_default();
        let s = self.begin("ListActors", &atespace, &req)?;
        let all: Vec<Actor> = s
            .actors
            .values()
            .filter(|a| {
                atespace.is_empty() || a.metadata.as_ref().is_some_and(|m| m.atespace == atespace)
            })
            .cloned()
            .collect();
        let start: usize = token.parse().unwrap_or(0);
        let size = if s.list_page_size == 0 {
            all.len()
        } else {
            s.list_page_size
        };
        let end = (start + size).min(all.len());
        let next_page_token = if end < all.len() {
            end.to_string()
        } else {
            String::new()
        };
        Ok(Response::new(ListActorsResponse {
            actors: all[start..end].to_vec(),
            next_page_token,
        }))
    }

    async fn resume_actor(
        &self,
        req: Request<ResumeActorRequest>,
    ) -> Result<Response<ResumeActorResponse>, Status> {
        let k = ref_key(&req.get_ref().actor);
        let mut s = self.begin("ResumeActor", &k, &req)?;
        let mut ip = s.worker_ip.clone();
        if let Some(lifecycle) = s.lifecycle.clone() {
            let a = s.actors.get(&k).ok_or_else(|| not_found("actor", &k))?;
            let template = s.templates.get(&ref_key(&a.actor_template));
            ip = lifecycle.resume(a, template);
        }
        let a = s.actors.get_mut(&k).ok_or_else(|| not_found("actor", &k))?;
        set_state(a, ActorState::Running, Some(&ip));
        Ok(Response::new(ResumeActorResponse {
            actor: Some(a.clone()),
            resumed: true,
        }))
    }

    async fn suspend_actor(
        &self,
        req: Request<SuspendActorRequest>,
    ) -> Result<Response<SuspendActorResponse>, Status> {
        let k = ref_key(&req.get_ref().actor);
        let mut s = self.begin("SuspendActor", &k, &req)?;
        if let Some(lifecycle) = s.lifecycle.clone() {
            let (atespace, name) = k.split_once('/').unwrap_or_default();
            lifecycle.stop(atespace, name);
        }
        let a = s.actors.get_mut(&k).ok_or_else(|| not_found("actor", &k))?;
        set_state(a, ActorState::Suspended, None);
        Ok(Response::new(SuspendActorResponse {
            actor: Some(a.clone()),
        }))
    }

    async fn revert_actor(
        &self,
        req: Request<RevertActorRequest>,
    ) -> Result<Response<RevertActorResponse>, Status> {
        let k = ref_key(&req.get_ref().actor);
        let mut s = self.begin("RevertActor", &k, &req)?;
        if let Some(lifecycle) = s.lifecycle.clone() {
            let (atespace, name) = k.split_once('/').unwrap_or_default();
            lifecycle.stop(atespace, name);
        }
        let a = s.actors.get_mut(&k).ok_or_else(|| not_found("actor", &k))?;
        set_state(a, ActorState::Suspended, None);
        Ok(Response::new(RevertActorResponse {
            actor: Some(a.clone()),
        }))
    }

    async fn delete_actor(
        &self,
        req: Request<DeleteActorRequest>,
    ) -> Result<Response<Actor>, Status> {
        let k = ref_key(&req.get_ref().actor);
        let mut s = self.begin("DeleteActor", &k, &req)?;
        if let Some(lifecycle) = s.lifecycle.clone() {
            let (atespace, name) = k.split_once('/').unwrap_or_default();
            lifecycle.stop(atespace, name);
        }
        let polls = s.deleting_polls;
        s.egress_policies.remove(&k);
        let Some(a) = s.actors.get_mut(&k) else {
            return Err(not_found("actor", &k));
        };
        if polls == 0 {
            let a = s.actors.remove(&k).unwrap_or_default();
            return Ok(Response::new(a));
        }
        set_state(a, ActorState::Deleting, None);
        let a = a.clone();
        s.deleting.insert(k, polls);
        Ok(Response::new(a))
    }

    async fn get_actor_egress_policy(
        &self,
        req: Request<GetActorEgressPolicyRequest>,
    ) -> Result<Response<EgressPolicy>, Status> {
        let k = ref_key(&req.get_ref().actor);
        let s = self.begin("GetActorEgressPolicy", &k, &req)?;
        if req.get_ref().name != "default" {
            return Err(Status::invalid_argument(format!(
                "name: Invalid value: {:?}: must be \"default\"",
                req.get_ref().name
            )));
        }
        s.egress_policies
            .get(&k)
            .cloned()
            .map(Response::new)
            .ok_or_else(|| Status::not_found(format!("actor {k} has no egress policy")))
    }

    async fn create_actor_egress_policy(
        &self,
        req: Request<CreateActorEgressPolicyRequest>,
    ) -> Result<Response<EgressPolicy>, Status> {
        let k = ref_key(&req.get_ref().actor);
        let mut s = self.begin("CreateActorEgressPolicy", &k, &req)?;
        if !s.actors.contains_key(&k) {
            return Err(not_found("actor", &k));
        }
        if s.egress_policies.contains_key(&k) {
            return Err(Status::already_exists(format!(
                "actor {k} already has an egress policy"
            )));
        }
        let policy = req.into_inner().egress_policy.unwrap_or_default();
        s.egress_policies.insert(k, policy.clone());
        Ok(Response::new(policy))
    }

    async fn create_tag(&self, req: Request<CreateTagRequest>) -> Result<Response<Tag>, Status> {
        let mut tag = req.get_ref().tag.clone().unwrap_or_default();
        let k = meta_key(&tag.metadata);
        let mut s = self.begin("CreateTag", &k, &req)?;
        if s.tags.contains_key(&k) {
            return Err(Status::already_exists(format!("tag {k} already exists")));
        }
        let src_key = ref_key(&tag.source_actor);
        let src = s
            .actors
            .get(&src_key)
            .ok_or_else(|| not_found("actor", &src_key))?;
        if src.status.as_ref().map(|st| st.state) != Some(ActorState::Suspended as i32) {
            return Err(Status::failed_precondition(format!(
                "source actor {src_key} must be suspended"
            )));
        }
        let actor_template_uid = s
            .templates
            .get(&ref_key(&src.actor_template))
            .and_then(|t| t.metadata.as_ref())
            .map(|m| m.uid.clone())
            .unwrap_or_default();
        tag.status = Some(TagStatus {
            actor_template_uid,
            ..Default::default()
        });
        s.tags.insert(k, tag.clone());
        Ok(Response::new(tag))
    }

    async fn get_tag(&self, req: Request<GetTagRequest>) -> Result<Response<Tag>, Status> {
        let k = ref_key(&req.get_ref().tag);
        let s = self.begin("GetTag", &k, &req)?;
        s.tags
            .get(&k)
            .cloned()
            .map(Response::new)
            .ok_or_else(|| not_found("tag", &k))
    }

    async fn list_tags(
        &self,
        req: Request<ListTagsRequest>,
    ) -> Result<Response<ListTagsResponse>, Status> {
        let atespace = req.get_ref().atespace.clone();
        let s = self.begin("ListTags", &atespace, &req)?;
        let tags = s
            .tags
            .values()
            .filter(|t| {
                atespace.is_empty() || t.metadata.as_ref().is_some_and(|m| m.atespace == atespace)
            })
            .cloned()
            .collect();
        Ok(Response::new(ListTagsResponse {
            tags,
            next_page_token: String::new(),
        }))
    }

    async fn delete_tag(&self, req: Request<DeleteTagRequest>) -> Result<Response<Tag>, Status> {
        let k = ref_key(&req.get_ref().tag);
        let mut s = self.begin("DeleteTag", &k, &req)?;
        s.tags
            .remove(&k)
            .map(Response::new)
            .ok_or_else(|| not_found("tag", &k))
    }
}
