//! The Substrate control API client.

use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};

use tonic::codegen::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Response, Status};
use tracing::warn;

use crate::ateapi::control_client::ControlClient;
use crate::ateapi::{
    Actor, ActorState, ActorTemplate, Atespace, CreateActorEgressPolicyRequest, CreateActorRequest,
    CreateActorTemplateRequest, CreateAtespaceRequest, CreateTagRequest, DeleteActorRequest,
    DeleteActorTemplateRequest, DeleteTagRequest, EgressPolicy, EgressRule,
    GetActorEgressPolicyRequest, GetActorRequest, GetActorTemplateRequest, GetTagRequest,
    ListActorTemplatesRequest, ListActorsRequest, ListOptions, ListTagsRequest, ObjectRef,
    ResourceMetadata, ResourceRequirements, ResumeActorRequest, RevertActorRequest,
    SuspendActorRequest, Tag, TagScope,
};
use crate::tls::{self, Connector, TlsOptions};
use crate::{Error, Result, RetryPolicy, build_actor_template};

/// Substrate's address inside the cluster.
pub const DEFAULT_TARGET: &str = "api.ate-system.svc.cluster.local:443";
const CLUSTER_AUTHORITY: &str = "api.ate-system.svc";
const TOKEN_FILE: &str = "/var/run/secrets/ateapi/token";
const CA_FILES: [&str; 2] = [
    "/run/servicedns-ca/trust-bundle.pem",
    "/run/servicedns-ca/ca.pem",
];

/// How to reach Substrate. Empty fields take defaults: the in-cluster
/// target, the token file in `SUBSTRATE_TOKEN_FILE` or the mounted service
/// account token, and the CA file in `SUBSTRATE_CA_FILE` or the mounted
/// trust bundle.
#[derive(Debug, Clone, Default)]
pub struct ClientOptions {
    /// `host:port`, optionally prefixed with `dns:///` or a URL scheme.
    pub target: String,
    /// The `:authority` and TLS server name to use instead of the target's.
    pub authority: String,
    pub token_file: String,
    pub ca_file: String,
    /// Skip verifying Substrate's certificate. For development only.
    pub insecure_tls: bool,
    /// Connect without TLS.
    pub plaintext: bool,
}

impl ClientOptions {
    /// Fills in the defaults, looking at the environment and the filesystem.
    fn resolve(mut self) -> Self {
        let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let first_existing = |paths: &[&str]| {
            paths
                .iter()
                .find(|p| Path::new(p).exists())
                .map(|p| p.to_string())
        };
        if self.target.is_empty() {
            self.target = DEFAULT_TARGET.to_owned();
        }
        if self.token_file.is_empty() {
            self.token_file = env("SUBSTRATE_TOKEN_FILE")
                .or_else(|| first_existing(&[TOKEN_FILE]))
                .unwrap_or_default();
        }
        if self.ca_file.is_empty() {
            self.ca_file = env("SUBSTRATE_CA_FILE")
                .or_else(|| first_existing(&CA_FILES))
                .unwrap_or_default();
        }
        if self.authority.is_empty() && self.target.contains(CLUSTER_AUTHORITY) {
            self.authority = CLUSTER_AUTHORITY.to_owned();
        }
        self
    }
}

/// Sends the token in the token file, read again on every call so that a
/// rotated token is picked up.
#[derive(Clone, Default)]
pub struct TokenInterceptor {
    token_file: Option<PathBuf>,
}

impl TokenInterceptor {
    pub fn from_file(path: impl Into<PathBuf>) -> Self {
        Self {
            token_file: Some(path.into()),
        }
    }
}

impl Interceptor for TokenInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let Some(path) = &self.token_file else {
            return Ok(req);
        };
        let token = std::fs::read_to_string(path).map_err(|e| {
            Status::unavailable(format!(
                "reading token file {:?}: {e}",
                path.display().to_string()
            ))
        })?;
        let value = format!("Bearer {}", token.trim())
            .parse()
            .map_err(|_| Status::unauthenticated("the token is not a valid header value"))?;
        req.metadata_mut().insert("authorization", value);
        Ok(req)
    }
}

type Control = ControlClient<InterceptedService<Channel, TokenInterceptor>>;

/// A client for Substrate's control API. Cloning it is cheap and shares the
/// connection.
#[derive(Clone)]
pub struct Client {
    control: Control,
    retry: RetryPolicy,
}

fn objref(atespace: &str, name: &str) -> Option<ObjectRef> {
    Some(ObjectRef {
        atespace: atespace.to_owned(),
        name: name.to_owned(),
    })
}

/// Returns the pod IP of the worker hosting the actor, or "" when it has no
/// worker, for example while it is suspended.
pub fn worker_ip(actor: &Actor) -> String {
    actor
        .status
        .as_ref()
        .and_then(|s| s.worker_assignment.as_ref())
        .and_then(|w| w.worker_pod_ips.first())
        .cloned()
        .unwrap_or_default()
}

fn state(actor: &Actor) -> ActorState {
    actor
        .status
        .as_ref()
        .and_then(|s| ActorState::try_from(s.state).ok())
        .unwrap_or(ActorState::Unspecified)
}

const UNAVAILABLE: &[Code] = &[Code::Unavailable];
const ABORTED_OR_UNAVAILABLE: &[Code] = &[Code::Aborted, Code::Unavailable];

/// The name Substrate requires for an actor's egress policy. An actor has at
/// most one.
const EGRESS_POLICY_NAME: &str = "default";

impl Client {
    /// Sets up a client. It connects on first use, so this fails only on bad
    /// options, such as an unreadable CA file.
    pub fn connect(opts: ClientOptions) -> io::Result<Self> {
        let opts = opts.resolve();
        let target = opts.target.trim_start_matches("dns:///");
        let scheme = if opts.plaintext { "http" } else { "https" };
        let uri = if target.contains("://") {
            target.to_owned()
        } else {
            format!("{scheme}://{target}")
        };
        let bad = |e: tonic::transport::Error| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("{uri}: {e}"))
        };
        let mut endpoint = Endpoint::from_shared(uri.clone()).map_err(bad)?;
        let authority = (!opts.authority.is_empty()).then(|| opts.authority.clone());
        if let Some(authority) = &authority {
            let origin = format!("{scheme}://{authority}").parse().map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("authority {authority:?}: {e}"),
                )
            })?;
            endpoint = endpoint.origin(origin);
        }
        let connector = if opts.plaintext {
            Connector::plaintext()
        } else {
            let config = tls::client_config(&TlsOptions {
                ca_file: (!opts.ca_file.is_empty()).then(|| opts.ca_file.clone().into()),
                identity_file: None,
                insecure_skip_verify: opts.insecure_tls,
                alpn: vec![b"h2".to_vec()],
            })?;
            Connector::tls(config, authority)
        };
        let channel = endpoint.connect_with_connector_lazy(connector);
        let token_file = (!opts.token_file.is_empty()).then(|| PathBuf::from(&opts.token_file));
        Ok(Self::from_channel(channel, TokenInterceptor { token_file }))
    }

    /// Uses an existing channel, as tests do.
    pub fn from_channel(channel: Channel, auth: TokenInterceptor) -> Self {
        Self {
            control: ControlClient::with_interceptor(channel, auth),
            retry: RetryPolicy::default(),
        }
    }

    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Calls `f` with a fresh request built from `req`, retrying `retryable`
    /// codes under the client's policy. Each attempt is a client span named
    /// after Substrate's `method`, under the caller's span.
    async fn call<Req, Resp, F, Fut>(
        &self,
        method: &str,
        retryable: &[Code],
        req: Req,
        f: F,
    ) -> Result<Resp, Status>
    where
        Req: Clone,
        F: Fn(Control, Request<Req>) -> Fut,
        Fut: Future<Output = Result<Response<Resp>, Status>>,
    {
        let name = format!("substrate.ateapi.v1.Control/{method}");
        self.retry
            .run(retryable, || {
                rax_otelenv::rpc::client_call(&name, Request::new(req.clone()), |r| {
                    f(self.control.clone(), r)
                })
            })
            .await
            .map(Response::into_inner)
    }

    /// Creates the atespace unless it exists.
    pub async fn ensure_atespace(&self, atespace: &str) -> Result<()> {
        let req = CreateAtespaceRequest {
            atespace: Some(Atespace {
                metadata: Some(ResourceMetadata {
                    name: atespace.to_owned(),
                    ..Default::default()
                }),
            }),
        };
        match self
            .call("CreateAtespace", UNAVAILABLE, req, |mut c, r| async move {
                c.create_atespace(r).await
            })
            .await
        {
            Err(e) if e.code() != Code::AlreadyExists => {
                Err(Error::new(format!("creating atespace {atespace:?}"), e))
            }
            _ => Ok(()),
        }
    }

    pub async fn get_actor_template(&self, atespace: &str, name: &str) -> Result<ActorTemplate> {
        let req = GetActorTemplateRequest {
            actor_template: objref(atespace, name),
        };
        self.call(
            "GetActorTemplate",
            UNAVAILABLE,
            req,
            |mut c, r| async move { c.get_actor_template(r).await },
        )
        .await
        .map_err(|e| Error::new(format!("getting actor template {atespace}/{name}"), e))
    }

    /// Returns the named template if it exists, and otherwise creates it from
    /// `tmpl`. A concurrent create of the same template is not an error: the
    /// winner's template is returned.
    pub async fn ensure_template(&self, tmpl: ActorTemplate) -> Result<ActorTemplate> {
        let meta = tmpl.metadata.clone().unwrap_or_default();
        let (atespace, name) = (meta.atespace.as_str(), meta.name.as_str());
        match self.get_actor_template(atespace, name).await {
            Ok(existing) => return Ok(existing),
            Err(e) if e.code() != Code::NotFound => return Err(e),
            Err(_) => {}
        }
        let req = CreateActorTemplateRequest {
            actor_template: Some(tmpl),
        };
        match self
            .call(
                "CreateActorTemplate",
                UNAVAILABLE,
                req,
                |mut c, r| async move { c.create_actor_template(r).await },
            )
            .await
        {
            Ok(created) => Ok(created),
            Err(e) if e.code() == Code::AlreadyExists => {
                self.get_actor_template(atespace, name).await
            }
            Err(e) => Err(Error::new(
                format!("creating actor template {atespace}/{name}"),
                e,
            )),
        }
    }

    /// Returns the template if it exists, and otherwise creates it from the
    /// image, resource limits (`None` for the defaults) and environment.
    pub async fn ensure_actor_template(
        &self,
        atespace: &str,
        name: &str,
        image: &str,
        resources: Option<&ResourceRequirements>,
        env: &BTreeMap<String, String>,
    ) -> Result<ActorTemplate> {
        self.ensure_template(build_actor_template(
            atespace,
            name,
            image,
            env,
            &[],
            "",
            resources,
        ))
        .await
    }

    /// Creates the actor from the template. An existing actor is returned as
    /// it is, unless it crashed, in which case it is reverted to its last
    /// snapshot (or, failing that, recreated), or it is being deleted, in
    /// which case creation is retried for a while.
    pub async fn ensure_actor(
        &self,
        atespace: &str,
        name: &str,
        template_atespace: &str,
        template: &str,
    ) -> Result<Actor> {
        self.ensure_actor_from_tag(atespace, name, template_atespace, template, "")
            .await
    }

    /// Like [`ensure_actor`](Self::ensure_actor), seeding a new actor from
    /// the snapshot Tag `source_tag` (a workspace snapshot) when it is not
    /// empty.
    pub async fn ensure_actor_from_tag(
        &self,
        atespace: &str,
        name: &str,
        template_atespace: &str,
        template: &str,
        source_tag: &str,
    ) -> Result<Actor> {
        let req = CreateActorRequest {
            actor: Some(Actor {
                metadata: Some(ResourceMetadata {
                    atespace: atespace.to_owned(),
                    name: name.to_owned(),
                    ..Default::default()
                }),
                actor_template: objref(template_atespace, template),
                source_tag: (!source_tag.is_empty())
                    .then(|| objref(atespace, source_tag))
                    .flatten(),
                ..Default::default()
            }),
        };
        let create = || {
            self.call(
                "CreateActor",
                UNAVAILABLE,
                req.clone(),
                |mut c, r| async move { c.create_actor(r).await },
            )
        };
        let context = || format!("creating actor {atespace}/{name}");
        match create().await {
            Ok(actor) => return Ok(actor),
            Err(e) if e.code() != Code::AlreadyExists => return Err(Error::new(context(), e)),
            Err(_) => {}
        }

        // The actor exists, as on every suspend and resume after the first.
        let existing = match self.get_actor(atespace, name).await {
            Ok(a) => a,
            // An actor being deleted went away after the create.
            Err(e) if e.code() == Code::NotFound => {
                return create().await.map_err(|e| Error::new(context(), e));
            }
            Err(e) => return Err(e),
        };
        match state(&existing) {
            ActorState::Crashed => {
                let req = RevertActorRequest {
                    actor: objref(atespace, name),
                };
                match self
                    .call(
                        "RevertActor",
                        ABORTED_OR_UNAVAILABLE,
                        req,
                        |mut c, r| async move { c.revert_actor(r).await },
                    )
                    .await
                {
                    Ok(resp) => {
                        warn!(
                            actor = name,
                            "existing actor is crashed, reverted to its last snapshot"
                        );
                        match resp.actor {
                            Some(actor) => Ok(actor),
                            None => self.get_actor(atespace, name).await,
                        }
                    }
                    Err(revert_err) => {
                        warn!(actor = name, error = %revert_err,
                            "existing actor is crashed and could not be reverted, deleting and recreating");
                        self.delete_actor(atespace, name).await?;
                        create().await.map_err(|e| {
                            Error::new(format!("recreating actor {atespace}/{name} after crash"), e)
                        })
                    }
                }
            }
            ActorState::Deleting => {
                for _ in 0..self.retry.deleting_wait_attempts {
                    tokio::time::sleep(self.retry.deleting_wait_interval).await;
                    match create().await {
                        Ok(actor) => return Ok(actor),
                        Err(e) if e.code() == Code::AlreadyExists => {}
                        Err(e) => return Err(Error::new(context(), e)),
                    }
                }
                Err(Error::new(
                    "",
                    Status::unavailable(format!(
                        "actor {atespace}/{name} is still deleting; please retry"
                    )),
                ))
            }
            _ => Ok(existing),
        }
    }

    /// Resumes the actor onto a worker and returns it with the worker's IP.
    pub async fn resume_actor(&self, atespace: &str, name: &str) -> Result<(Actor, String)> {
        let req = ResumeActorRequest {
            actor: objref(atespace, name),
        };
        let resp = self
            .call(
                "ResumeActor",
                ABORTED_OR_UNAVAILABLE,
                req,
                |mut c, r| async move { c.resume_actor(r).await },
            )
            .await
            .map_err(|e| Error::new(format!("resuming actor {atespace}/{name}"), e))?;
        let actor = resp.actor.ok_or_else(|| {
            Error::new(
                "",
                Status::internal(format!(
                    "nil actor returned when resuming {atespace}/{name}"
                )),
            )
        })?;
        let ip = worker_ip(&actor);
        Ok((actor, ip))
    }

    /// Suspends the actor, which snapshots its state.
    pub async fn suspend_actor(&self, atespace: &str, name: &str) -> Result<()> {
        let req = SuspendActorRequest {
            actor: objref(atespace, name),
            options: None,
        };
        self.call(
            "SuspendActor",
            ABORTED_OR_UNAVAILABLE,
            req,
            |mut c, r| async move { c.suspend_actor(r).await },
        )
        .await
        .map(drop)
        .map_err(|e| Error::new(format!("suspending actor {atespace}/{name}"), e))
    }

    pub async fn get_actor(&self, atespace: &str, name: &str) -> Result<Actor> {
        let req = GetActorRequest {
            actor: objref(atespace, name),
        };
        self.call("GetActor", UNAVAILABLE, req, |mut c, r| async move {
            c.get_actor(r).await
        })
        .await
        .map_err(|e| Error::new(format!("getting actor {atespace}/{name}"), e))
    }

    /// Lists every actor in the atespace, or in all atespaces when it is
    /// empty.
    pub async fn list_actors(&self, atespace: &str) -> Result<Vec<Actor>> {
        let mut actors = Vec::new();
        let mut req = ListActorsRequest {
            atespace: atespace.to_owned(),
            options: Some(ListOptions::default()),
        };
        loop {
            let resp = self
                .call(
                    "ListActors",
                    UNAVAILABLE,
                    req.clone(),
                    |mut c, r| async move { c.list_actors(r).await },
                )
                .await
                .map_err(|e| Error::new(format!("listing actors in {atespace:?}"), e))?;
            actors.extend(resp.actors);
            if resp.next_page_token.is_empty() {
                return Ok(actors);
            }
            req.options = Some(ListOptions {
                page_token: resp.next_page_token,
                ..Default::default()
            });
        }
    }

    /// Fetches the actor's egress policy. The error has code NotFound when the
    /// actor has none.
    pub async fn get_actor_egress_policy(
        &self,
        atespace: &str,
        actor: &str,
    ) -> Result<EgressPolicy> {
        let req = GetActorEgressPolicyRequest {
            actor: objref(atespace, actor),
            name: EGRESS_POLICY_NAME.to_owned(),
        };
        self.call(
            "GetActorEgressPolicy",
            UNAVAILABLE,
            req,
            |mut c, r| async move { c.get_actor_egress_policy(r).await },
        )
        .await
        .map_err(|e| {
            Error::new(
                format!("getting egress policy of actor {atespace}/{actor}"),
                e,
            )
        })
    }

    /// Creates the actor's egress policy with `rules` unless it has one.
    pub async fn ensure_actor_egress_policy(
        &self,
        atespace: &str,
        actor: &str,
        rules: &[EgressRule],
    ) -> Result<()> {
        let req = CreateActorEgressPolicyRequest {
            actor: objref(atespace, actor),
            egress_policy: Some(EgressPolicy {
                metadata: Some(ResourceMetadata {
                    atespace: atespace.to_owned(),
                    name: EGRESS_POLICY_NAME.to_owned(),
                    ..Default::default()
                }),
                rules: rules.to_vec(),
            }),
        };
        match self
            .call(
                "CreateActorEgressPolicy",
                UNAVAILABLE,
                req,
                |mut c, r| async move { c.create_actor_egress_policy(r).await },
            )
            .await
        {
            Err(e) if e.code() != Code::AlreadyExists => Err(Error::new(
                format!("creating egress policy of actor {atespace}/{actor}"),
                e,
            )),
            _ => Ok(()),
        }
    }

    /// Deletes the actor and waits until it is gone. A missing actor is not
    /// an error.
    pub async fn delete_actor(&self, atespace: &str, name: &str) -> Result<()> {
        let req = DeleteActorRequest {
            actor: objref(atespace, name),
            options: None,
        };
        match self
            .call(
                "DeleteActor",
                ABORTED_OR_UNAVAILABLE,
                req,
                |mut c, r| async move { c.delete_actor(r).await },
            )
            .await
        {
            Err(e) if e.code() == Code::NotFound => return Ok(()),
            Err(e) => return Err(Error::new(format!("deleting actor {atespace}/{name}"), e)),
            Ok(_) => {}
        }
        let wait = async {
            loop {
                let req = Request::new(GetActorRequest {
                    actor: objref(atespace, name),
                });
                let get = rax_otelenv::rpc::client_call(
                    "substrate.ateapi.v1.Control/GetActor",
                    req,
                    |r| {
                        let mut c = self.control.clone();
                        async move { c.get_actor(r).await }
                    },
                );
                if let Err(e) = get.await
                    && e.code() == Code::NotFound
                {
                    return;
                }
                tokio::time::sleep(self.retry.poll_interval).await;
            }
        };
        tokio::time::timeout(self.retry.delete_timeout, wait)
            .await
            .map_err(|_| {
                Error::new(
                    format!("waiting for actor {atespace}/{name} deletion"),
                    Status::deadline_exceeded("context deadline exceeded"),
                )
            })
    }

    /// Lists the templates in the atespace. Only the first page is returned.
    pub async fn list_actor_templates(&self, atespace: &str) -> Result<Vec<ActorTemplate>> {
        let req = ListActorTemplatesRequest {
            atespace: atespace.to_owned(),
            options: None,
        };
        let resp = self
            .call(
                "ListActorTemplates",
                UNAVAILABLE,
                req,
                |mut c, r| async move { c.list_actor_templates(r).await },
            )
            .await
            .map_err(|e| Error::new(format!("listing actor templates in {atespace}"), e))?;
        if !resp.next_page_token.is_empty() {
            warn!(atespace, "actor template listing was truncated");
        }
        Ok(resp.actor_templates)
    }

    /// Deletes the template. A missing template is not an error.
    pub async fn delete_actor_template(&self, atespace: &str, name: &str) -> Result<()> {
        let req = DeleteActorTemplateRequest {
            actor_template: objref(atespace, name),
            options: None,
        };
        match self
            .call(
                "DeleteActorTemplate",
                ABORTED_OR_UNAVAILABLE,
                req,
                |mut c, r| async move { c.delete_actor_template(r).await },
            )
            .await
        {
            Err(e) if e.code() != Code::NotFound => Err(Error::new(
                format!("deleting actor template {atespace}/{name}"),
                e,
            )),
            _ => Ok(()),
        }
    }

    // Snapshot tags.

    /// Creates an atespace-scoped Tag capturing the snapshot of the suspended
    /// actor `source_actor`. A tag left by an earlier incomplete attempt
    /// should be deleted before retrying.
    pub async fn create_tag(&self, atespace: &str, name: &str, source_actor: &str) -> Result<Tag> {
        let req = CreateTagRequest {
            tag: Some(Tag {
                metadata: Some(ResourceMetadata {
                    atespace: atespace.to_owned(),
                    name: name.to_owned(),
                    ..Default::default()
                }),
                scope: TagScope::Atespace as i32,
                source_actor: objref(atespace, source_actor),
                ..Default::default()
            }),
        };
        self.call(
            "CreateTag",
            ABORTED_OR_UNAVAILABLE,
            req,
            |mut c, r| async move { c.create_tag(r).await },
        )
        .await
        .map_err(|e| {
            Error::new(
                format!("creating tag {atespace}/{name} from actor {source_actor}"),
                e,
            )
        })
    }

    pub async fn get_tag(&self, atespace: &str, name: &str) -> Result<Tag> {
        let req = GetTagRequest {
            tag: objref(atespace, name),
        };
        self.call("GetTag", UNAVAILABLE, req, |mut c, r| async move {
            c.get_tag(r).await
        })
        .await
        .map_err(|e| Error::new(format!("getting tag {atespace}/{name}"), e))
    }

    /// Lists every Tag in the atespace, or in all atespaces when it is empty.
    pub async fn list_tags(&self, atespace: &str) -> Result<Vec<Tag>> {
        let mut tags = Vec::new();
        let mut req = ListTagsRequest {
            atespace: atespace.to_owned(),
            options: Some(ListOptions::default()),
        };
        loop {
            let resp = self
                .call(
                    "ListTags",
                    UNAVAILABLE,
                    req.clone(),
                    |mut c, r| async move { c.list_tags(r).await },
                )
                .await
                .map_err(|e| Error::new(format!("listing tags in {atespace:?}"), e))?;
            tags.extend(resp.tags);
            if resp.next_page_token.is_empty() {
                return Ok(tags);
            }
            req.options = Some(ListOptions {
                page_token: resp.next_page_token,
                ..Default::default()
            });
        }
    }

    /// Deletes a Tag and the snapshot it owns. A missing Tag is not an error.
    pub async fn delete_tag(&self, atespace: &str, name: &str) -> Result<()> {
        let req = DeleteTagRequest {
            tag: objref(atespace, name),
            options: None,
        };
        match self
            .call(
                "DeleteTag",
                ABORTED_OR_UNAVAILABLE,
                req,
                |mut c, r| async move { c.delete_tag(r).await },
            )
            .await
        {
            Err(e) if e.code() != Code::NotFound => {
                Err(Error::new(format!("deleting tag {atespace}/{name}"), e))
            }
            _ => Ok(()),
        }
    }
}
