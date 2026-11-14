//! Validation the API server applies before saving a resource.
//!
//! A resource's name and atespace become Substrate resource names (the task's
//! actor and ActorTemplate, and the atespace itself), which must be lowercase
//! RFC 1123 labels. Checking them at apply time turns an asynchronous
//! ActorCreationFailed condition into an immediate error.

use std::sync::LazyLock;

use regex::Regex;

use crate::substrate::ateapi::v1::{EgressRule, HttpRuleEffects, Ports};
use crate::v1alpha1::{
    File, Gateway, GatewayRef, GitRepo, Model, ObjectMeta, Profile, RetryConfig, SkillsConfig,
    Task, TaskSpec, Workspace, WorkspaceSetup,
};

/// A problem that makes a resource unusable, phrased for the person who wrote
/// the manifest.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ValidationError(String);

impl ValidationError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }

    /// Prefixes the message with the field it is about.
    pub fn prefixed(self, field: &str) -> Self {
        Self(format!("{field}: {}", self.0))
    }
}

/// The longest name or atespace a resource may have.
pub const MAX_NAME_LENGTH: usize = 63;

/// Where the task's workspace is mounted unless the binding sets a path. It is
/// the task command's working directory, and a task with no workspace gets an
/// empty directory there.
pub const DEFAULT_WORKSPACE_PATH: &str = "/workspace";

/// The longest environment variable value Substrate accepts (EnvVar.value in
/// ateapi.proto caps it at 32,768 characters), applied to the byte length.
pub const MAX_ENV_VALUE_SIZE: usize = 32 << 10;

/// The largest a task's launch configuration may be once encoded as YAML, the
/// form in which it reaches the runner through `AX_TASK_YAML`. It keeps 1 KiB
/// of [`MAX_ENV_VALUE_SIZE`] in reserve for fields the control plane fills in
/// after validation, such as metadata.atespace, metadata.uid,
/// metadata.createTime and spec.sessionId.
pub const MAX_TASK_YAML_SIZE: usize = MAX_ENV_VALUE_SIZE - (1 << 10);

/// The largest spec.config a task may have, measured as its JSON encoding. It
/// leaves room for the rest of the spec within [`MAX_TASK_YAML_SIZE`] and gives
/// a more specific error than [`validate_task_size`].
pub const MAX_TASK_CONFIG_SIZE: usize = 16 << 10;

static NAME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$").unwrap());

/// Reports whether `s` can be used as a resource name or atespace: a lowercase
/// RFC 1123 label of at most [`MAX_NAME_LENGTH`] characters.
pub fn validate_name(s: &str) -> Result<(), ValidationError> {
    if s.is_empty() {
        return Err(ValidationError::new("must not be empty"));
    }
    if s.len() > MAX_NAME_LENGTH || !NAME.is_match(s) {
        return Err(ValidationError::new(format!(
            "must be a lowercase RFC 1123 label: at most {MAX_NAME_LENGTH} lowercase alphanumeric characters or '-', starting and ending with an alphanumeric character"
        )));
    }
    Ok(())
}

/// Checks a resource's name and, when set, its atespace. An empty atespace is
/// fine here; the API server defaults it.
pub fn validate_object_meta(meta: Option<&ObjectMeta>) -> Result<(), ValidationError> {
    let (name, atespace) = meta.map_or(("", ""), |m| (m.name.as_str(), m.atespace.as_str()));
    validate_name(name)
        .map_err(|e| e.prefixed(&format!("metadata.name: invalid value {name:?}")))?;
    if !atespace.is_empty() {
        validate_name(atespace)
            .map_err(|e| e.prefixed(&format!("metadata.atespace: invalid value {atespace:?}")))?;
    }
    Ok(())
}

/// Reports the first problem with a workspace that would make it unusable.
pub fn validate_workspace(w: &Workspace) -> Result<(), ValidationError> {
    validate_object_meta(w.metadata.as_ref())?;
    let Some(spec) = &w.spec else { return Ok(()) };
    validate_profile("spec.profile", spec.profile.as_ref())?;
    validate_setup(spec.setup.as_ref())
}

/// Checks a compute profile: an `ateActorTemplate` names an existing
/// ActorTemplate and stands alone; otherwise the resources must be valid.
fn validate_profile(field: &str, p: Option<&Profile>) -> Result<(), ValidationError> {
    let Some(p) = p else { return Ok(()) };
    let tpl = p.ate_actor_template.as_str();
    if !tpl.is_empty() {
        validate_name(tpl)
            .map_err(|e| e.prefixed(&format!("{field}.ateActorTemplate: invalid value {tpl:?}")))?;
        if !p.image.is_empty() || p.resources.is_some() {
            return Err(ValidationError::new(format!(
                "{field}.ateActorTemplate: cannot be combined with image or resources"
            )));
        }
        return Ok(());
    }
    crate::resources::validate_resources_at(&format!("{field}.resources"), p.resources.as_ref())
}

fn validate_gateway_ref(field: &str, r: Option<&GatewayRef>) -> Result<(), ValidationError> {
    let Some(r) = r else { return Ok(()) };
    validate_name(&r.name)
        .map_err(|e| e.prefixed(&format!("{field}.name: invalid value {:?}", r.name)))
}

fn validate_setup(setup: Option<&WorkspaceSetup>) -> Result<(), ValidationError> {
    let Some(setup) = setup else { return Ok(()) };
    for (i, g) in setup.git.iter().enumerate() {
        validate_git_repo(&format!("spec.setup.git[{i}]"), g)?;
    }
    for (i, f) in setup.files.iter().enumerate() {
        validate_setup_file(&format!("spec.setup.files[{i}]"), f)?;
    }
    validate_skills(setup.skills.as_ref())?;
    validate_gateway_ref("spec.setup.gateway", setup.gateway.as_ref())
}

fn validate_git_repo(field: &str, g: &GitRepo) -> Result<(), ValidationError> {
    let repo = g.repo.as_str();
    if repo.trim().is_empty() {
        return Err(ValidationError::new(format!("{field}: repo is required")));
    }
    if has_invalid_git_arg(repo) {
        return Err(ValidationError::new(format!(
            "{field}.repo: invalid repository {repo:?}"
        )));
    }
    let lower = repo.to_lowercase();
    if lower.starts_with("git@") || lower.starts_with("ssh://") {
        return Err(ValidationError::new(format!(
            "{field}.repo: SSH git URLs ({repo:?}) are not supported; use HTTPS"
        )));
    }
    if !g.r#ref.is_empty() && has_invalid_git_arg(&g.r#ref) {
        return Err(ValidationError::new(format!(
            "{field}.ref: invalid ref {:?}",
            g.r#ref
        )));
    }
    if g.depth < 0 {
        return Err(ValidationError::new(format!(
            "{field}.depth: must be greater than or equal to 0"
        )));
    }
    if !g.name.is_empty() {
        validate_relative_path(&format!("{field}.name"), &g.name)?;
        if clean_path(&g.name) == "." {
            return Err(ValidationError::new(format!(
                "{field}.name: {:?} is invalid; set dir: \".\" to clone into the workspace root",
                g.name
            )));
        }
    }
    validate_relative_path(&format!("{field}.dir"), &g.dir)
}

fn validate_setup_file(field: &str, f: &File) -> Result<(), ValidationError> {
    let p = f.path.as_str();
    if p.trim().is_empty() {
        return Err(ValidationError::new(format!("{field}: path is required")));
    }
    validate_relative_path(&format!("{field}.path"), p)?;
    if p.ends_with('/') || clean_path(p) == "." {
        return Err(ValidationError::new(format!(
            "{field}.path: {p:?} must name a file, not a directory"
        )));
    }
    Ok(())
}

fn validate_skills(skills: Option<&SkillsConfig>) -> Result<(), ValidationError> {
    let p = skills.map_or("", |s| s.path.as_str());
    if !p.is_empty() && !p.starts_with('/') && escapes_workspace_root(p) {
        return Err(ValidationError::new(format!(
            "spec.setup.skills.path: {p:?} must not escape the workspace root"
        )));
    }
    Ok(())
}

/// Reports whether `s` could be read by git as an option or split into
/// several arguments.
fn has_invalid_git_arg(s: &str) -> bool {
    s.starts_with('-') || s.contains([' ', '\t', '\r', '\n', '\0'])
}

fn validate_relative_path(field: &str, p: &str) -> Result<(), ValidationError> {
    if p.is_empty() {
        return Ok(());
    }
    if p.starts_with('/') {
        return Err(ValidationError::new(format!(
            "{field}: {p:?} must be relative to the workspace root"
        )));
    }
    if escapes_workspace_root(p) {
        return Err(ValidationError::new(format!(
            "{field}: {p:?} must not escape the workspace root"
        )));
    }
    Ok(())
}

fn escapes_workspace_root(rel: &str) -> bool {
    let clean = clean_path(rel);
    clean == ".." || clean.starts_with("../")
}

/// Lexically cleans a slash-separated path the way Go's `path.Clean` does:
/// it drops empty and `.` elements and resolves `..` against the elements
/// before it. An empty result is `.`.
pub(crate) fn clean_path(p: &str) -> String {
    let rooted = p.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for part in p.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if out.last().is_some_and(|l| *l != "..") {
                    out.pop();
                } else if !rooted {
                    out.push("..");
                }
            }
            _ => out.push(part),
        }
    }
    let joined = out.join("/");
    match (rooted, joined.is_empty()) {
        (true, _) => format!("/{joined}"),
        (false, true) => ".".into(),
        (false, false) => joined,
    }
}

/// Reports the first problem with a model that would make it unusable.
pub fn validate_model(m: &Model) -> Result<(), ValidationError> {
    validate_object_meta(m.metadata.as_ref())
}

/// Reports the first problem with a gateway that would make it unusable. The
/// API server calls it before saving. Agent Substrate checks the rest of each
/// egress rule later, when a task that binds the gateway is created.
pub fn validate_gateway(g: &Gateway) -> Result<(), ValidationError> {
    validate_object_meta(g.metadata.as_ref())?;
    let rules = g.spec.as_ref().map_or(&[][..], |s| &s.egress[..]);
    for (i, rule) in rules.iter().enumerate() {
        // A rule kind added by a newer Substrate would skip the checks below.
        let Some(RuleFields {
            kind,
            hostnames,
            effects,
            ports,
        }) = egress_rule_fields(rule)
        else {
            return Err(ValidationError::new(format!(
                "spec.egress[{i}]: exactly one of http, https, or tlsPassthrough is required"
            )));
        };
        let field = format!("spec.egress[{i}].{kind}");
        let credential = effects.is_some_and(|e| !e.replace_headers.is_empty());
        if credential && kind == "http" {
            return Err(ValidationError::new(format!(
                "{field}.effects.replaceHeaders: not allowed on an http rule: the credential would be sent in cleartext"
            )));
        }
        for (j, name) in hostnames.iter().enumerate() {
            if let Some(reason) = denied_egress_hostname(name) {
                return Err(ValidationError::new(format!(
                    "{field}.hostnames[{j}]: {name:?} is not allowed: {reason}"
                )));
            }
            if credential && name.starts_with("*.") {
                return Err(ValidationError::new(format!(
                    "{field}.effects.replaceHeaders: not allowed with wildcard hostname {name:?}: name each host that gets the credential"
                )));
            }
        }
        // Unlike http and https, passthrough has no default port.
        if kind == "tlsPassthrough" && ports.is_none_or(|p| p.all.is_none() && p.numbers.is_empty())
        {
            return Err(ValidationError::new(format!(
                "{field}.ports: required, set numbers or all"
            )));
        }
    }
    Ok(())
}

/// The fields of an egress rule that the checks read.
struct RuleFields<'a> {
    /// The kind of rule that is set, named as in a manifest.
    kind: &'static str,
    hostnames: &'a [String],
    effects: Option<&'a HttpRuleEffects>,
    ports: Option<&'a Ports>,
}

/// The fields of the one kind of rule that is set, or `None` unless exactly
/// one is.
fn egress_rule_fields(rule: &EgressRule) -> Option<RuleFields<'_>> {
    let (kind, hostnames, effects, ports) = match (&rule.http, &rule.https, &rule.tls_passthrough) {
        (Some(r), None, None) => ("http", &r.hostnames, r.effects.as_ref(), r.ports.as_ref()),
        (None, Some(r), None) => ("https", &r.hostnames, r.effects.as_ref(), r.ports.as_ref()),
        (None, None, Some(r)) => ("tlsPassthrough", &r.hostnames, None, r.ports.as_ref()),
        _ => return None,
    };
    Some(RuleFields {
        kind,
        hostnames,
        effects,
        ports,
    })
}

/// Why a hostname is not allowed, or `None` if it is.
fn denied_egress_hostname(name: &str) -> Option<&'static str> {
    if name == "*" {
        Some("it matches every host, including hosts inside the cluster")
    } else if is_ip_address(name) {
        Some("IP addresses cannot be used, name the host instead")
    } else if name == "localhost" || name.ends_with(".localhost") {
        Some("it would be the egress gateway's localhost; a task reaches its own without a rule")
    } else {
        None
    }
}

/// Reports whether `name` is an IP address. As in Agent Substrate, a name whose
/// last label is a number counts as one.
fn is_ip_address(name: &str) -> bool {
    // Go's netip.ParseAddr also takes an IPv6 zone, which Rust's parser does not.
    let unzoned = name.split_once('%').map_or(name, |(addr, _)| addr);
    if name.parse::<std::net::IpAddr>().is_ok() || unzoned.parse::<std::net::Ipv6Addr>().is_ok() {
        return true;
    }
    let last = name.rsplit('.').next().unwrap_or("");
    !last.is_empty() && last.bytes().all(|b| b.is_ascii_digit())
}

/// Reports the first problem with a task that would make it impossible to run
/// correctly.
pub fn validate_task(t: &Task) -> Result<(), ValidationError> {
    validate_object_meta(t.metadata.as_ref())?;
    let spec = t.spec.as_ref();
    validate_profile("spec.profile", spec.and_then(|s| s.profile.as_ref()))?;
    if let Some(r) = spec.and_then(|s| s.workspace.as_ref()) {
        if r.name.is_empty() {
            return Err(ValidationError::new("spec.workspace: name is required"));
        }
        if !r.path.is_empty() && !r.path.starts_with('/') {
            return Err(ValidationError::new(format!(
                "spec.workspace: path {:?} must be absolute",
                r.path
            )));
        }
    }
    validate_gateway_ref("spec.gateway", spec.and_then(|s| s.gateway.as_ref()))?;
    validate_retry("spec.retry", spec.and_then(|s| s.retry.as_ref()))?;
    if let Some(spec) = spec {
        validate_task_config(spec)?;
    }
    validate_task_size(t)
}

fn validate_retry(field: &str, r: Option<&RetryConfig>) -> Result<(), ValidationError> {
    let Some(r) = r else { return Ok(()) };
    if r.max_retries < 0 {
        return Err(ValidationError::new(format!(
            "{field}.maxRetries: must not be negative"
        )));
    }
    let nanos = |d: &pbjson_types::Duration| -> Option<i128> {
        // A valid Duration's seconds and nanos agree in sign, within the
        // range protobuf allows.
        let valid = d.seconds.abs() <= 315_576_000_000
            && d.nanos.abs() < 1_000_000_000
            && !(d.seconds > 0 && d.nanos < 0 || d.seconds < 0 && d.nanos > 0);
        valid.then(|| i128::from(d.seconds) * 1_000_000_000 + i128::from(d.nanos))
    };
    let initial = r.initial_backoff.as_ref().map(nanos);
    let max = r.max_backoff.as_ref().map(nanos);
    if initial.is_some_and(|d| d.is_none_or(|n| n < 0)) {
        return Err(ValidationError::new(format!(
            "{field}.initialBackoff: must not be negative"
        )));
    }
    if max.is_some_and(|d| d.is_none_or(|n| n < 0)) {
        return Err(ValidationError::new(format!(
            "{field}.maxBackoff: must not be negative"
        )));
    }
    if let (Some(Some(initial)), Some(Some(max))) = (initial, max)
        && max > 0
        && initial > max
    {
        return Err(ValidationError::new(format!(
            "{field}.initialBackoff: must not exceed maxBackoff"
        )));
    }
    let m = r.backoff_multiplier;
    if m != 0.0 && (!m.is_finite() || m < 1.0) {
        return Err(ValidationError::new(format!(
            "{field}.backoffMultiplier: must be >= 1.0"
        )));
    }
    Ok(())
}

/// Checks that the task's launch configuration, encoded as the control plane
/// encodes it for `AX_TASK_YAML`, fits within [`MAX_TASK_YAML_SIZE`]. Larger
/// tasks would be rejected by Substrate when their template is created.
pub fn validate_task_size(t: &Task) -> Result<(), ValidationError> {
    let launch = Task {
        status: None,
        ..t.clone()
    };
    let data =
        crate::to_yaml(&launch).map_err(|e| ValidationError::new(format!("encoding task: {e}")))?;
    if data.len() > MAX_TASK_YAML_SIZE {
        return Err(ValidationError::new(format!(
            "task: encoded launch configuration is {} bytes, which exceeds the limit of {MAX_TASK_YAML_SIZE} bytes",
            data.len()
        )));
    }
    Ok(())
}

/// Checks that spec.config fits within [`MAX_TASK_CONFIG_SIZE`]. The config is
/// otherwise opaque: its schema belongs to the runner.
pub fn validate_task_config(spec: &TaskSpec) -> Result<(), ValidationError> {
    let Some(config) = &spec.config else {
        return Ok(());
    };
    let data = serde_json::to_vec(config)
        .map_err(|e| ValidationError::new(format!("spec.config: {e}")))?;
    if data.len() > MAX_TASK_CONFIG_SIZE {
        return Err(ValidationError::new(format!(
            "spec.config: {} bytes exceeds the limit of {MAX_TASK_CONFIG_SIZE} bytes",
            data.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1alpha1::{
        EnvVar, Goal, McpConfig, McpServer, ResourceList, ResourcesSpec, WorkspaceRef,
        WorkspaceSpec,
    };

    fn meta(name: &str, atespace: &str) -> Option<ObjectMeta> {
        Some(ObjectMeta {
            name: name.into(),
            atespace: atespace.into(),
            ..Default::default()
        })
    }

    pub(crate) fn config(json: serde_json::Value) -> pbjson_types::Struct {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn names() {
        let long_ok = "a".repeat(63);
        for name in [
            "a",
            "0",
            "task123",
            "my-task",
            "a-b-c",
            "123-abc",
            long_ok.as_str(),
        ] {
            assert!(validate_name(name).is_ok(), "{name}");
        }
        let long_bad = "a".repeat(64);
        for name in [
            "",
            "Task-With-Caps",
            "my_task",
            "my.task",
            "-leading-dash",
            "trailing-dash-",
            "with space",
            "tab\tchar",
            "ünïcödé",
            "slash/name",
            long_bad.as_str(),
        ] {
            assert!(validate_name(name).is_err(), "{name:?}");
        }
    }

    #[test]
    fn object_meta() {
        let cases: Vec<(&str, Option<ObjectMeta>, &str)> = vec![
            ("nil metadata", None, "metadata.name"),
            (
                "missing name",
                meta("", "default"),
                r#"metadata.name: invalid value """#,
            ),
            ("name only", meta("task123", ""), ""),
            ("name and atespace", meta("task123", "team-a"), ""),
            (
                "uppercase name",
                meta("Task-With-Caps", ""),
                r#"metadata.name: invalid value "Task-With-Caps""#,
            ),
            ("underscore in name", meta("my_task", ""), "metadata.name"),
            ("name too long", meta(&"x".repeat(64), ""), "metadata.name"),
            (
                "uppercase atespace",
                meta("task123", "Default"),
                r#"metadata.atespace: invalid value "Default""#,
            ),
            (
                "dotted atespace",
                meta("task123", "team.a"),
                "metadata.atespace",
            ),
        ];
        for (name, m, want) in cases {
            match validate_object_meta(m.as_ref()) {
                Ok(()) => assert!(
                    want.is_empty(),
                    "{name}: expected error containing {want:?}"
                ),
                Err(e) => assert!(
                    !want.is_empty() && e.to_string().contains(want),
                    "{name}: {e}"
                ),
            }
        }
        assert!(
            validate_task(&Task {
                metadata: meta("Bad", ""),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            validate_workspace(&Workspace {
                metadata: meta("Bad", ""),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            validate_model(&Model {
                metadata: meta("Bad", ""),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            validate_gateway(&Gateway {
                metadata: meta("Bad", ""),
                ..Default::default()
            })
            .is_err()
        );
    }

    #[test]
    fn gateways() {
        use crate::substrate::ateapi::v1::{
            AllPorts, CredentialHeader, HttpRule, HttpsRule, TlsPassthroughRule,
        };
        use crate::v1alpha1::GatewaySpec;
        let names = |h: &[&str]| h.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let http = |effects: Option<HttpRuleEffects>, h: &[&str]| EgressRule {
            http: Some(HttpRule {
                hostnames: names(h),
                effects,
                ..Default::default()
            }),
            ..Default::default()
        };
        let https = |effects: Option<HttpRuleEffects>, h: &[&str]| EgressRule {
            https: Some(HttpsRule {
                hostnames: names(h),
                effects,
                ..Default::default()
            }),
            ..Default::default()
        };
        let passthrough = |ports: Option<Ports>, h: &[&str]| EgressRule {
            tls_passthrough: Some(TlsPassthroughRule {
                hostnames: names(h),
                ports,
            }),
            ..Default::default()
        };
        let credential = || {
            Some(HttpRuleEffects {
                replace_headers: vec![CredentialHeader {
                    header: "Authorization".into(),
                    credential_uri: "ate-secret://k8s.io/default/default/token/value".into(),
                    ..Default::default()
                }],
            })
        };
        let port443 = || {
            Some(Ports {
                numbers: vec![443],
                ..Default::default()
            })
        };
        let cases: Vec<(&str, EgressRule, &str)> = vec![
            (
                "host with a credential",
                https(credential(), &["api.example.com"]),
                "",
            ),
            ("subdomain wildcard", https(None, &["*.example.com"]), ""),
            (
                "passthrough with port numbers",
                passthrough(port443(), &["db.example.com"]),
                "",
            ),
            (
                "passthrough with every port",
                passthrough(
                    Some(Ports {
                        all: Some(AllPorts {}),
                        ..Default::default()
                    }),
                    &["db.example.com"],
                ),
                "",
            ),
            (
                "names that resemble denied ones",
                https(None, &["notlocalhost", "1.example.com"]),
                "",
            ),
            (
                "every host",
                https(None, &["*"]),
                r#"spec.egress[0].https.hostnames[0]: "*" is not allowed: it matches every host"#,
            ),
            (
                "subdomain wildcard with a credential",
                https(credential(), &["*.example.com"]),
                r#"spec.egress[0].https.effects.replaceHeaders: not allowed with wildcard hostname "*.example.com""#,
            ),
            (
                "credential over http",
                http(credential(), &["api.example.com"]),
                "spec.egress[0].http.effects.replaceHeaders: not allowed on an http rule",
            ),
            (
                "rule without a kind",
                EgressRule::default(),
                "spec.egress[0]: exactly one of http, https, or tlsPassthrough is required",
            ),
            (
                "rule with two kinds",
                EgressRule {
                    https: https(None, &["example.com"]).https,
                    ..http(None, &["example.com"])
                },
                "exactly one of http, https, or tlsPassthrough is required",
            ),
            (
                "passthrough without ports",
                passthrough(None, &["db.example.com"]),
                "spec.egress[0].tlsPassthrough.ports: required",
            ),
            (
                "passthrough to localhost",
                passthrough(port443(), &["example.com", "localhost"]),
                r#"spec.egress[0].tlsPassthrough.hostnames[1]: "localhost" is not allowed: it would be the egress gateway's localhost"#,
            ),
            (
                "name under localhost",
                http(None, &["app.localhost"]),
                "it would be the egress gateway's localhost",
            ),
            (
                "ipv6 address",
                https(None, &["fd00::1"]),
                r#"spec.egress[0].https.hostnames[0]: "fd00::1" is not allowed: IP addresses cannot be used"#,
            ),
            (
                "ip address in short form",
                https(None, &["127.1"]),
                "IP addresses cannot be used",
            ),
        ];
        for (name, rule, want) in cases {
            let gw = Gateway {
                metadata: meta("gw", ""),
                spec: Some(GatewaySpec { egress: vec![rule] }),
                ..Default::default()
            };
            match validate_gateway(&gw) {
                Ok(()) => assert!(
                    want.is_empty(),
                    "{name}: expected error containing {want:?}"
                ),
                Err(e) => assert!(
                    !want.is_empty() && e.to_string().contains(want),
                    "{name}: {e}"
                ),
            }
        }
    }

    #[test]
    fn tasks() {
        let ws = |name: &str, path: &str| {
            Some(WorkspaceRef {
                name: name.into(),
                path: path.into(),
            })
        };
        let limits = |cpu: &str, memory: &str| ResourcesSpec {
            limits: Some(ResourceList {
                cpu: cpu.into(),
                memory: memory.into(),
            }),
        };
        let template = |name: &str| Profile {
            ate_actor_template: name.into(),
            ..Default::default()
        };
        let secs = |seconds: i64| pbjson_types::Duration { seconds, nanos: 0 };
        let retry = |r: RetryConfig| TaskSpec {
            retry: Some(r),
            ..Default::default()
        };
        let blob = |n: usize| config(serde_json::json!({ "blob": "x".repeat(n) }));
        let cases: Vec<(&str, Option<TaskSpec>, &str)> = vec![
            ("nil spec", None, ""),
            ("no workspace", Some(TaskSpec::default()), ""),
            (
                "default path",
                Some(TaskSpec {
                    workspace: ws("a", ""),
                    ..Default::default()
                }),
                "",
            ),
            (
                "explicit path",
                Some(TaskSpec {
                    workspace: ws("a", "/x"),
                    ..Default::default()
                }),
                "",
            ),
            (
                "nameless binding",
                Some(TaskSpec {
                    workspace: ws("", "/w"),
                    ..Default::default()
                }),
                "spec.workspace: name is required",
            ),
            (
                "gateway",
                Some(TaskSpec {
                    gateway: Some(GatewayRef { name: "gw".into() }),
                    ..Default::default()
                }),
                "",
            ),
            (
                "nameless gateway",
                Some(TaskSpec {
                    gateway: Some(GatewayRef::default()),
                    ..Default::default()
                }),
                "spec.gateway.name",
            ),
            (
                "profile image and resources",
                Some(TaskSpec {
                    profile: Some(Profile {
                        image: "us-docker.pkg.dev/p/img@sha256:abc".into(),
                        resources: Some(limits("2", "4Gi")),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                "",
            ),
            (
                "profile ate_actor_template",
                Some(TaskSpec {
                    profile: Some(template("base-template")),
                    ..Default::default()
                }),
                "",
            ),
            (
                "profile ate_actor_template invalid name",
                Some(TaskSpec {
                    profile: Some(template("Bad_Name")),
                    ..Default::default()
                }),
                "spec.profile.ateActorTemplate",
            ),
            (
                "profile ate_actor_template combined with image",
                Some(TaskSpec {
                    profile: Some(Profile {
                        image: "us-docker.pkg.dev/p/img@sha256:abc".into(),
                        ..template("base-template")
                    }),
                    ..Default::default()
                }),
                "cannot be combined with image or resources",
            ),
            (
                "profile ate_actor_template combined with resources",
                Some(TaskSpec {
                    profile: Some(Profile {
                        resources: Some(limits("1", "")),
                        ..template("base-template")
                    }),
                    ..Default::default()
                }),
                "cannot be combined with image or resources",
            ),
            (
                "goal retry and session_id",
                Some(TaskSpec {
                    goal: Some(Goal {
                        spec: "Fix failing unit tests".into(),
                    }),
                    session_id: "session-42".into(),
                    retry: Some(RetryConfig {
                        max_retries: 3,
                        initial_backoff: Some(secs(10)),
                        max_backoff: Some(secs(300)),
                        backoff_multiplier: 2.0,
                    }),
                    ..Default::default()
                }),
                "",
            ),
            (
                "negative max_retries",
                Some(retry(RetryConfig {
                    max_retries: -1,
                    ..Default::default()
                })),
                "spec.retry.maxRetries",
            ),
            (
                "negative initial_backoff",
                Some(retry(RetryConfig {
                    initial_backoff: Some(secs(-1)),
                    ..Default::default()
                })),
                "spec.retry.initialBackoff",
            ),
            (
                "negative max_backoff",
                Some(retry(RetryConfig {
                    max_backoff: Some(secs(-1)),
                    ..Default::default()
                })),
                "spec.retry.maxBackoff",
            ),
            (
                "initial_backoff exceeds max_backoff",
                Some(retry(RetryConfig {
                    initial_backoff: Some(secs(120)),
                    max_backoff: Some(secs(60)),
                    ..Default::default()
                })),
                "spec.retry.initialBackoff: must not exceed maxBackoff",
            ),
            (
                "backoff_multiplier below 1",
                Some(retry(RetryConfig {
                    backoff_multiplier: 0.5,
                    ..Default::default()
                })),
                "spec.retry.backoffMultiplier",
            ),
            (
                "relative path",
                Some(TaskSpec {
                    workspace: ws("a", "rel"),
                    ..Default::default()
                }),
                r#"spec.workspace: path "rel" must be absolute"#,
            ),
            (
                "config",
                Some(TaskSpec {
                    config: Some(config(
                        serde_json::json!({"suite": "swe", "shard": {"index": 7}}),
                    )),
                    ..Default::default()
                }),
                "",
            ),
            (
                "oversized config",
                Some(TaskSpec {
                    config: Some(blob(MAX_TASK_CONFIG_SIZE)),
                    ..Default::default()
                }),
                "spec.config:",
            ),
            (
                "config near the limit",
                Some(TaskSpec {
                    config: Some(blob(MAX_TASK_CONFIG_SIZE - 100)),
                    ..Default::default()
                }),
                "",
            ),
            (
                // Each value fits on its own, but together they do not fit in
                // one Substrate environment variable.
                "oversized task",
                Some(TaskSpec {
                    config: Some(blob(MAX_TASK_CONFIG_SIZE - 100)),
                    env: vec![EnvVar {
                        name: "BIG".into(),
                        value: "y".repeat(16 << 10),
                    }],
                    ..Default::default()
                }),
                "task: encoded launch configuration",
            ),
            (
                "oversized commands",
                Some(TaskSpec {
                    commands: vec!["sh".into(), "-c".into(), "z".repeat(MAX_ENV_VALUE_SIZE)],
                    ..Default::default()
                }),
                "task: encoded launch configuration",
            ),
        ];
        for (name, spec, want) in cases {
            let task = Task {
                metadata: meta("task", ""),
                spec,
                ..Default::default()
            };
            match validate_task(&task) {
                Ok(()) => assert!(
                    want.is_empty(),
                    "{name}: expected error containing {want:?}"
                ),
                Err(e) => assert!(
                    !want.is_empty() && e.to_string().contains(want),
                    "{name}: {e}"
                ),
            }
        }
    }

    #[test]
    fn valid_workspaces() {
        let full = WorkspaceSpec {
            profile: Some(Profile {
                image: "us-docker.pkg.dev/p/img@sha256:abc".into(),
                resources: Some(ResourcesSpec {
                    limits: Some(ResourceList {
                        cpu: "2".into(),
                        memory: "4Gi".into(),
                    }),
                }),
                ..Default::default()
            }),
            setup: Some(WorkspaceSetup {
                git: vec![GitRepo {
                    repo: "https://github.com/org/repo.git".into(),
                    dir: ".".into(),
                    r#ref: "main".into(),
                    depth: 1,
                    ..Default::default()
                }],
                files: vec![File {
                    path: "AGENTS.md".into(),
                    content: "hi".into(),
                }],
                commands: vec!["go mod download".into()],
                gateway: Some(GatewayRef {
                    name: "build-gw".into(),
                }),
                ..Default::default()
            }),
            mcp: Some(McpConfig {
                servers: vec![McpServer {
                    name: "gh".into(),
                    endpoint: "https://example.com/mcp".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
        };
        let local = WorkspaceSpec {
            setup: Some(WorkspaceSetup {
                git: vec![GitRepo {
                    repo: "/opt/e2e-fixtures/sample-repo.git".into(),
                    r#ref: "main".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        for (name, spec) in [
            ("nil spec", None),
            ("full", Some(full)),
            ("local repo", Some(local)),
        ] {
            let ws = Workspace {
                metadata: meta("valid-ws", ""),
                spec,
                ..Default::default()
            };
            validate_workspace(&ws).unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }

    #[test]
    fn invalid_workspaces() {
        let setup = |s: WorkspaceSetup| WorkspaceSpec {
            setup: Some(s),
            ..Default::default()
        };
        let files = |path: &str| {
            setup(WorkspaceSetup {
                files: vec![File {
                    path: path.into(),
                    content: "x".into(),
                }],
                ..Default::default()
            })
        };
        let git = |g: GitRepo| {
            setup(WorkspaceSetup {
                git: vec![g],
                ..Default::default()
            })
        };
        let repo = |r: &str| GitRepo {
            repo: r.into(),
            ..Default::default()
        };
        let https = || repo("https://example.com/r.git");
        let profile = |p: Profile| WorkspaceSpec {
            profile: Some(p),
            ..Default::default()
        };
        let template = || Profile {
            ate_actor_template: "base-template".into(),
            ..Default::default()
        };
        let cases: Vec<(&str, WorkspaceSpec, &str)> = vec![
            (
                "missing file path",
                files(""),
                "spec.setup.files[0]: path is required",
            ),
            (
                "file path escapes",
                files("../etc/passwd"),
                "spec.setup.files[0].path",
            ),
            (
                "file path absolute",
                files("/etc/passwd"),
                "spec.setup.files[0].path",
            ),
            ("file path dot", files("."), "spec.setup.files[0].path"),
            (
                "file path trailing slash",
                files("dir/"),
                "spec.setup.files[0].path",
            ),
            (
                "missing git repo",
                git(GitRepo {
                    r#ref: "main".into(),
                    ..Default::default()
                }),
                "spec.setup.git[0]: repo is required",
            ),
            (
                "ssh git repo",
                git(repo("git@github.com:org/repo.git")),
                "SSH git URLs",
            ),
            (
                "uppercase ssh git repo",
                git(repo("SSH://git@github.com/org/repo.git")),
                "SSH git URLs",
            ),
            (
                "git repo leading dash",
                git(repo("--upload-pack=bad")),
                "spec.setup.git[0].repo",
            ),
            (
                "git ref option injection",
                git(GitRepo {
                    r#ref: "--upload-pack=touch /tmp/pwn".into(),
                    ..https()
                }),
                "spec.setup.git[0].ref",
            ),
            (
                "git ref whitespace",
                git(GitRepo {
                    r#ref: "main ".into(),
                    ..https()
                }),
                "spec.setup.git[0].ref",
            ),
            (
                "negative git depth",
                git(GitRepo {
                    depth: -1,
                    ..https()
                }),
                "spec.setup.git[0].depth",
            ),
            (
                "git name escapes",
                git(GitRepo {
                    name: "../escaped-repo".into(),
                    ..https()
                }),
                "spec.setup.git[0].name",
            ),
            (
                "git name absolute",
                git(GitRepo {
                    name: "/etc/repo".into(),
                    ..https()
                }),
                "spec.setup.git[0].name",
            ),
            (
                "git name dot",
                git(GitRepo {
                    name: "./".into(),
                    ..https()
                }),
                "set dir",
            ),
            (
                "git dir escapes",
                git(GitRepo {
                    dir: "../escape".into(),
                    ..https()
                }),
                "spec.setup.git[0].dir",
            ),
            (
                "git dir absolute",
                git(GitRepo {
                    dir: "/etc".into(),
                    ..https()
                }),
                "spec.setup.git[0].dir",
            ),
            (
                "skills path escapes",
                setup(WorkspaceSetup {
                    skills: Some(SkillsConfig {
                        path: "../outside-skills".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                "spec.setup.skills.path",
            ),
            (
                "invalid profile resources",
                profile(Profile {
                    resources: Some(ResourcesSpec {
                        limits: Some(ResourceList {
                            cpu: "bad".into(),
                            ..Default::default()
                        }),
                    }),
                    ..Default::default()
                }),
                "spec.profile.resources.limits.cpu",
            ),
            (
                "invalid ate_actor_template",
                profile(Profile {
                    ate_actor_template: "Bad_Name".into(),
                    ..Default::default()
                }),
                "spec.profile.ateActorTemplate",
            ),
            (
                "ate_actor_template with image",
                profile(Profile {
                    image: "us-docker.pkg.dev/p/img@sha256:abc".into(),
                    ..template()
                }),
                "cannot be combined with image or resources",
            ),
            (
                "ate_actor_template with resources",
                profile(Profile {
                    resources: Some(ResourcesSpec::default()),
                    ..template()
                }),
                "cannot be combined with image or resources",
            ),
            (
                "invalid setup gateway",
                setup(WorkspaceSetup {
                    gateway: Some(GatewayRef {
                        name: "Bad_Name".into(),
                    }),
                    ..Default::default()
                }),
                "spec.setup.gateway.name",
            ),
        ];
        for (name, spec, want) in cases {
            let ws = Workspace {
                metadata: meta("valid-ws", ""),
                spec: Some(spec),
                ..Default::default()
            };
            let err = validate_workspace(&ws).expect_err(name).to_string();
            assert!(err.contains(want), "{name}: {err}, want {want:?}");
        }
    }

    #[test]
    fn clean_paths() {
        for (input, want) in [
            ("", "."),
            (".", "."),
            ("./", "."),
            ("a/b/../c", "a/c"),
            ("../x", "../x"),
            ("a/../..", ".."),
            ("a//b/", "a/b"),
            ("/a/../..", "/"),
        ] {
            assert_eq!(clean_path(input), want, "{input}");
        }
    }

    #[test]
    fn workspace_path() {
        assert_eq!(TaskSpec::default().workspace_path(), DEFAULT_WORKSPACE_PATH);
        let with = |path: &str| TaskSpec {
            workspace: Some(WorkspaceRef {
                name: "one".into(),
                path: path.into(),
            }),
            ..Default::default()
        };
        assert_eq!(with("").workspace_path(), DEFAULT_WORKSPACE_PATH);
        assert_eq!(with("/opt/code").workspace_path(), "/opt/code");
    }
}
