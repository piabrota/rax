//! Validation the API server applies before saving a resource.
//!
//! A resource's name and atespace become Substrate resource names (the task's
//! actor and ActorTemplate, and the atespace itself), which must be lowercase
//! RFC 1123 labels. Checking them at apply time turns an asynchronous
//! ActorCreationFailed condition into an immediate error.

use std::sync::LazyLock;

use regex::Regex;

use crate::substrate::ateapi::v1::{EgressRule, HttpRuleEffects, Ports};
use crate::v1alpha1::{Gateway, Model, ObjectMeta, Task, TaskSpec, Workspace};

/// A problem that makes a resource unusable, phrased for the person who wrote
/// the manifest.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ValidationError(String);

impl ValidationError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }

    pub(crate) fn prefixed(self, field: &str) -> Self {
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
/// after validation, such as metadata.atespace and metadata.createTime.
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
    if let Some(spec) = &w.spec {
        for (i, f) in spec.files.iter().enumerate() {
            if f.path.is_empty() {
                return Err(ValidationError::new(format!(
                    "spec.files[{i}]: path is required"
                )));
            }
        }
    }
    Ok(())
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
    if spec
        .and_then(|s| s.gateway.as_ref())
        .is_some_and(|r| r.name.is_empty())
    {
        return Err(ValidationError::new("spec.gateway: name is required"));
    }
    if let Some(spec) = spec {
        validate_task_config(spec)?;
    }
    validate_task_size(t)?;
    crate::validate_resources(spec.and_then(|s| s.resources.as_ref()))
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
    use crate::v1alpha1::{EnvVar, File, GatewayRef, WorkspaceRef, WorkspaceSpec};

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
                "spec.gateway: name is required",
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
                "oversized command",
                Some(TaskSpec {
                    command: vec!["sh".into(), "-c".into(), "z".repeat(MAX_ENV_VALUE_SIZE)],
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
    fn workspace_files_need_a_path() {
        let ws = Workspace {
            metadata: meta("valid-ws", ""),
            spec: Some(WorkspaceSpec {
                files: vec![File {
                    path: "".into(),
                    content: "missing path".into(),
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = validate_workspace(&ws).unwrap_err().to_string();
        assert!(err.contains("spec.files[0]: path is required"), "{err}");
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
