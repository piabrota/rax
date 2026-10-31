//! A port of AX's `claude-code-runner`: a task runner that runs Claude Code
//! configured from the Task itself.
//!
//! When the task leaves `spec.command` empty, the runner runs one headless
//! `claude --print` session in the workspace. `spec.config` holds the prompt
//! and claude's flags, named as in `claude --help` (see [`claude_args`]), and
//! the session is connected to the MCP servers the bound Workspace declares.
//! A task that sets `spec.command` runs it unchanged, exactly as with
//! `rax-task-runner`. Everything else, from workspace setup to supervision
//! and shutdown, is the standard [`rax_task_runner::runner`] lifecycle.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use pbjson_types::value::Kind;
use pbjson_types::{Struct, Value};
use rax_api::v1alpha1::{McpConfig, Task, Workspace};
use regex::Regex;
use serde_json::{Map, Value as Json, json};

/// The Claude Code launcher installed by `Dockerfile.claude-code`.
pub const CLAUDE_BINARY: &str = "claude";

/// The `spec.config` key holding the prompt, the positional argument of
/// `claude [options] [prompt]`. Every other key is a claude flag.
pub const PROMPT_KEY: &str = "prompt";

/// A camelCase config key, e.g. `model` or `maxTurns`. The flag it names is
/// the key with each capital letter turned into a dash and its lowercase,
/// the spelling `claude --help` lists for every flag.
static CONFIG_KEY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z][a-z0-9]*([A-Z][a-z0-9]+)*$").unwrap());

/// The claude flag named by the camelCase key `k`, e.g. `--max-turns` for
/// `maxTurns`.
pub fn flag_name(k: &str) -> String {
    let mut out = String::from("--");
    for c in k.chars() {
        if c.is_ascii_uppercase() {
            out.push('-');
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// The arguments of a headless claude run described by `config` and the
/// Workspace MCP servers in `mcp`.
///
/// Each config key other than `prompt` names a claude flag in camelCase, so
/// `maxTurns` is `--max-turns`, and its value maps to arguments by type: a
/// string or number becomes `--key=value`, true becomes `--key`, false omits
/// the flag, a list repeats `--key=value` for each element, and an object is
/// passed as JSON, as `--settings` and `--mcp-config` accept. claude itself
/// validates the flags and their values.
pub fn claude_args(
    config: Option<&Struct>,
    mcp: Option<&McpConfig>,
) -> Result<Vec<String>, String> {
    let empty = BTreeMap::new();
    let fields: BTreeMap<_, _> = match config {
        Some(c) => c.fields.iter().collect(),
        None => empty,
    };
    let prompt = match fields
        .get(&PROMPT_KEY.to_owned())
        .and_then(|v| v.kind.as_ref())
    {
        Some(Kind::StringValue(p)) if !p.is_empty() => p.clone(),
        _ => return Err("spec.config: prompt must be a non-empty string".into()),
    };

    let mut args = vec![CLAUDE_BINARY.to_owned(), "--print".to_owned()];
    // Flags in sorted key order, as AX emits them.
    for (k, v) in fields.iter().filter(|(k, _)| k.as_str() != PROMPT_KEY) {
        if !CONFIG_KEY.is_match(k) {
            return Err(format!(
                "spec.config: key {k:?} must be a claude flag name in camelCase, e.g. maxTurns"
            ));
        }
        args.extend(flag_values(&flag_name(k), v).map_err(|e| format!("spec.config.{k}: {e}"))?);
    }

    let servers = mcp_servers(mcp);
    if !servers.is_empty() {
        let config = json!({ "mcpServers": servers });
        args.push(format!("--mcp-config={}", go_json(&config)));
    }

    // End option parsing so a prompt that starts with "-" is not taken as a
    // flag.
    args.push("--".into());
    args.push(prompt);
    Ok(args)
}

/// Converts one config value into arguments for `flag`. Values are attached
/// with `=` so variadic flags such as `--allowed-tools` cannot consume the
/// arguments that follow.
fn flag_values(flag: &str, v: &Value) -> Result<Vec<String>, String> {
    match &v.kind {
        Some(Kind::BoolValue(true)) => Ok(vec![flag.to_owned()]),
        Some(Kind::BoolValue(false)) => Ok(vec![]),
        Some(Kind::StructValue(s)) => Ok(vec![format!(
            "{flag}={}",
            serde_json::to_string(&struct_json(s)).map_err(|e| e.to_string())?
        )]),
        Some(Kind::ListValue(list)) => list
            .values
            .iter()
            .map(|elem| {
                scalar(elem)
                    .map(|s| format!("{flag}={s}"))
                    .map_err(|e| format!("list elements must be strings or numbers: {e}"))
            })
            .collect(),
        _ => scalar(v).map(|s| vec![format!("{flag}={s}")]),
    }
}

fn scalar(v: &Value) -> Result<String, String> {
    match &v.kind {
        Some(Kind::StringValue(s)) => Ok(s.clone()),
        // Rust's shortest round-trip form without an exponent, as Go's
        // strconv.FormatFloat(f, 'f', -1, 64).
        Some(Kind::NumberValue(n)) => Ok(format!("{n}")),
        _ => Err(format!("unsupported value {}", describe(v))),
    }
}

/// A value as Go's `%v` prints its `AsInterface()`, for error messages.
fn describe(v: &Value) -> String {
    match &v.kind {
        None | Some(Kind::NullValue(_)) => "<nil>".into(),
        Some(Kind::BoolValue(b)) => b.to_string(),
        Some(Kind::ListValue(l)) => format!(
            "[{}]",
            l.values.iter().map(describe).collect::<Vec<_>>().join(" ")
        ),
        Some(Kind::StructValue(s)) => {
            let fields: BTreeMap<_, _> = s.fields.iter().collect();
            let inner: Vec<_> = fields
                .iter()
                .map(|(k, v)| format!("{k}:{}", describe(v)))
                .collect();
            format!("map[{}]", inner.join(" "))
        }
        Some(Kind::NumberValue(n)) => format!("{n}"),
        Some(Kind::StringValue(s)) => s.clone(),
    }
}

/// A Struct as compact JSON with its keys sorted, as protojson writes it.
fn struct_json(s: &Struct) -> Json {
    let fields: BTreeMap<_, _> = s.fields.iter().collect();
    Json::Object(
        fields
            .into_iter()
            .map(|(k, v)| (k.clone(), value_json(v)))
            .collect::<Map<_, _>>(),
    )
}

fn value_json(v: &Value) -> Json {
    match &v.kind {
        None | Some(Kind::NullValue(_)) => Json::Null,
        Some(Kind::BoolValue(b)) => Json::Bool(*b),
        Some(Kind::StringValue(s)) => Json::String(s.clone()),
        // Whole numbers print without a fraction, as protojson prints them.
        Some(Kind::NumberValue(n)) if n.fract() == 0.0 && n.abs() < 1e15 => json!(*n as i64),
        Some(Kind::NumberValue(n)) => json!(n),
        Some(Kind::ListValue(l)) => Json::Array(l.values.iter().map(value_json).collect()),
        Some(Kind::StructValue(s)) => struct_json(s),
    }
}

/// JSON as Go's encoding/json writes it: compact, with `<`, `>`, `&`, U+2028
/// and U+2029 escaped. Those characters only occur inside strings, so they
/// are safe to replace in the encoded text.
fn go_json(v: &Json) -> String {
    serde_json::to_string(v)
        .expect("JSON values always encode")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}

/// Converts the Workspace's MCP servers, both the static ones and those
/// listed under its registries, into the `mcpServers` object of Claude Code's
/// MCP config, keyed and ordered by name. A server with an endpoint is
/// reached over streamable HTTP; one with a command is started over stdio.
/// Servers with neither, or with a name already taken, are skipped.
pub fn mcp_servers(mcp: Option<&McpConfig>) -> Map<String, Json> {
    let mut servers = BTreeMap::new();
    let Some(mcp) = mcp else {
        return Map::new();
    };
    let all = mcp
        .servers
        .iter()
        .chain(mcp.registries.iter().flat_map(|r| r.servers.iter()));
    for srv in all {
        let name = srv.name.as_str();
        if name.is_empty() || servers.contains_key(name) {
            tracing::warn!(name, "skipping MCP server with a missing or duplicate name");
            continue;
        }
        let entry = if !srv.endpoint.is_empty() {
            json!({ "type": "http", "url": srv.endpoint })
        } else if !srv.command.is_empty() {
            // Go encodes an empty argument list as null.
            let args = if srv.args.is_empty() {
                Json::Null
            } else {
                json!(srv.args)
            };
            json!({ "args": args, "command": srv.command, "type": "stdio" })
        } else {
            tracing::warn!(
                name,
                "skipping MCP server with neither an endpoint nor a command"
            );
            continue;
        };
        servers.insert(name.to_owned(), entry);
    }
    servers.into_iter().collect()
}

/// The task with `spec.command` set to the claude invocation described by
/// `spec.config` and the MCP servers of `workspace`. A task that already sets
/// `spec.command`, or has no `spec.config`, is returned unchanged.
pub fn with_claude_command(mut task: Task, workspace: Option<&Workspace>) -> Result<Task, String> {
    let Some(spec) = task.spec.as_mut() else {
        return Ok(task);
    };
    if !spec.command.is_empty() || spec.config.is_none() {
        return Ok(task);
    }
    let mcp = workspace
        .and_then(|w| w.spec.as_ref())
        .and_then(|s| s.mcp.as_ref());
    spec.command = claude_args(spec.config.as_ref(), mcp)?;
    Ok(task)
}

#[cfg(test)]
mod tests;
