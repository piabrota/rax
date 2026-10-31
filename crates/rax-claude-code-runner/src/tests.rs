//! Ports of AX's claude-code-runner config tests.

use super::*;
use rax_api::v1alpha1::{McpRegistry, McpServer, TaskSpec, WorkspaceSpec};

fn config(v: Json) -> Struct {
    serde_json::from_value(v).unwrap()
}

#[test]
fn rejects_bad_configs() {
    for (name, m) in [
        ("missing prompt", json!({"model": "claude-opus-5-5"})),
        ("empty prompt", json!({"prompt": ""})),
        ("non-string prompt", json!({"prompt": 1})),
        ("flag with dashes", json!({"prompt": "p", "--model": "x"})),
        ("kebab-case key", json!({"prompt": "p", "max-turns": 5})),
        (
            "snake_case key",
            json!({"prompt": "p", "allowed_tools": ["Edit"]}),
        ),
        ("PascalCase key", json!({"prompt": "p", "MaxTurns": 5})),
        ("capital run", json!({"prompt": "p", "maxTURNS": 5})),
        ("trailing capital", json!({"prompt": "p", "modelX": "x"})),
        ("flag with equals", json!({"prompt": "p", "model=x": "y"})),
        ("null value", json!({"prompt": "p", "model": null})),
        (
            "nested list",
            json!({"prompt": "p", "allowedTools": [["Bash"]]}),
        ),
        (
            "object in list",
            json!({"prompt": "p", "allowedTools": [{}]}),
        ),
        (
            "bool in list",
            json!({"prompt": "p", "allowedTools": [true]}),
        ),
        ("empty flag name", json!({"prompt": "p", "": "x"})),
        ("leading digit flag", json!({"prompt": "p", "1model": "x"})),
    ] {
        let got = claude_args(Some(&config(m)), None);
        assert!(got.is_err(), "{name}: {got:?}");
    }
    assert!(claude_args(None, None).is_err(), "no config");
}

#[test]
fn minimal() {
    let got = claude_args(
        Some(&config(json!({"prompt": "fix the tests"}))),
        Some(&McpConfig::default()),
    )
    .unwrap();
    assert_eq!(got, ["claude", "--print", "--", "fix the tests"]);
}

#[test]
fn flags() {
    let got = claude_args(
        Some(&config(json!({
            "prompt": "-starts with a dash",
            "model": "claude-opus-5-5",
            "maxTurns": 50,
            "permissionMode": "bypassPermissions",
            "allowedTools": ["Bash(git diff:*)", "Edit"],
            "verbose": true,
            "debug": false,
            "settings": {"includeCoAuthoredBy": false},
        }))),
        None,
    )
    .unwrap();
    // Flags are emitted in sorted key order.
    assert_eq!(
        got,
        [
            "claude",
            "--print",
            "--allowed-tools=Bash(git diff:*)",
            "--allowed-tools=Edit",
            "--max-turns=50",
            "--model=claude-opus-5-5",
            "--permission-mode=bypassPermissions",
            r#"--settings={"includeCoAuthoredBy":false}"#,
            "--verbose",
            "--",
            "-starts with a dash",
        ]
    );
}

#[test]
fn numbers_print_as_go_prints_them() {
    let got = claude_args(
        Some(&config(
            json!({"prompt": "p", "maxBudgetUsd": 2.5, "maxTurns": 1e21}),
        )),
        None,
    )
    .unwrap();
    assert_eq!(
        got[2..4],
        ["--max-budget-usd=2.5", "--max-turns=1000000000000000000000"]
    );
}

#[test]
fn workspace_mcp() {
    let server = |name: &str, endpoint: &str| McpServer {
        name: name.into(),
        endpoint: endpoint.into(),
        ..Default::default()
    };
    let mcp = McpConfig {
        servers: vec![
            server("docs", "https://example.com/mcp"),
            McpServer {
                name: "git".into(),
                command: "git-mcp".into(),
                args: vec!["--repo".into(), ".".into()],
                ..Default::default()
            },
            server("empty", ""),
        ],
        registries: vec![McpRegistry {
            provider: "google".into(),
            servers: vec![
                server("docs", "https://shadowed.example.com/mcp"),
                server("build", "https://build.example.com/mcp"),
            ],
            ..Default::default()
        }],
    };
    let got = claude_args(Some(&config(json!({"prompt": "p"}))), Some(&mcp)).unwrap();
    let cfg = got
        .iter()
        .find_map(|a| a.strip_prefix("--mcp-config="))
        .expect("an --mcp-config flag");
    assert_eq!(
        cfg,
        r#"{"mcpServers":{"build":{"type":"http","url":"https://build.example.com/mcp"},"docs":{"type":"http","url":"https://example.com/mcp"},"git":{"args":["--repo","."],"command":"git-mcp","type":"stdio"}}}"#,
        "byte for byte what AX's json.Marshal writes"
    );
    assert_eq!(got[got.len() - 2..], ["--", "p"]);
}

#[test]
fn mcp_config_escapes_like_go() {
    let mcp = McpConfig {
        servers: vec![McpServer {
            name: "local".into(),
            command: "sh".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let servers = mcp_servers(Some(&mcp));
    assert_eq!(
        serde_json::to_string(&servers).unwrap(),
        r#"{"local":{"args":null,"command":"sh","type":"stdio"}}"#
    );
    assert_eq!(
        go_json(&json!({"q": "a<b>&c"})),
        "{\"q\":\"a\\u003cb\\u003e\\u0026c\"}"
    );
}

#[test]
fn with_claude_command_only_fills_an_empty_command() {
    let cfg = config(json!({"prompt": "p"}));

    let explicit = Task {
        spec: Some(TaskSpec {
            command: vec!["make".into(), "test".into()],
            config: Some(cfg.clone()),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(
        with_claude_command(explicit.clone(), None).unwrap(),
        explicit
    );

    let no_config = Task {
        spec: Some(TaskSpec::default()),
        ..Default::default()
    };
    assert_eq!(
        with_claude_command(no_config.clone(), None).unwrap(),
        no_config
    );

    let task = Task {
        spec: Some(TaskSpec {
            config: Some(cfg),
            ..Default::default()
        }),
        ..Default::default()
    };
    let ws = Workspace {
        spec: Some(WorkspaceSpec {
            mcp: Some(McpConfig {
                servers: vec![McpServer {
                    name: "docs".into(),
                    endpoint: "https://example.com/mcp".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let got = with_claude_command(task, Some(&ws)).unwrap();
    let cmd = &got.spec.unwrap().command;
    assert_eq!(cmd[0], CLAUDE_BINARY);
    assert!(
        cmd.iter().any(|a| a.starts_with("--mcp-config=")),
        "{cmd:?} does not load the workspace MCP servers"
    );
}
