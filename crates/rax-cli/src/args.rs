//! Argument parsing, done by hand as AX does so global flags may appear
//! anywhere before `--`.

use crate::Error;

/// The command name, the flags every command shares, and the command's own
/// arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalArgs {
    pub cmd: String,
    pub args: Vec<String>,
    pub atespace: String,
    pub atespace_explicit: bool,
    pub server: String,
    pub kube_context: String,
    pub namespace: String,
}

impl Default for GlobalArgs {
    fn default() -> Self {
        Self {
            cmd: String::new(),
            args: Vec::new(),
            atespace: "default".into(),
            atespace_explicit: false,
            server: String::new(),
            kube_context: String::new(),
            namespace: rax_api::DEFAULT_SYSTEM_NAMESPACE.into(),
        }
    }
}

/// Splits out the command name and the global flags. Flags are not
/// interpreted after `--`, so a command run by `rax ssh <task> -- ...` is
/// passed through intact. The system namespace defaults to
/// `$AX_SYSTEM_NAMESPACE`, then `ax-system`, and `-n`/`--namespace`
/// overrides both.
pub fn parse_global_args(args: &[String]) -> GlobalArgs {
    parse_global_args_with_env(args, std::env::var("AX_SYSTEM_NAMESPACE").ok().as_deref())
}

fn parse_global_args_with_env(args: &[String], env_namespace: Option<&str>) -> GlobalArgs {
    let mut g = GlobalArgs::default();
    if let Some(ns) = env_namespace.filter(|n| !n.is_empty()) {
        g.namespace = ns.into();
    }
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        let mut value = |target: &mut String| {
            if i + 1 < args.len() {
                *target = args[i + 1].clone();
                i += 1;
                true
            } else {
                false
            }
        };
        if arg == "--" {
            g.args.extend_from_slice(&args[i..]);
            break;
        } else if arg == "-a" || arg == "--atespace" {
            if value(&mut g.atespace) {
                g.atespace_explicit = true;
            }
        } else if let Some(v) = arg.strip_prefix("--atespace=") {
            g.atespace = v.into();
            g.atespace_explicit = true;
        } else if arg == "--server" {
            value(&mut g.server);
        } else if let Some(v) = arg.strip_prefix("--server=") {
            g.server = v.into();
        } else if arg == "--context" {
            value(&mut g.kube_context);
        } else if let Some(v) = arg.strip_prefix("--context=") {
            g.kube_context = v.into();
        } else if arg == "-n" || arg == "--namespace" {
            value(&mut g.namespace);
        } else if let Some(v) = arg.strip_prefix("--namespace=") {
            g.namespace = v.into();
        } else if g.cmd.is_empty() && !arg.starts_with('-') {
            g.cmd = arg.into();
        } else {
            g.args.push(arg.into());
        }
        i += 1;
    }
    g
}

pub const SSH_USAGE: &str = r#"Usage:
  rax ssh <task-name> [-- command...]

Run a command inside a running task's container, or /bin/sh when no command is
given. The task must be Running and have spec.debug: true.

Examples:
  rax ssh task123
  rax ssh task123 -- ls -la /workspace
  rax ssh -a my-atespace task123 -- python3 main.py"#;

/// The parsed arguments of `rax ssh`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SshArgs {
    pub task: String,
    pub command: Vec<String>,
    pub help: bool,
}

/// Splits `rax ssh` arguments into the task and the command, `/bin/sh` by
/// default. Everything after the task name is the command, with or without a
/// `--` right after the name; a later `--` belongs to the command.
pub fn parse_ssh_args(args: &[String]) -> Result<SshArgs, Error> {
    let Some(first) = args.first().filter(|a| *a != "--") else {
        return Err("missing task name".into());
    };
    if first == "-h" || first == "--help" {
        return Ok(SshArgs {
            help: true,
            ..Default::default()
        });
    }
    if first.starts_with('-') {
        return Err(format!("unknown flag {first:?}"));
    }
    let mut rest = &args[1..];
    if rest.first().is_some_and(|a| a == "--") {
        rest = &rest[1..];
    }
    let command = if rest.is_empty() {
        vec!["/bin/sh".to_owned()]
    } else {
        rest.to_vec()
    };
    Ok(SshArgs {
        task: first.clone(),
        command,
        help: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn global_args() {
        let base = GlobalArgs::default;
        let cases = [
            (
                v(&["-a", "team", "get", "tasks", "--namespace=ax"]),
                GlobalArgs {
                    cmd: "get".into(),
                    args: v(&["tasks"]),
                    atespace: "team".into(),
                    atespace_explicit: true,
                    namespace: "ax".into(),
                    ..base()
                },
            ),
            (
                v(&["apply", "-f", "resource.yaml"]),
                GlobalArgs {
                    cmd: "apply".into(),
                    args: v(&["-f", "resource.yaml"]),
                    ..base()
                },
            ),
            (
                v(&["apply", "-a", "default"]),
                GlobalArgs {
                    cmd: "apply".into(),
                    atespace_explicit: true,
                    ..base()
                },
            ),
            (
                v(&["apply", "--atespace", "team"]),
                GlobalArgs {
                    cmd: "apply".into(),
                    atespace: "team".into(),
                    atespace_explicit: true,
                    ..base()
                },
            ),
            (
                v(&["-a", "team", "apply", "--atespace=default"]),
                GlobalArgs {
                    cmd: "apply".into(),
                    atespace_explicit: true,
                    ..base()
                },
            ),
            (
                v(&[
                    "ssh",
                    "task123",
                    "--",
                    "cmd",
                    "-n",
                    "foo",
                    "-a",
                    "file",
                    "--atespace",
                    "remote",
                    "--atespace=remote",
                ]),
                GlobalArgs {
                    cmd: "ssh".into(),
                    args: v(&[
                        "task123",
                        "--",
                        "cmd",
                        "-n",
                        "foo",
                        "-a",
                        "file",
                        "--atespace",
                        "remote",
                        "--atespace=remote",
                    ]),
                    ..base()
                },
            ),
            (
                v(&["ssh", "-a", "team", "task123", "--", "cmd", "--context=x"]),
                GlobalArgs {
                    cmd: "ssh".into(),
                    args: v(&["task123", "--", "cmd", "--context=x"]),
                    atespace: "team".into(),
                    atespace_explicit: true,
                    ..base()
                },
            ),
            (
                v(&["--server", "127.0.0.1:1", "--context=kind", "get"]),
                GlobalArgs {
                    cmd: "get".into(),
                    server: "127.0.0.1:1".into(),
                    kube_context: "kind".into(),
                    ..base()
                },
            ),
        ];
        for (args, want) in cases {
            assert_eq!(parse_global_args(&args), want, "{args:?}");
        }
    }

    #[test]
    fn global_args_namespace_precedence() {
        for (env, args, want) in [
            // Defaults to ax-system.
            (None, v(&["ssh", "task123"]), "ax-system"),
            (Some(""), v(&["ssh", "task123"]), "ax-system"),
            // Falls back to AX_SYSTEM_NAMESPACE when -n is omitted.
            (
                Some("env-ax-system"),
                v(&["ssh", "task123"]),
                "env-ax-system",
            ),
            // An explicit -n overrides AX_SYSTEM_NAMESPACE.
            (
                Some("env-ax-system"),
                v(&["-n", "flag-ax-system", "ssh", "task123"]),
                "flag-ax-system",
            ),
            // Even when it names the default.
            (
                Some("env-ax-system"),
                v(&["--namespace=ax-system", "ssh", "task123"]),
                "ax-system",
            ),
        ] {
            assert_eq!(
                parse_global_args_with_env(&args, env).namespace,
                want,
                "{args:?} with env={env:?}"
            );
        }
    }

    #[test]
    fn ssh_args() {
        let ok = |args: &[&str], task: &str, cmd: &[&str]| {
            assert_eq!(
                parse_ssh_args(&v(args)).unwrap(),
                SshArgs {
                    task: task.into(),
                    command: v(cmd),
                    help: false
                },
                "{args:?}"
            );
        };
        assert!(parse_ssh_args(&v(&["--help"])).unwrap().help);
        assert!(parse_ssh_args(&v(&["-h"])).unwrap().help);
        assert!(parse_ssh_args(&[]).is_err());
        assert!(parse_ssh_args(&v(&["--", "ls"])).is_err());
        assert!(parse_ssh_args(&v(&["--verbose", "t"])).is_err());
        ok(&["task123"], "task123", &["/bin/sh"]);
        ok(&["t", "--", "ls", "-la", "/w"], "t", &["ls", "-la", "/w"]);
        ok(&["t", "python3", "main.py"], "t", &["python3", "main.py"]);
        ok(
            &["t", "--", "git", "log", "--", "f"],
            "t",
            &["git", "log", "--", "f"],
        );
        ok(
            &["t", "git", "log", "--", "f"],
            "t",
            &["git", "log", "--", "f"],
        );
        ok(&["t", "--", "git", "--help"], "t", &["git", "--help"]);
    }
}
