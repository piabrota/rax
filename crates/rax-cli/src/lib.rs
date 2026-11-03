//! The `rax` command line, a port of AX's `ax` CLI: the same commands, flags,
//! output and tunnels, so scripts written for `ax` work with `rax`.

pub mod args;
pub mod commands;
pub mod format;
pub mod router_tls;
pub mod tunnel;

/// An error to print as `Error: ...` before exiting with status 1.
pub type Error = String;

pub const USAGE: &str = r#"RAX CLI - Autonomous agent execution control

Usage:
  rax [command] [flags]

Available Commands:
  apply -f <file>         Apply resources (tasks, workspaces, models) from a file or stdin
  get tasks               List tasks
  get task <name>         Get a specific task
  get workspaces          List workspaces
  get workspace <name>    Get a specific workspace
  get models              List models
  get model <name>        Get a specific model
  describe task <name>    Show detailed information about a task
  describe workspace <name> Show detailed information about a workspace
  describe model <name>   Show detailed information about a model
  ssh <task-name> [-- cmd] Run a command or shell inside the running task container
                          (Ctrl-C is forwarded; press it twice quickly, or Ctrl-\, to exit locally)
  suspend task <name>     Suspend execution of a task and checkpoint state
  resume task <name>      Resume execution of a suspended task
  delete task <name>      Delete a task
  delete workspace <name> Delete a workspace
  delete model <name>     Delete a model
  ctx, context            Show active Kubernetes context and AX connection
  tunnel <list|stop>      Manage background tunnels to Kubernetes clusters
  version                 Print RAX version

Flags:
  -a, --atespace string       Task atespace scope (default: "default")
  -n, --namespace string      Kubernetes namespace where AX is installed (default: "ax-system")
  --context string            Kubernetes context (defaults to active kubectx / current-context)
  --server string             AX API server address (default: auto-detected from kube context or $AX_SERVER)"#;

/// Runs the CLI on `argv` (without the program name) and returns the exit
/// status.
pub fn run(argv: Vec<String>) -> i32 {
    if argv.is_empty() {
        println!("{USAGE}");
        return 1;
    }
    let opts = args::parse_global_args(&argv);
    if opts.cmd.is_empty() {
        println!("{USAGE}");
        return 1;
    }
    let fail = |e: Error| {
        eprintln!("Error: {e}");
        1
    };

    // Commands that need no server.
    match opts.cmd.as_str() {
        "version" => {
            println!("rax version v1alpha1");
            return 0;
        }
        "help" | "-h" | "--help" => {
            println!("{USAGE}");
            return 0;
        }
        "ctx" | "context" => {
            commands::run_context(&opts.kube_context);
            return 0;
        }
        "tunnel" => return commands::run_tunnel(&opts.args).map_or_else(fail, |_| 0),
        "ssh" => {
            // Answer help and usage errors before resolving the server, which
            // may start a port-forward.
            match args::parse_ssh_args(&opts.args) {
                Ok(parsed) if parsed.help => {
                    println!("{}", args::SSH_USAGE);
                    return 0;
                }
                Ok(_) => {}
                Err(e) => {
                    println!("{}", args::SSH_USAGE);
                    return fail(e);
                }
            }
        }
        _ => {}
    }

    let known = [
        "apply", "get", "describe", "suspend", "resume", "delete", "ssh",
    ];
    if !known.contains(&opts.cmd.as_str()) {
        eprintln!("unknown command: {}", opts.cmd);
        println!("{USAGE}");
        return 1;
    }

    let server = match tunnel::ensure_server_url(tunnel::Options {
        server_url: opts.server.clone(),
        context: opts.kube_context.clone(),
        namespace: opts.namespace.clone(),
        ..Default::default()
    }) {
        Ok(s) => s,
        Err(e) => return fail(e),
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("building the tokio runtime");
    let atespace = opts.atespace.clone();
    let result = rt.block_on(async {
        match opts.cmd.as_str() {
            "apply" => {
                let flag = opts.atespace_explicit.then_some(atespace.as_str());
                commands::run_apply(&server, flag, &opts.args).await
            }
            "get" => commands::run_get(&server, &atespace, &opts.args).await,
            "describe" => commands::run_describe(&server, &atespace, &opts.args).await,
            "suspend" => commands::run_suspend(&server, &atespace, &opts.args).await,
            "resume" => commands::run_resume(&server, &atespace, &opts.args).await,
            "delete" => commands::run_delete(&server, &atespace, &opts.args).await,
            "ssh" => {
                // ssh leaves through process::exit so a blocked stdin read
                // can't hold the runtime open.
                let code = match commands::run_ssh(
                    &server,
                    &atespace,
                    &opts.kube_context,
                    &opts.namespace,
                    &opts.args,
                )
                .await
                {
                    Ok(code) => code,
                    Err(e) => fail(e),
                };
                std::process::exit(code);
            }
            _ => unreachable!(),
        }
    });
    result.map_or_else(fail, |_| 0)
}
