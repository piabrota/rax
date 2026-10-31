//! `rax-server`: the RAX control plane binary.

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use rax_server::controller::{ControllerOptions, SubstrateActors, TaskReconciler};
use rax_server::pods::{KubeClient, PodOptions, PodRunner};
use rax_server::{AxService, ServerOptions, router};
use rax_store::{
    Locker, MemoryLocker, MemoryStore, PostgresOptions, PostgresStore, Store, describe_url,
};
use rax_substrate::ClientOptions;

/// Serves the AX gRPC API and `/healthz` on one port.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Listen address. Like Go, `:8080` listens on every interface.
    #[arg(long, env = "ADDR", default_value = "0.0.0.0:8080")]
    addr: String,

    /// PostgreSQL connection URL. libpq variables such as PGPASSWORD fill in
    /// what it leaves out. `memory://` keeps everything in this process, for
    /// local runs and tests.
    #[arg(
        long,
        env = "AX_STORE",
        default_value = "postgres://ax@localhost:5432/ax?sslmode=disable"
    )]
    store: String,

    /// Agent Substrate control API endpoint.
    #[arg(long, default_value = rax_substrate::DEFAULT_TARGET)]
    substrate_endpoint: String,

    /// Authority and TLS server name for the Substrate endpoint.
    #[arg(long, default_value = "api.ate-system.svc")]
    substrate_authority: String,

    /// Bearer token file for Substrate. Defaults to SUBSTRATE_TOKEN_FILE or
    /// the mounted service account token.
    #[arg(long, default_value = "", hide_default_value = true)]
    substrate_token_file: String,

    /// CA PEM file for Substrate TLS. Defaults to SUBSTRATE_CA_FILE or the
    /// mounted trust bundle.
    #[arg(long, default_value = "", hide_default_value = true)]
    substrate_ca_file: String,

    /// Skip verifying Substrate's TLS certificate.
    #[arg(long)]
    substrate_insecure_tls: bool,

    /// Connect to Substrate over plaintext gRPC.
    #[arg(long)]
    substrate_plaintext: bool,

    /// Default Substrate ActorTemplate, as `name` or `atespace/name`.
    #[arg(long, default_value = "default-template")]
    template: String,

    /// Atespace of the default ActorTemplate.
    #[arg(long, default_value = rax_api::DEFAULT_SYSTEM_NAMESPACE)]
    template_atespace: String,

    /// Container image for tasks that do not set spec.image.
    #[arg(long, env = "AX_DEFAULT_TASK_IMAGE", default_value = rax_api::DEFAULT_TASK_IMAGE)]
    default_task_image: String,

    /// Where tasks run: `substrate` (AX's actors on Agent Substrate) or `pod`
    /// (one Kubernetes pod per run, in --pod-namespace, for clusters without
    /// Substrate).
    #[arg(long, env = "RAX_RUNNER", default_value = "substrate", value_parser = ["substrate", "pod"])]
    runner: String,

    /// The namespace task pods run in (`--runner=pod`).
    #[arg(long, default_value = "ax-tasks")]
    pod_namespace: String,

    /// A Secret in --pod-namespace whose keys become every task pod's
    /// environment. Repeat for more. Tasks cannot name Secrets themselves.
    #[arg(long = "pod-env-from-secret")]
    pod_env_from_secrets: Vec<String>,

    /// The task pods' service account. Empty mounts no token.
    #[arg(long, default_value = "")]
    pod_service_account: String,

    /// Limits for task pods that set none.
    #[arg(long, default_value = "500m")]
    pod_cpu_limit: String,
    #[arg(long, default_value = "256Mi")]
    pod_memory_limit: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tokio_postgres=warn".into()),
        )
        .init();
    if let Err(e) = run(Args::parse()).await {
        tracing::error!("{e}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let (store, locker): (Arc<dyn Store>, Arc<dyn Locker>) = if args.store == "memory://" {
        tracing::warn!(
            "using the in-memory store: state is lost on exit and not shared between replicas"
        );
        (Arc::new(MemoryStore::new()), Arc::new(MemoryLocker::new()))
    } else {
        tracing::info!(store = describe_url(&args.store), "opening store");
        let pg = tokio::time::timeout(
            Duration::from_secs(60),
            PostgresStore::open(&args.store, PostgresOptions::default()),
        )
        .await
        .map_err(|_| format!("opening store {}: timed out", describe_url(&args.store)))?
        .map_err(|e| format!("opening store {}: {e}", describe_url(&args.store)))?;
        let pg = Arc::new(pg);
        (pg.clone(), pg)
    };

    let mut opts = ServerOptions {
        locker: Some(locker),
        ..Default::default()
    };
    if args.runner == "pod" {
        tracing::info!(
            namespace = args.pod_namespace,
            default_task_image = args.default_task_image,
            "running tasks as pods"
        );
        let runner = Arc::new(PodRunner::new(
            KubeClient::in_cluster().map_err(|e| format!("the Kubernetes API: {e}"))?,
            PodOptions {
                namespace: args.pod_namespace,
                default_task_image: args.default_task_image,
                env_from_secrets: args.pod_env_from_secrets,
                service_account: args.pod_service_account,
                default_cpu_limit: args.pod_cpu_limit,
                default_memory_limit: args.pod_memory_limit,
            },
        ));
        opts.reconciler = Some(runner.clone());
        opts.actors = Some(runner);
        return serve(&args.addr, AxService::new(store, opts)).await;
    }

    tracing::info!(
        substrate_endpoint = args.substrate_endpoint,
        template = args.template,
        default_task_image = args.default_task_image,
        "connecting to substrate"
    );
    let client = rax_substrate::Client::connect(ClientOptions {
        target: args.substrate_endpoint,
        authority: args.substrate_authority,
        token_file: args.substrate_token_file,
        ca_file: args.substrate_ca_file,
        insecure_tls: args.substrate_insecure_tls,
        plaintext: args.substrate_plaintext,
    });
    // As in AX, the server needs Substrate: without it no task could run.
    let client = client.map_err(|e| format!("initializing substrate client: {e}"))?;
    let reconciler = TaskReconciler::new(
        client.clone(),
        ControllerOptions {
            default_template: args.template,
            default_template_atespace: args.template_atespace,
            default_task_image: args.default_task_image,
            router_addr: std::env::var("ATENET_ROUTER_ADDR").unwrap_or_default(),
            ..Default::default()
        },
    );
    opts.reconciler = Some(Arc::new(reconciler));
    opts.actors = Some(Arc::new(SubstrateActors::new(client)));
    serve(&args.addr, AxService::new(store, opts)).await
}

async fn serve(addr: &str, service: AxService) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(listen_addr(addr)).await?;
    tracing::info!(addr = %listener.local_addr()?, "starting rax-server");
    axum::serve(listener, router(service))
        .with_graceful_shutdown(shutdown())
        .await?;
    tracing::info!("shut down rax-server");
    Ok(())
}

/// Accepts Go's host-less `:port` form, which AX's manifests pass as
/// `--addr=:8080`, as well as anything `TcpListener::bind` takes.
fn listen_addr(addr: &str) -> String {
    match addr.strip_prefix(':') {
        Some(port) => format!("0.0.0.0:{port}"),
        None => addr.to_string(),
    }
}

async fn shutdown() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("installing the SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}

#[cfg(test)]
mod tests {
    use super::listen_addr;

    #[test]
    fn listen_addr_accepts_go_port_form() {
        assert_eq!(listen_addr(":8080"), "0.0.0.0:8080");
        assert_eq!(listen_addr("127.0.0.1:8080"), "127.0.0.1:8080");
        assert_eq!(listen_addr("[::]:8080"), "[::]:8080");
    }
}
