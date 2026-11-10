//! OpenTelemetry for rax-server, as AX's ax-server sets it up: a tracer
//! provider, a meter provider and a logger provider for structured events,
//! all exporting over OTLP/gRPC; a Prometheus handler for the same metrics;
//! and JSON log lines stamped with the current trace context.
//!
//! Configuration follows the OpenTelemetry environment variables:
//! `OTEL_EXPORTER_OTLP_ENDPOINT` (and the per-signal variants) choose the
//! collector; `OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER` and
//! `OTEL_LOGS_EXPORTER` choose the exporters; `OTEL_TRACES_SAMPLER` and
//! `OTEL_TRACES_SAMPLER_ARG` override the default sampler; and
//! `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` override the resource.
//! A value rax-server does not understand is logged and ignored, which keeps
//! rax-server's default.

mod config;
mod event;
mod log;

use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter, WithExportConfig as _};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::resource::{EnvResourceDetector, TelemetryResourceDetector};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_semantic_conventions::resource::{SERVICE_INSTANCE_ID, SERVICE_NAME};

pub use config::{
    EXPORTER_CONSOLE, EXPORTER_NONE, EXPORTER_OTLP, EXPORTER_PROMETHEUS, Exporters, SamplerSpec,
    parse_sampler, resolve_exporters,
};
pub use event::{EVENT_SCOPE, Event, emit, set_event_output};
pub use log::{JsonFormat, subscriber};

use config::{
    LOGS_EXPORTER_ENV, METRICS_EXPORTER_ENV, TRACES_EXPORTER_ENV, resolve_exporters_env,
    resolve_sampler,
};

/// The share of new traces sampled unless `OTEL_TRACES_SAMPLER` validly says
/// otherwise. The default sampler is parent based, so a caller's sampling
/// decision, carried in `traceparent`, is followed.
pub const DEFAULT_SAMPLE_RATIO: f64 = 0.1;

/// The default sampler: parent based, sampling [`DEFAULT_SAMPLE_RATIO`] of
/// new traces.
pub fn default_sampler() -> SamplerSpec {
    SamplerSpec::ParentBased(Box::new(SamplerSpec::TraceIdRatio(DEFAULT_SAMPLE_RATIO)))
}

/// A failure building a provider or an exporter.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct TelemetryError(String);

/// The providers [`init`] installed as the OpenTelemetry globals.
pub struct Providers {
    pub tracer: SdkTracerProvider,
    pub meter: SdkMeterProvider,
    /// `None` when `OTEL_LOGS_EXPORTER` does not select otlp. [`emit`] then
    /// writes events to stdout.
    pub logger: Option<SdkLoggerProvider>,
    registry: prometheus::Registry,
}

/// Builds the providers for `service_name` and installs them as the
/// OpenTelemetry globals with the W3C trace context propagator. SDK errors
/// go to the log, as JSON lines like the rest. Call [`Providers::shutdown`]
/// on the result before the process exits. Must run inside a Tokio runtime,
/// which the OTLP exporters use.
///
/// Metrics are pushed every `OTEL_METRIC_EXPORT_INTERVAL` (60 s by default)
/// while `OTEL_METRICS_EXPORTER` selects otlp. Traces and events are exported
/// only once something records them.
pub fn init(service_name: &str) -> Result<Providers, TelemetryError> {
    let res = new_resource(service_name);
    let tracer = new_tracer_provider(&res)?;
    let (meter, registry) = new_meter_provider(&res)?;
    let logger = new_logger_provider(&res)?;

    opentelemetry::global::set_tracer_provider(tracer.clone());
    opentelemetry::global::set_meter_provider(meter.clone());
    // Only trace context crosses process boundaries. Baggage from a caller is
    // not trusted, so it is not propagated.
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    Ok(Providers {
        tracer,
        meter,
        logger,
        registry,
    })
}

impl Providers {
    /// The meter provider's metrics, together with process metrics, in the
    /// Prometheus text exposition format. The process metrics are served
    /// here only, not pushed over OTLP.
    pub fn metrics_text(&self) -> String {
        Providers::text(&self.registry)
    }

    /// An HTTP router serving [`Providers::metrics_text`] on `GET /metrics`.
    pub fn metrics_router(&self) -> axum::Router {
        let registry = self.registry.clone();
        axum::Router::new().route(
            "/metrics",
            axum::routing::get(move || {
                let registry = registry.clone();
                async move {
                    (
                        [(http::header::CONTENT_TYPE, prometheus::TEXT_FORMAT)],
                        Providers::text(&registry),
                    )
                }
            }),
        )
    }

    fn text(registry: &prometheus::Registry) -> String {
        use prometheus::Encoder as _;
        let mut buf = Vec::new();
        if let Err(e) = prometheus::TextEncoder::new().encode(&registry.gather(), &mut buf) {
            tracing::warn!(error = %e, "encoding metrics");
        }
        String::from_utf8(buf).unwrap_or_default()
    }

    /// Flushes and stops the providers, and stops sending events over OTLP.
    /// It blocks while the exporters flush, at most `timeout` each.
    pub fn shutdown(&self, timeout: Duration) -> Result<(), TelemetryError> {
        if let Some(sink) = event::SINK.write().unwrap().as_mut() {
            sink.logger = None;
        }
        let mut errs = Vec::new();
        if let Err(e) = self.tracer.shutdown_with_timeout(timeout) {
            errs.push(format!("traces: {e}"));
        }
        if let Err(e) = self.meter.shutdown_with_timeout(timeout) {
            errs.push(format!("metrics: {e}"));
        }
        if let Some(logger) = &self.logger
            && let Err(e) = logger.shutdown_with_timeout(timeout)
        {
            errs.push(format!("events: {e}"));
        }
        if errs.is_empty() {
            Ok(())
        } else {
            Err(TelemetryError(errs.join("; ")))
        }
    }
}

/// Describes the process. `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES`
/// override the defaults; the bundled manifests set `service.instance.id` to
/// the pod UID and add the `k8s.*` attributes there.
fn new_resource(service_name: &str) -> Resource {
    let mut b = Resource::builder_empty()
        .with_detector(Box::new(TelemetryResourceDetector))
        .with_schema_url(
            [KeyValue::new(SERVICE_NAME, service_name.to_string())],
            opentelemetry_semantic_conventions::SCHEMA_URL,
        );
    // Without OTEL_RESOURCE_ATTRIBUTES, the hostname tells replicas apart.
    if let Some(host) = hostname() {
        b = b.with_attribute(KeyValue::new(SERVICE_INSTANCE_ID, host));
    }
    b = b.with_detector(Box::new(EnvResourceDetector::new()));
    if let Ok(name) = std::env::var("OTEL_SERVICE_NAME")
        && !name.is_empty()
    {
        b = b.with_attribute(KeyValue::new(SERVICE_NAME, name));
    }
    b.build()
}

fn hostname() -> Option<String> {
    let h = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())?;
    let h = h.trim();
    (!h.is_empty()).then(|| h.to_string())
}

fn new_tracer_provider(res: &Resource) -> Result<SdkTracerProvider, TelemetryError> {
    let sampler = resolve_sampler(default_sampler());
    let mut b = SdkTracerProvider::builder()
        .with_resource(res.clone())
        .with_sampler(sampler.to_sdk());
    let export = resolve_exporters_env(
        TRACES_EXPORTER_ENV,
        &[EXPORTER_OTLP],
        &Exporters::of(&[EXPORTER_OTLP]),
    )
    .has(EXPORTER_OTLP);
    if export {
        let exp = SpanExporter::builder()
            .with_tonic()
            .with_protocol(opentelemetry_otlp::Protocol::Grpc)
            .build()
            .map_err(|e| TelemetryError(format!("creating OTLP trace exporter: {e}")))?;
        b = b.with_batch_exporter(exp);
    }
    // Without an exporter the provider still samples and propagates context,
    // so traces through rax-server stay whole.
    tracing::info!(export, sampler = %sampler, "tracing initialized");
    Ok(b.build())
}

/// A meter provider with a Prometheus reader, which `/metrics` serves, and
/// an OTLP periodic reader while `OTEL_METRICS_EXPORTER` selects otlp. The
/// variable controls only the push: prometheus or none turns it off, and
/// `--metrics-addr` controls `/metrics`.
fn new_meter_provider(
    res: &Resource,
) -> Result<(SdkMeterProvider, prometheus::Registry), TelemetryError> {
    let registry = prometheus::Registry::new();
    #[cfg(target_os = "linux")]
    registry
        .register(Box::new(
            prometheus::process_collector::ProcessCollector::for_self(),
        ))
        .map_err(|e| TelemetryError(format!("registering process metrics: {e}")))?;
    let prom = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .map_err(|e| TelemetryError(format!("creating Prometheus exporter: {e}")))?;
    let mut b = SdkMeterProvider::builder()
        .with_resource(res.clone())
        .with_reader(prom);
    let push = resolve_exporters_env(
        METRICS_EXPORTER_ENV,
        &[EXPORTER_OTLP, EXPORTER_PROMETHEUS],
        &Exporters::of(&[EXPORTER_OTLP, EXPORTER_PROMETHEUS]),
    )
    .has(EXPORTER_OTLP);
    if push {
        let exp = MetricExporter::builder()
            .with_tonic()
            .with_protocol(opentelemetry_otlp::Protocol::Grpc)
            .build()
            .map_err(|e| TelemetryError(format!("creating OTLP metric exporter: {e}")))?;
        b = b.with_reader(PeriodicReader::builder(exp).build());
    }
    tracing::info!(export = push, "metrics initialized");
    Ok((b.build(), registry))
}

/// The provider for structured events, or `None` when `OTEL_LOGS_EXPORTER`
/// does not select otlp. It also sets whether [`emit`] writes events to
/// stdout beside OTLP (console). rax-server's own log lines go to stdout,
/// not through this provider.
///
/// The default is none rather than the specification's otlp, as in AX and
/// Agent Substrate: events stay on stdout, where they are never lost, until
/// an operator points them at a collector.
fn new_logger_provider(res: &Resource) -> Result<Option<SdkLoggerProvider>, TelemetryError> {
    use opentelemetry::logs::LoggerProvider as _;
    let selected = resolve_exporters_env(
        LOGS_EXPORTER_ENV,
        &[EXPORTER_OTLP, EXPORTER_CONSOLE],
        &Exporters::default(),
    );
    let console = selected.has(EXPORTER_CONSOLE);
    if !selected.has(EXPORTER_OTLP) {
        tracing::info!(exporter = %selected, "OTLP logs export disabled");
        *event::SINK.write().unwrap() = Some(event::Sink {
            logger: None,
            console,
        });
        return Ok(None);
    }
    let exp = LogExporter::builder()
        .with_tonic()
        .with_protocol(opentelemetry_otlp::Protocol::Grpc)
        .build()
        .map_err(|e| TelemetryError(format!("creating OTLP log exporter: {e}")))?;
    // Batched, so an unreachable collector never blocks the caller emitting.
    let lp = SdkLoggerProvider::builder()
        .with_resource(res.clone())
        .with_batch_exporter(exp)
        .build();
    *event::SINK.write().unwrap() = Some(event::Sink {
        logger: Some(lp.logger(EVENT_SCOPE)),
        console,
    });
    tracing::info!(exporter = %selected, "OTLP logs export initialized");
    Ok(Some(lp))
}
