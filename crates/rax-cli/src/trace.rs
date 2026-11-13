//! `rax --trace`: traces one command, as AX's `ax --trace` does.
//!
//! The command gets a sampled root span, whose trace ID goes to stderr, and
//! every AX RPC it makes is a client span under it that sends a W3C
//! `traceparent` (never baggage). Resolving the server address, which can
//! start a port-forward, is a child span too. Without `--trace` the CLI
//! creates no tracer and sends no trace context, so the server's own sampler
//! decides for CLI traffic.
//!
//! Spans are exported over OTLP/gRPC only when `OTEL_EXPORTER_OTLP_ENDPOINT`
//! or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set, unlike rax-server, which
//! exports by default. `OTEL_TRACES_EXPORTER`, `OTEL_SERVICE_NAME` and
//! `OTEL_RESOURCE_ATTRIBUTES` follow rax-server's rules (rax-otelenv). The
//! ended spans are kept and exported in one call when the command ends,
//! waiting at most [`TRACE_SHUTDOWN_TIMEOUT`] and without retries: a retry
//! would come later than the CLI waits, and only hide the real error behind a
//! timeout. A failed export is a warning that names the endpoint.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Once, OnceLock};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use opentelemetry::propagation::{Injector, TextMapPropagator};
use opentelemetry::trace::{
    SpanKind, Status as SpanStatus, TraceContextExt, Tracer as _, TracerProvider as _,
};
use opentelemetry::{Context, KeyValue};
use opentelemetry_otlp::{RetryPolicy, WithTonicConfig as _};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{
    Sampler, SdkTracerProvider, Span, SpanData, SpanExporter as _, SpanProcessor, Tracer,
};
use rax_otelenv::{EXPORTER_OTLP, Exporters, resolve_exporters};
use tonic::body::Body;
use tonic::transport::Channel;

/// How long the CLI waits to export its spans.
pub const TRACE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
/// How long the export may take when Ctrl-C pressed twice leaves a hung
/// `rax ssh` session, which must end at once.
pub const INTERRUPT_FLUSH_TIMEOUT: Duration = Duration::from_millis(200);

const TRACER_NAME: &str = "rax-cli";
const SERVICE_NAME: &str = "rax";
const TRACES_EXPORTER_ENV: &str = "OTEL_TRACES_EXPORTER";
const OTLP_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const OTLP_TRACES_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";

/// The trace of the running command, once [`start`] has made one.
static ACTIVE: OnceLock<Arc<CommandTrace>> = OnceLock::new();

/// The trace of the running command, if it is traced.
pub fn active() -> Option<Arc<CommandTrace>> {
    ACTIVE.get().cloned()
}

/// Keeps the ended spans until the command ends and they are exported at once.
#[derive(Debug, Default, Clone)]
struct EndedSpans(Arc<Mutex<Vec<SpanData>>>);

impl EndedSpans {
    fn take(&self) -> Vec<SpanData> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

impl SpanProcessor for EndedSpans {
    fn on_start(&self, _: &mut Span, _: &Context) {}

    fn on_end(&self, span: SpanData) {
        if span.span_context.is_sampled() {
            self.0.lock().unwrap().push(span);
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
        Ok(())
    }
}

/// Where the spans go when they are exported.
struct Export {
    exporter: opentelemetry_otlp::SpanExporter,
    endpoint: String,
    spans: EndedSpans,
    runtime: tokio::runtime::Handle,
}

pub struct CommandTrace {
    provider: SdkTracerProvider,
    tracer: Tracer,
    /// Holds the command's root span.
    root: Context,
    export: Option<Export>,
    done: Once,
}

/// Starts tracing the command `cmd` when `force` is set and prints the trace
/// ID. `runtime` runs the OTLP exporter.
pub fn start(
    cmd: &str,
    force: bool,
    runtime: &tokio::runtime::Handle,
) -> Option<Arc<CommandTrace>> {
    if !force {
        return None;
    }
    let t = Arc::new(CommandTrace::new(cmd, env_export(runtime)));
    eprintln!("Trace ID: {}", t.root.span().span_context().trace_id());
    let _ = ACTIVE.set(t.clone());
    Some(t)
}

impl CommandTrace {
    fn new(cmd: &str, mut export: Option<Export>) -> Self {
        let resource = rax_otelenv::resource(SERVICE_NAME, []);
        let mut b = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_resource(resource.clone());
        if let Some(e) = &mut export {
            // Spans reach the exporter without the provider, so it needs the
            // resource itself.
            e.exporter.set_resource(&resource);
            b = b.with_span_processor(e.spans.clone());
        }
        let provider = b.build();
        let tracer = provider.tracer(TRACER_NAME);
        let span = tracer.start(format!("rax {cmd}"));
        Self {
            provider,
            tracer,
            root: Context::current_with_span(span),
            export,
            done: Once::new(),
        }
    }

    /// Starts a child span of the command for one step. End it with
    /// [`Step::end`].
    pub fn step(&self, name: &str) -> Step {
        let span = self.tracer.start_with_context(name.to_owned(), &self.root);
        Step(Some(self.root.with_span(span)))
    }

    /// Ends the command span, with `err` as its error, and exports the spans.
    pub fn end(&self, err: Option<&str>) {
        self.end_within(err, TRACE_SHUTDOWN_TIMEOUT);
    }

    /// Like [`CommandTrace::end`], waiting at most `limit` for the export.
    /// Only the first call has an effect.
    pub fn end_within(&self, err: Option<&str>, limit: Duration) {
        self.done.call_once(|| {
            let span = self.root.span();
            record_error(&span, err);
            span.end();
            if let Some(e) = &self.export {
                let spans = e.spans.take();
                // A thread of its own, so this also works from inside the
                // runtime, as when `rax ssh` exits on a double Ctrl-C.
                let result = std::thread::scope(|s| {
                    s.spawn(|| {
                        e.runtime.block_on(async {
                            tokio::time::timeout(limit, e.exporter.export(spans)).await
                        })
                    })
                    .join()
                });
                let err = match result {
                    Ok(Ok(Ok(()))) => None,
                    Ok(Ok(Err(err))) => Some(err.to_string()),
                    Ok(Err(_)) => Some(format!("timed out after {limit:?}")),
                    Err(_) => Some("the exporter panicked".into()),
                };
                if let Some(err) = err {
                    eprintln!(
                        "Warning: could not export the command trace to {}: {err}",
                        e.endpoint
                    );
                }
                let _ = e.exporter.shutdown_with_timeout(limit);
            }
            let _ = self.provider.shutdown_with_timeout(limit);
        });
    }
}

/// A step of the command, as a span under the command span.
pub struct Step(Option<Context>);

impl Step {
    /// A step of an untraced command, which records nothing.
    pub fn none() -> Self {
        Step(None)
    }

    pub fn end(self, err: Option<&str>) {
        if let Some(cx) = self.0 {
            let span = cx.span();
            record_error(&span, err);
            span.end();
        }
    }
}

fn record_error(span: &opentelemetry::trace::SpanRef<'_>, err: Option<&str>) {
    if let Some(err) = err {
        span.add_event(
            "exception",
            vec![KeyValue::new("exception.message", err.to_owned())],
        );
        span.set_status(SpanStatus::error(err.to_owned()));
    }
}

/// The exporter `OTEL_TRACES_EXPORTER` and the OTLP endpoint variables
/// select, or `None` when the spans stay local.
fn env_export(runtime: &tokio::runtime::Handle) -> Option<Export> {
    let value = std::env::var(TRACES_EXPORTER_ENV).unwrap_or_default();
    let (selected, err) = resolve_exporters(
        TRACES_EXPORTER_ENV,
        &value,
        &[EXPORTER_OTLP],
        &Exporters::of(&[EXPORTER_OTLP]),
    );
    if let Some(err) = err {
        eprintln!("Warning: {err}; using {:?}", selected.to_string());
    }
    if !selected.has(EXPORTER_OTLP) {
        return None;
    }
    let endpoint = [OTLP_TRACES_ENDPOINT_ENV, OTLP_ENDPOINT_ENV]
        .iter()
        .filter_map(|v| std::env::var(v).ok())
        .find(|v| !v.is_empty())?;
    // The exporter's channel is bound to the runtime it is built in.
    let _guard = runtime.enter();
    match opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_retry_policy(RetryPolicy::disabled())
        .build()
    {
        Ok(exporter) => Some(Export {
            exporter,
            endpoint,
            spans: EndedSpans::default(),
            runtime: runtime.clone(),
        }),
        Err(e) => {
            eprintln!("Warning: not exporting the command trace to {endpoint}: {e}");
            None
        }
    }
}

/// A channel to the AX server that, while the command is traced, makes each
/// call a client span under the command span and sends its `traceparent`.
#[derive(Clone)]
pub struct Traced {
    inner: Channel,
    trace: Option<Arc<CommandTrace>>,
}

impl Traced {
    pub fn new(inner: Channel) -> Self {
        Self {
            inner,
            trace: active(),
        }
    }
}

struct HeaderInjector<'a>(&'a mut http::HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(k), Ok(v)) = (
            http::HeaderName::from_bytes(key.as_bytes()),
            http::HeaderValue::from_str(&value),
        ) {
            self.0.insert(k, v);
        }
    }
}

type ResponseFuture =
    Pin<Box<dyn Future<Output = Result<http::Response<Body>, tonic::transport::Error>> + Send>>;

impl tower::Service<http::Request<Body>> for Traced {
    type Response = http::Response<Body>;
    type Error = tonic::transport::Error;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        tower::Service::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, mut req: http::Request<Body>) -> Self::Future {
        let Some(t) = &self.trace else {
            return Box::pin(tower::Service::call(&mut self.inner, req));
        };
        // The span is named as otelgrpc names it: package.Service/Method.
        let name = req.uri().path().trim_start_matches('/').to_owned();
        let (service, method) = name.split_once('/').unwrap_or((&name, ""));
        let mut attrs = vec![
            KeyValue::new("rpc.system", "grpc"),
            KeyValue::new("rpc.service", service.to_owned()),
            KeyValue::new("rpc.method", method.to_owned()),
        ];
        if let Some(host) = req.uri().host() {
            attrs.push(KeyValue::new("server.address", host.to_owned()));
        }
        if let Some(port) = req.uri().port_u16() {
            attrs.push(KeyValue::new("server.port", i64::from(port)));
        }
        let span = t
            .tracer
            .span_builder(name.clone())
            .with_kind(SpanKind::Client)
            .with_attributes(attrs)
            .start_with_context(&t.tracer, &t.root);
        let cx = t.root.with_span(span);
        TraceContextPropagator::new().inject_context(&cx, &mut HeaderInjector(req.headers_mut()));
        let call = tower::Service::call(&mut self.inner, req);
        Box::pin(async move {
            let res = call.await;
            let span = cx.span();
            match &res {
                Ok(resp) => {
                    // A failed call answers with its status in the headers
                    // (trailers-only); a successful one in the trailers.
                    let code = resp
                        .headers()
                        .get("grpc-status")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<i64>().ok())
                        .unwrap_or(0);
                    span.set_attribute(KeyValue::new("rpc.grpc.status_code", code));
                    if code != 0 {
                        let msg = resp
                            .headers()
                            .get("grpc-message")
                            .and_then(|v| v.to_str().ok())
                            .map(percent_decode)
                            .unwrap_or_default();
                        span.set_status(SpanStatus::error(msg));
                    }
                }
                Err(e) => span.set_status(SpanStatus::error(e.to_string())),
            }
            span.end();
            res
        })
    }
}

/// Decodes the percent-encoding gRPC uses for `grpc-message`.
fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && let Some(hex) = b.get(i + 1..i + 3)
            && let Ok(v) = u8::from_str_radix(std::str::from_utf8(hex).unwrap_or("-"), 16)
        {
            out.push(v);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_grpc_messages() {
        assert_eq!(
            percent_decode("task%20%22a%22 not found"),
            "task \"a\" not found"
        );
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%e2%9c%93"), "✓");
    }

    #[test]
    fn untraced_commands_make_no_trace() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert!(start("get", false, rt.handle()).is_none());
    }

    #[test]
    fn steps_and_errors_end_once() {
        let spans = EndedSpans::default();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut t = CommandTrace::new("get", None);
        // Keep the spans without an exporter, to look at them.
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_span_processor(spans.clone())
            .build();
        t.tracer = provider.tracer(TRACER_NAME);
        t.root = Context::current_with_span(t.tracer.start("rax get"));
        t.step("resolve ax-server address").end(Some("no tunnel"));
        t.end(Some("boom"));
        t.end(None);
        drop(rt);
        let got = spans.take();
        let names: Vec<_> = got.iter().map(|s| s.name.to_string()).collect();
        assert_eq!(names, ["resolve ax-server address", "rax get"]);
        assert!(
            got.iter()
                .all(|s| matches!(s.status, SpanStatus::Error { .. }))
        );
        assert_eq!(got[0].parent_span_id, got[1].span_context.span_id());
        assert!(got[1].span_context.is_sampled());
    }
}
