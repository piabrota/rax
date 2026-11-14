//! Spans and duration histograms for gRPC calls, as AX records them with
//! otelgrpc's stats handlers: a server span for each AX RPC rax-server
//! serves, and a client span for each call it makes to Substrate. Both use
//! the global tracer and meter providers and propagate only `traceparent`,
//! never baggage.
//!
//! Spans are named `package.Service/Method`. `rpc.server.call.duration` and
//! `rpc.client.call.duration` are histograms in seconds labelled with
//! `rpc.system.name`, `rpc.method` and `rpc.response.status_code`; the
//! client one has no `server.address` or `server.port`, whose values would
//! be peer IP addresses.

use std::future::Future;
use std::sync::OnceLock;
use std::time::Instant;

use opentelemetry::metrics::Histogram;
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::{
    FutureExt as _, SpanKind, Status as SpanStatus, TraceContextExt as _, Tracer as _,
};
use opentelemetry::{Context, KeyValue, global};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tonic::Code;
use tonic::metadata::{KeyRef, MetadataKey, MetadataMap, MetadataValue};

/// The instrumentation scope of the spans and histograms.
pub const SCOPE: &str =
    "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc";

/// The boundaries, in seconds, of both duration histograms. otelgrpc's stop
/// at 10 s; a resume can wait for a snapshot restore and the 15 s workspace
/// readiness timeout, so these go to 60 s, with boundaries on each side of
/// 15 s.
pub const RPC_DURATION_BUCKETS: [f64; 12] = [
    0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 15.0, 20.0, 30.0, 60.0,
];

fn histogram(
    name: &'static str,
    cell: &'static OnceLock<Histogram<f64>>,
) -> &'static Histogram<f64> {
    cell.get_or_init(|| {
        global::meter(SCOPE)
            .f64_histogram(name)
            .with_unit("s")
            .with_description("Measures the duration of a gRPC call.")
            .with_boundaries(RPC_DURATION_BUCKETS.to_vec())
            .build()
    })
}

static SERVER_DURATION: OnceLock<Histogram<f64>> = OnceLock::new();
static CLIENT_DURATION: OnceLock<Histogram<f64>> = OnceLock::new();

/// The canonical name of a gRPC status code, such as `NOT_FOUND`.
pub fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

struct MetadataExtractor<'a>(&'a MetadataMap);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|k| match k {
                KeyRef::Ascii(k) => Some(k.as_str()),
                KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

struct MetadataInjector<'a>(&'a mut MetadataMap);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(k), Ok(v)) = (
            MetadataKey::from_bytes(key.as_bytes()),
            MetadataValue::try_from(value.as_str()),
        ) {
            self.0.insert(k, v);
        }
    }
}

fn attributes(method: &str, code: Option<Code>) -> Vec<KeyValue> {
    let mut attrs = vec![
        KeyValue::new("rpc.system.name", "grpc"),
        KeyValue::new("rpc.method", method.to_owned()),
    ];
    if let Some(code) = code {
        attrs.push(KeyValue::new("rpc.response.status_code", code_name(code)));
    }
    attrs
}

/// Runs the handler of one served RPC, `method` as `package.Service/Method`,
/// in a server span that is a child of the caller's `traceparent`, and
/// records its duration. Calls made while `handler` runs, such as Substrate
/// calls through [`client_call`], are children of the span.
///
/// Only call this for methods the server registers: otelgrpc's handler
/// records no others, so a client cannot make new span names or metric
/// series.
pub async fn server_call<T, F>(
    method: &str,
    metadata: &MetadataMap,
    handler: F,
) -> Result<T, tonic::Status>
where
    F: Future<Output = Result<T, tonic::Status>>,
{
    let parent = TraceContextPropagator::new().extract(&MetadataExtractor(metadata));
    let tracer = global::tracer(SCOPE);
    let span = tracer
        .span_builder(method.to_owned())
        .with_kind(SpanKind::Server)
        .with_attributes(attributes(method, None))
        .start_with_context(&tracer, &parent);
    let cx = parent.with_span(span);
    let start = Instant::now();
    let out = handler.with_context(cx.clone()).await;
    let code = out.as_ref().map_or_else(|s| s.code(), |_| Code::Ok);
    finish(
        &cx,
        code,
        out.as_ref().err().map(|s| s.message()),
        server_error(code),
    );
    histogram("rpc.server.call.duration", &SERVER_DURATION).record(
        start.elapsed().as_secs_f64(),
        &attributes(method, Some(code)),
    );
    out
}

/// Makes one client call, `method` as `package.Service/Method`, in a client
/// span under the current context, sending its `traceparent` with `req`,
/// and records its duration. Every code other than OK marks the span as an
/// error, even an expected one such as the NotFound a delete polls for.
pub async fn client_call<R, T, F, Fut>(
    method: &str,
    mut req: tonic::Request<R>,
    call: F,
) -> Result<T, tonic::Status>
where
    F: FnOnce(tonic::Request<R>) -> Fut,
    Fut: Future<Output = Result<T, tonic::Status>>,
{
    let parent = Context::current();
    let tracer = global::tracer(SCOPE);
    let span = tracer
        .span_builder(method.to_owned())
        .with_kind(SpanKind::Client)
        .with_attributes(attributes(method, None))
        .start_with_context(&tracer, &parent);
    let cx = parent.with_span(span);
    TraceContextPropagator::new().inject_context(&cx, &mut MetadataInjector(req.metadata_mut()));
    let start = Instant::now();
    let out = call(req).with_context(cx.clone()).await;
    let code = out.as_ref().map_or_else(|s| s.code(), |_| Code::Ok);
    finish(
        &cx,
        code,
        out.as_ref().err().map(|s| s.message()),
        code != Code::Ok,
    );
    histogram("rpc.client.call.duration", &CLIENT_DURATION).record(
        start.elapsed().as_secs_f64(),
        &attributes(method, Some(code)),
    );
    out
}

/// Whether a served call's code makes its span an error, as the semantic
/// conventions for gRPC servers have it: codes that point at the server.
fn server_error(code: Code) -> bool {
    matches!(
        code,
        Code::Unknown
            | Code::DeadlineExceeded
            | Code::Unimplemented
            | Code::Internal
            | Code::Unavailable
            | Code::DataLoss
    )
}

fn finish(cx: &Context, code: Code, message: Option<&str>, error: bool) {
    let span = cx.span();
    span.set_attribute(KeyValue::new("rpc.response.status_code", code_name(code)));
    if error {
        span.set_status(SpanStatus::error(message.unwrap_or_default().to_owned()));
    }
    span.end();
}
