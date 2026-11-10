//! Ports of AX's telemetry tests: rax-server's OpenTelemetry setup against an
//! in-process OTLP/gRPC collector.
//!
//! The providers, the event sink and the environment are process-wide, so
//! the tests take turns.

use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use opentelemetry::logs::Severity;
use opentelemetry::trace::{
    Span as _, SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId, TraceState,
    Tracer as _, TracerProvider as _,
};
use opentelemetry::{Context, KeyValue, global};
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{KeyValue as PbKeyValue, any_value};
use rax_server::telemetry::{self, DEFAULT_SAMPLE_RATIO, Event, Providers};
use serde_json::Value;

/// A shared buffer the log subscriber and the event sink write to.
#[derive(Clone, Default)]
struct Buf(Arc<Mutex<Vec<u8>>>);

impl Write for Buf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Buf {
    fn take(&self) -> String {
        String::from_utf8(std::mem::take(&mut *self.0.lock().unwrap())).unwrap()
    }
}

/// The log lines in `out` that carry an event attribute.
fn stdout_events(out: &str) -> Vec<serde_json::Map<String, Value>> {
    out.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| match serde_json::from_str::<Value>(l) {
            Ok(Value::Object(m)) => m,
            _ => panic!("log line is not a JSON object: {l}"),
        })
        .filter(|m| m.contains_key("event"))
        .collect()
}

static TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Installs the JSON log subscriber once, writing to the returned buffer,
/// and points the event sink at the same buffer.
fn capture_logs() -> Buf {
    static LOGS: OnceLock<Buf> = OnceLock::new();
    let buf = LOGS
        .get_or_init(|| {
            let buf = Buf::default();
            let writer = buf.clone();
            tracing::subscriber::set_global_default(telemetry::subscriber(
                move || writer.clone(),
                "info",
            ))
            .unwrap();
            buf
        })
        .clone();
    telemetry::set_event_output(Some(Box::new(buf.clone())));
    buf.take();
    buf
}

const OTEL_VARS: &[&str] = &[
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_TRACES_EXPORTER",
    "OTEL_METRICS_EXPORTER",
    "OTEL_LOGS_EXPORTER",
    "OTEL_TRACES_SAMPLER",
    "OTEL_TRACES_SAMPLER_ARG",
    "OTEL_SERVICE_NAME",
    "OTEL_RESOURCE_ATTRIBUTES",
];

/// Sets exactly `vars` among the OTEL_* variables.
fn set_env(vars: &[(&str, &str)]) {
    for v in OTEL_VARS {
        // SAFETY: the tests take turns, and nothing else in this process
        // reads the environment meanwhile.
        unsafe { std::env::remove_var(v) };
    }
    for (k, v) in vars {
        unsafe { std::env::set_var(k, v) };
    }
}

const NO_EXPORT: &[(&str, &str)] = &[
    ("OTEL_TRACES_EXPORTER", "none"),
    ("OTEL_METRICS_EXPORTER", "prometheus"),
    ("OTEL_LOGS_EXPORTER", "none"),
];

async fn shutdown(p: Providers) {
    tokio::task::spawn_blocking(move || p.shutdown(Duration::from_secs(5)))
        .await
        .unwrap()
        .expect("shutdown");
}

#[derive(Default)]
struct Received {
    spans: Vec<ExportTraceServiceRequest>,
    logs: Vec<ExportLogsServiceRequest>,
    metrics: Vec<ExportMetricsServiceRequest>,
}

/// An in-process OTLP/gRPC collector that keeps what it receives.
#[derive(Clone, Default)]
struct Collector(Arc<Mutex<Received>>);

#[tonic::async_trait]
impl TraceService for Collector {
    async fn export(
        &self,
        req: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        self.0.lock().unwrap().spans.push(req.into_inner());
        Ok(tonic::Response::new(Default::default()))
    }
}

#[tonic::async_trait]
impl LogsService for Collector {
    async fn export(
        &self,
        req: tonic::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
        self.0.lock().unwrap().logs.push(req.into_inner());
        Ok(tonic::Response::new(Default::default()))
    }
}

#[tonic::async_trait]
impl MetricsService for Collector {
    async fn export(
        &self,
        req: tonic::Request<ExportMetricsServiceRequest>,
    ) -> Result<tonic::Response<ExportMetricsServiceResponse>, tonic::Status> {
        self.0.lock().unwrap().metrics.push(req.into_inner());
        Ok(tonic::Response::new(Default::default()))
    }
}

/// Starts a collector and returns it with its endpoint.
async fn start_collector() -> (Collector, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let c = Collector::default();
    let server = tonic::transport::Server::builder()
        .add_service(TraceServiceServer::new(c.clone()))
        .add_service(LogsServiceServer::new(c.clone()))
        .add_service(MetricsServiceServer::new(c.clone()))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));
    tokio::spawn(server);
    (c, format!("http://{addr}"))
}

fn attr(attrs: &[PbKeyValue], key: &str) -> String {
    attrs
        .iter()
        .find(|kv| kv.key == key)
        .and_then(|kv| kv.value.as_ref()?.value.as_ref())
        .map(|v| match v {
            any_value::Value::StringValue(s) => s.clone(),
            other => format!("{other:?}"),
        })
        .unwrap_or_default()
}

const TEST_EVENT: Event = Event {
    name: "ax.test.event",
    body: "Test event",
    severity: Severity::Info,
};

fn counter() -> opentelemetry::metrics::Counter<u64> {
    global::meter("test")
        .u64_counter("ax.test.requests")
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exports_all_signals_over_otlp() {
    let _turn = TURN.lock().await;
    let (c, endpoint) = start_collector().await;
    set_env(&[
        ("OTEL_EXPORTER_OTLP_ENDPOINT", &endpoint),
        ("OTEL_TRACES_SAMPLER", "always_on"),
        ("OTEL_LOGS_EXPORTER", "otlp"),
    ]);
    let out = capture_logs();
    let p = telemetry::init("rax-server").unwrap();

    global::tracer("test").start("test-span");
    counter().add(1, &[]);
    telemetry::emit(
        &TEST_EVENT,
        &[
            KeyValue::new("ax.task.id", "t-1"),
            KeyValue::new("count", 3),
        ],
    );

    // Shutdown flushes the batchers and runs a last metrics export.
    shutdown(p).await;

    let got = c.0.lock().unwrap();
    let rs = &got
        .spans
        .first()
        .expect("collector received no spans")
        .resource_spans[0];
    assert_eq!(
        attr(&rs.resource.as_ref().unwrap().attributes, "service.name"),
        "rax-server"
    );
    assert_eq!(rs.scope_spans[0].spans[0].name, "test-span");

    let names: Vec<&str> = got
        .metrics
        .iter()
        .flat_map(|r| &r.resource_metrics)
        .flat_map(|rm| &rm.scope_metrics)
        .flat_map(|sm| &sm.metrics)
        .map(|m| m.name.as_str())
        .collect();
    assert!(
        names.contains(&"ax.test.requests"),
        "pushed metrics = {names:?}"
    );

    let lr = &got
        .logs
        .first()
        .expect("collector received no log records")
        .resource_logs[0]
        .scope_logs[0]
        .log_records[0];
    assert_eq!(lr.event_name, TEST_EVENT.name);
    assert_eq!(
        lr.body.as_ref().and_then(|b| b.value.clone()),
        Some(any_value::Value::StringValue(TEST_EVENT.body.into()))
    );
    assert_eq!(attr(&lr.attributes, "ax.task.id"), "t-1");
    // Without console, an event sent over OTLP is not also written to stdout.
    let events = stdout_events(&out.take());
    assert!(
        events.is_empty(),
        "event also written to stdout: {events:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn honors_exporter_none() {
    let _turn = TURN.lock().await;
    let (c, endpoint) = start_collector().await;
    set_env(&[
        ("OTEL_EXPORTER_OTLP_ENDPOINT", &endpoint),
        ("OTEL_TRACES_SAMPLER", "always_on"),
        ("OTEL_TRACES_EXPORTER", "none"),
        ("OTEL_METRICS_EXPORTER", "none"),
        ("OTEL_LOGS_EXPORTER", "none"),
    ]);
    let p = telemetry::init("rax-server").unwrap();
    assert!(p.logger.is_none(), "logger provider built with none");

    // Spans are still sampled, so trace context keeps flowing, but not sent.
    let span = global::tracer("test").start("unexported");
    assert!(span.span_context().is_sampled());
    drop(span);
    counter().add(1, &[]);
    // /metrics still serves with the push off.
    assert!(p.metrics_text().contains("ax_test_requests_total"));
    shutdown(p).await;
    let got = c.0.lock().unwrap();
    assert_eq!(
        (got.spans.len(), got.metrics.len(), got.logs.len()),
        (0, 0, 0)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn emit_writes_events_to_stdout_by_default() {
    let _turn = TURN.lock().await;
    let (c, endpoint) = start_collector().await;
    set_env(&[
        ("OTEL_EXPORTER_OTLP_ENDPOINT", &endpoint),
        ("OTEL_TRACES_EXPORTER", "none"),
        ("OTEL_METRICS_EXPORTER", "none"),
    ]);
    let out = capture_logs();
    let p = telemetry::init("rax-server").unwrap();
    assert!(p.logger.is_none(), "the default is none");

    telemetry::emit(&TEST_EVENT, &[KeyValue::new("ax.task.id", "t-1")]);
    shutdown(p).await;

    let events = stdout_events(&out.take());
    assert_eq!(events.len(), 1, "{events:?}");
    let ev = &events[0];
    assert_eq!(ev["event"], TEST_EVENT.name);
    assert_eq!(ev["msg"], TEST_EVENT.body);
    assert_eq!(ev["level"], "INFO");
    assert_eq!(ev["ax.task.id"], "t-1");
    assert!(ev["time"].as_str().unwrap().ends_with('Z'));
    assert_eq!(c.0.lock().unwrap().logs.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn emit_with_console_writes_both() {
    let _turn = TURN.lock().await;
    let (c, endpoint) = start_collector().await;
    set_env(&[
        ("OTEL_EXPORTER_OTLP_ENDPOINT", &endpoint),
        ("OTEL_TRACES_EXPORTER", "none"),
        ("OTEL_METRICS_EXPORTER", "none"),
        ("OTEL_LOGS_EXPORTER", "otlp,console"),
    ]);
    let out = capture_logs();
    let p = telemetry::init("rax-server").unwrap();

    telemetry::emit(&TEST_EVENT, &[]);
    shutdown(p).await;

    assert_eq!(stdout_events(&out.take()).len(), 1);
    assert_eq!(c.0.lock().unwrap().logs.len(), 1);
}

/// Starts `n` root spans and returns the share sampled.
fn sampled_share(n: usize) -> f64 {
    let tracer = global::tracer("test");
    let sampled = (0..n)
        .filter(|_| tracer.start("root").span_context().is_sampled())
        .count();
    sampled as f64 / n as f64
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_sampler_follows_parent_and_samples_a_tenth_of_roots() {
    let _turn = TURN.lock().await;
    set_env(NO_EXPORT);
    let p = telemetry::init("rax-server").unwrap();

    // 10% of 4000 is 400, with a standard deviation of about 19.
    let got = sampled_share(4000);
    assert!(
        (0.075..=0.125).contains(&got),
        "{got:.3} of root spans sampled, want about {DEFAULT_SAMPLE_RATIO}"
    );

    let tracer = p.tracer.tracer("test");
    for parent_sampled in [true, false] {
        let flags = if parent_sampled {
            TraceFlags::SAMPLED
        } else {
            TraceFlags::default()
        };
        let parent = SpanContext::new(
            TraceId::from_bytes([1; 16]),
            SpanId::from_bytes([1; 8]),
            flags,
            true,
            TraceState::default(),
        );
        let cx = Context::new().with_remote_span_context(parent);
        let span = tracer.start_with_context("child", &cx);
        assert_eq!(span.span_context().is_sampled(), parent_sampled);
    }
    shutdown(p).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_sampler_setting_keeps_default() {
    let _turn = TURN.lock().await;
    for (sampler, arg) in [
        ("parentbased_traceidratoi", "0.1"), // typo
        ("parentbased_traceidratio", ""),
        ("parentbased_traceidratio", "10"),
    ] {
        let mut vars = NO_EXPORT.to_vec();
        vars.extend([
            ("OTEL_TRACES_SAMPLER", sampler),
            ("OTEL_TRACES_SAMPLER_ARG", arg),
        ]);
        set_env(&vars);
        let p = telemetry::init("rax-server").unwrap();
        let got = sampled_share(2000);
        assert!(
            (0.065..=0.135).contains(&got),
            "{sampler}={arg}: {got:.3} of root spans sampled"
        );
        shutdown(p).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn propagator_carries_trace_context_only() {
    let _turn = TURN.lock().await;
    set_env(NO_EXPORT);
    let p = telemetry::init("rax-server").unwrap();
    let fields: Vec<String> =
        global::get_text_map_propagator(|prop| prop.fields().map(String::from).collect());
    assert_eq!(fields, ["traceparent", "tracestate"], "no baggage");
    shutdown(p).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_handler_serves_instruments_and_process_metrics() {
    let _turn = TURN.lock().await;
    set_env(NO_EXPORT);
    let p = telemetry::init("rax-server").unwrap();

    let c = counter();
    // 300 sets stay under the SDK's default limit of 2000.
    for i in 0..300 {
        c.add(1, &[KeyValue::new("key", i.to_string())]);
    }
    // Served over HTTP, as on --metrics-addr.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(axum::serve(listener, p.metrics_router()).into_future());
    let body = http_get(addr, "/metrics").await;

    let series = body
        .lines()
        .filter(|l| l.starts_with("ax_test_requests_total{"))
        .count();
    assert_eq!(series, 300);
    for want in [r#"service_name="rax-server""#, "process_cpu_seconds_total"] {
        assert!(body.contains(want), "/metrics has no {want}");
    }
    assert!(!body.contains("otel_metric_overflow"));
    shutdown(p).await;
}

async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    assert!(
        out.starts_with("HTTP/1.1 200"),
        "{}",
        out.lines().next().unwrap_or("")
    );
    out
}

#[test]
fn log_lines_carry_trace_and_span_ids() {
    let buf = Buf::default();
    let writer = buf.clone();
    let sub = telemetry::subscriber(move || writer.clone(), "info");
    let sc = SpanContext::new(
        TraceId::from_hex("0af70000000000000000000000000000").unwrap(),
        SpanId::from_hex("00f0670000000000").unwrap(),
        TraceFlags::SAMPLED,
        false,
        TraceState::default(),
    );
    tracing::subscriber::with_default(sub, || {
        {
            let _cx = Context::new().with_remote_span_context(sc.clone()).attach();
            tracing::info!(k = "v", "with span");
            let _s = tracing::info_span!("op", component = "test").entered();
            tracing::info!("with attrs");
        }
        tracing::info!("without span");
    });
    let out = buf.take();
    let lines: Vec<Value> = out
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 3, "{out}");
    for line in &lines[..2] {
        assert_eq!(
            line["trace_id"], "0af70000000000000000000000000000",
            "{line}"
        );
        assert_eq!(line["span_id"], "00f0670000000000", "{line}");
        assert_eq!(line["trace_flags"], "01", "{line}");
    }
    assert_eq!(lines[0]["msg"], "with span");
    assert_eq!(lines[0]["k"], "v");
    assert_eq!(lines[1]["component"], "test", "span fields lost");
    assert!(lines[2].get("trace_id").is_none(), "{}", lines[2]);
    let keys: Vec<&str> = lines[0]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        [
            "time",
            "level",
            "msg",
            "k",
            "trace_id",
            "span_id",
            "trace_flags"
        ]
    );
}
