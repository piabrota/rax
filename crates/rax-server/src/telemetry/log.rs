//! rax-server's log lines: one JSON object per line on stdout, in the shape
//! Go's `slog.JSONHandler` writes for AX (`time`, `level`, `msg`, then the
//! fields), with `trace_id`, `span_id` and `trace_flags` added while a trace
//! is active.

use std::fmt;
use std::time::SystemTime;

use opentelemetry::trace::TraceContextExt;
use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::format::{JsonFields, Writer};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, FormattedFields};
use tracing_subscriber::registry::LookupSpan;

/// Formats each event as a JSON line. Use it with [`JsonFields`] as the field
/// formatter, so the fields of enclosing spans, which play the part of
/// slog's `With` attributes, come out as JSON too.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonFormat;

impl<S, N> FormatEvent<S, N> for JsonFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut fields = Map::new();
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                let ext = span.extensions();
                let Some(formatted) = ext.get::<FormattedFields<N>>() else {
                    continue;
                };
                if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(formatted) {
                    fields.extend(map);
                }
            }
        }
        let mut visitor = JsonVisitor::default();
        event.record(&mut visitor);
        fields.extend(visitor.fields);
        let line = json_line(
            SystemTime::now(),
            level_name(*event.metadata().level()),
            &visitor.message.unwrap_or_default(),
            fields,
        );
        writeln!(writer, "{line}")
    }
}

/// slog's names for its levels. tracing's TRACE is below slog's DEBUG, which
/// slog writes as an offset from DEBUG.
pub(crate) fn level_name(level: Level) -> &'static str {
    match level {
        Level::ERROR => "ERROR",
        Level::WARN => "WARN",
        Level::INFO => "INFO",
        Level::DEBUG => "DEBUG",
        Level::TRACE => "DEBUG-4",
    }
}

/// Builds one log line: `time`, `level` and `msg`, then `fields`, then the
/// current trace context if a span is active, the fields the OpenTelemetry
/// specification defines for trace context in non-OTLP logs.
pub(crate) fn json_line(
    time: SystemTime,
    level: &str,
    msg: &str,
    fields: Map<String, Value>,
) -> String {
    let mut rec = Map::new();
    rec.insert("time".into(), Value::String(rfc3339_nano(time)));
    rec.insert("level".into(), Value::String(level.into()));
    rec.insert("msg".into(), Value::String(msg.into()));
    rec.extend(fields);
    let cx = opentelemetry::Context::current();
    let span = cx.span();
    let sc = span.span_context();
    if sc.is_valid() {
        rec.insert("trace_id".into(), Value::String(sc.trace_id().to_string()));
        rec.insert("span_id".into(), Value::String(sc.span_id().to_string()));
        rec.insert(
            "trace_flags".into(),
            Value::String(format!("{:02x}", sc.trace_flags().to_u8())),
        );
    }
    Value::Object(rec).to_string()
}

/// Go's `time.RFC3339Nano` in UTC: trailing zeros of the fraction dropped.
fn rfc3339_nano(t: SystemTime) -> String {
    let t = chrono::DateTime::<chrono::Utc>::from(t);
    let mut s = t.format("%Y-%m-%dT%H:%M:%S%.9f").to_string();
    while s.ends_with('0') {
        s.pop();
    }
    if s.ends_with('.') {
        s.pop();
    }
    s.push('Z');
    s
}

#[derive(Default)]
struct JsonVisitor {
    message: Option<String>,
    fields: Map<String, Value>,
}

impl JsonVisitor {
    fn put(&mut self, field: &Field, v: Value) {
        if field.name() == "message" {
            self.message = Some(match v {
                Value::String(s) => s,
                other => other.to_string(),
            });
        } else {
            self.fields.insert(field.name().into(), v);
        }
    }
}

impl Visit for JsonVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, Value::String(value.into()));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, value.into());
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.put(field, value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, value.into());
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.put(field, Value::String(value.to_string()));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.put(field, Value::String(format!("{value:?}")));
    }
}

/// The subscriber rax-server logs through: [`JsonFormat`] lines on `writer`,
/// filtered by `RUST_LOG` or `default_filter`.
pub fn subscriber<W>(writer: W, default_filter: &str) -> impl Subscriber + Send + Sync
where
    W: for<'w> tracing_subscriber::fmt::MakeWriter<'w> + Send + Sync + 'static,
{
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| default_filter.into()),
        )
        .fmt_fields(JsonFields::new())
        .event_format(JsonFormat)
        .with_writer(writer)
        .finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_times_as_go_does() {
        use std::time::{Duration, UNIX_EPOCH};
        let t = UNIX_EPOCH + Duration::new(1_793_998_800, 123);
        assert_eq!(rfc3339_nano(t), "2026-11-06T21:00:00.000000123Z");
        let t = UNIX_EPOCH + Duration::new(1_793_998_800, 500_000_000);
        assert_eq!(rfc3339_nano(t), "2026-11-06T21:00:00.5Z");
        assert_eq!(
            rfc3339_nano(UNIX_EPOCH + Duration::from_secs(1_793_998_800)),
            "2026-11-06T21:00:00Z"
        );
    }
}
