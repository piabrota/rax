//! Structured events, such as task state changes: OTLP log records with an
//! event name, or JSON log lines on stdout while OTLP logs are off.

use std::io::Write;
use std::sync::{Mutex, RwLock};
use std::time::SystemTime;

use opentelemetry::logs::{AnyValue, LogRecord, Logger, Severity};
use opentelemetry::{Array, KeyValue, Value};
use opentelemetry_sdk::logs::SdkLogger;
use serde_json::Map;

use super::log::json_line;

/// The instrumentation scope of every event rax-server emits.
pub const EVENT_SCOPE: &str = "rax-server/telemetry";

/// A kind of structured event. `name` is the OTLP log record's event name.
/// `body` and `severity` belong to the kind, not the call site, so the OTLP
/// and stdout forms of an event cannot differ.
#[derive(Clone, Copy, Debug)]
pub struct Event {
    pub name: &'static str,
    pub body: &'static str,
    pub severity: Severity,
}

impl Event {
    /// Maps the event's severity onto the four log levels.
    pub(crate) fn level(&self) -> tracing::Level {
        let s = self.severity as i32;
        if s >= Severity::Error as i32 {
            tracing::Level::ERROR
        } else if s >= Severity::Warn as i32 {
            tracing::Level::WARN
        } else if s >= Severity::Info as i32 {
            tracing::Level::INFO
        } else {
            tracing::Level::DEBUG
        }
    }
}

/// Where [`emit`] sends events. [`super::init`] sets it from
/// `OTEL_LOGS_EXPORTER`.
#[derive(Default)]
pub(crate) struct Sink {
    /// Set while `OTEL_LOGS_EXPORTER` selects otlp.
    pub(crate) logger: Option<SdkLogger>,
    /// Also write events that went over OTLP to stdout.
    pub(crate) console: bool,
}

pub(crate) static SINK: RwLock<Option<Sink>> = RwLock::new(None);

type Out = Box<dyn Write + Send>;
static OUT: Mutex<Option<Out>> = Mutex::new(None);

/// Sends the stdout form of events to `out` instead of stdout, and returns
/// what was there before. For tests.
pub fn set_event_output(out: Option<Out>) -> Option<Out> {
    std::mem::replace(&mut *OUT.lock().unwrap(), out)
}

/// Records `ev` with `attrs`. It sends an OTLP log record when
/// `OTEL_LOGS_EXPORTER` selects otlp, and writes the event as a JSON log
/// line, with its name in `event`, when it does not, or also when console is
/// selected. An event is therefore never dropped because OTLP is off.
///
/// The OTLP record is batched, so a slow or unreachable collector never
/// blocks the caller. A record still queued when the process dies is lost,
/// unless console also wrote it to stdout.
pub fn emit(ev: &Event, attrs: &[KeyValue]) {
    let now = SystemTime::now();
    let (sent, console) = match SINK.read().unwrap().as_ref() {
        Some(sink) => (emit_otlp(sink, ev, now, attrs), sink.console),
        None => (false, false),
    };
    if sent && !console {
        return;
    }
    if ev.level() > tracing::level_filters::LevelFilter::current() {
        return;
    }
    let mut fields = Map::new();
    fields.insert("event".into(), ev.name.into());
    for kv in attrs {
        fields.insert(kv.key.to_string(), json_value(&kv.value));
    }
    let line = json_line(now, super::log::level_name(ev.level()), ev.body, fields);
    let mut out = OUT.lock().unwrap();
    let _ = match out.as_mut() {
        Some(w) => writeln!(w, "{line}"),
        None => writeln!(std::io::stdout().lock(), "{line}"),
    };
}

/// Sends the OTLP record and reports whether it did.
fn emit_otlp(sink: &Sink, ev: &Event, t: SystemTime, attrs: &[KeyValue]) -> bool {
    let Some(logger) = &sink.logger else {
        return false;
    };
    if !logger.event_enabled(ev.severity, EVENT_SCOPE, Some(ev.name)) {
        return false;
    }
    let mut rec = logger.create_log_record();
    rec.set_event_name(ev.name);
    rec.set_timestamp(t);
    rec.set_severity_number(ev.severity);
    rec.set_severity_text(ev.severity.name());
    rec.set_body(AnyValue::String(ev.body.into()));
    rec.add_attributes(
        attrs
            .iter()
            .map(|kv| (kv.key.clone(), any_value(&kv.value))),
    );
    // The SDK takes the trace and span IDs from the current context.
    logger.emit(rec);
    true
}

/// An attribute value as a log record holds it.
pub(crate) fn any_value(v: &Value) -> AnyValue {
    match v {
        Value::Bool(b) => AnyValue::Boolean(*b),
        Value::I64(i) => AnyValue::Int(*i),
        Value::F64(f) => AnyValue::Double(*f),
        Value::String(s) => AnyValue::String(s.clone()),
        Value::Array(a) => AnyValue::ListAny(Box::new(match a {
            Array::Bool(v) => v.iter().map(|b| AnyValue::Boolean(*b)).collect(),
            Array::I64(v) => v.iter().map(|i| AnyValue::Int(*i)).collect(),
            Array::F64(v) => v.iter().map(|f| AnyValue::Double(*f)).collect(),
            Array::String(v) => v.iter().map(|s| AnyValue::String(s.clone())).collect(),
            _ => Vec::new(),
        })),
        _ => AnyValue::String(v.as_str().into_owned().into()),
    }
}

/// An attribute value as a JSON log line holds it.
pub(crate) fn json_value(v: &Value) -> serde_json::Value {
    match v {
        Value::Bool(b) => (*b).into(),
        Value::I64(i) => (*i).into(),
        Value::F64(f) => (*f).into(),
        Value::String(s) => s.as_str().into(),
        Value::Array(a) => match a {
            Array::Bool(v) => v.clone().into(),
            Array::I64(v) => v.clone().into(),
            Array::F64(v) => v.clone().into(),
            Array::String(v) => v.iter().map(|s| s.as_str()).collect::<Vec<_>>().into(),
            _ => serde_json::Value::Array(Vec::new()),
        },
        _ => v.as_str().into_owned().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_forms_hold_the_same_kinds() {
        let cases: Vec<(Value, AnyValue, serde_json::Value)> = vec![
            ("s".into(), AnyValue::String("s".into()), "s".into()),
            ((-3i64).into(), AnyValue::Int(-3), (-3).into()),
            (1.5.into(), AnyValue::Double(1.5), 1.5.into()),
            (true.into(), AnyValue::Boolean(true), true.into()),
            (
                Value::Array(vec![1i64, 2].into()),
                AnyValue::ListAny(Box::new(vec![AnyValue::Int(1), AnyValue::Int(2)])),
                serde_json::json!([1, 2]),
            ),
        ];
        for (v, any, json) in cases {
            assert_eq!(any_value(&v), any, "{v:?}");
            assert_eq!(json_value(&v), json, "{v:?}");
        }
    }

    #[test]
    fn levels_follow_severity() {
        let ev = |severity| Event {
            name: "e",
            body: "b",
            severity,
        };
        assert_eq!(ev(Severity::Error2).level(), tracing::Level::ERROR);
        assert_eq!(ev(Severity::Warn).level(), tracing::Level::WARN);
        assert_eq!(ev(Severity::Info4).level(), tracing::Level::INFO);
        assert_eq!(ev(Severity::Debug).level(), tracing::Level::DEBUG);
        assert_eq!(ev(Severity::Trace).level(), tracing::Level::DEBUG);
    }
}
