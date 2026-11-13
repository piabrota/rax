//! The OpenTelemetry environment rules rax-server and the `rax` CLI share,
//! as AX keeps them in its `otelenv` package: how an `OTEL_*_EXPORTER`
//! variable selects exporters, and the resource that describes the process.
//! This crate depends only on the OpenTelemetry API and SDK, so the CLI does
//! not link rax-server's metric and log exporters.

use std::collections::BTreeSet;
use std::fmt;

use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::resource::{EnvResourceDetector, TelemetryResourceDetector};
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;

/// The names the `OTEL_*_EXPORTER` variables use. `none` selects the empty
/// set; it is not an exporter.
pub const EXPORTER_OTLP: &str = "otlp";
pub const EXPORTER_CONSOLE: &str = "console";
pub const EXPORTER_PROMETHEUS: &str = "prometheus";
pub const EXPORTER_NONE: &str = "none";

/// The set of exporters an `OTEL_*_EXPORTER` variable selects. The empty set
/// is `none`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Exporters(BTreeSet<String>);

impl Exporters {
    pub fn of(names: &[&str]) -> Self {
        Exporters(names.iter().map(|n| n.to_string()).collect())
    }

    pub fn has(&self, name: &str) -> bool {
        self.0.contains(name)
    }
}

/// Lists the names in order, or `none`.
impl fmt::Display for Exporters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return f.write_str(EXPORTER_NONE);
        }
        let names: Vec<&str> = self.0.iter().map(String::as_str).collect();
        f.write_str(&names.join(","))
    }
}

/// Reads `value` as a comma-separated exporter list, as the OpenTelemetry
/// specification allows. Names are trimmed and lowercased; empty and unknown
/// names are skipped with an error, and the rest apply. A value that is
/// empty, names no known exporter, or lists `none` with an exporter keeps
/// `def`, as the specification asks for a setting it cannot use. The returned
/// set is the one to use even when there is an error.
pub fn resolve_exporters(
    env: &str,
    value: &str,
    known: &[&str],
    def: &Exporters,
) -> (Exporters, Option<String>) {
    // Unset and empty read the same, because templated manifests can render
    // an empty variable.
    if value.trim().is_empty() {
        return (def.clone(), None);
    }
    let mut got = Exporters::default();
    let mut none = false;
    let mut skipped = Vec::new();
    for item in value.split(',') {
        let name = item.trim().to_lowercase();
        if name == EXPORTER_NONE {
            none = true;
        } else if known.contains(&name.as_str()) {
            got.0.insert(name);
        } else {
            skipped.push(name);
        }
    }
    let skip_err = (!skipped.is_empty())
        .then(|| format!("{env}: skipped empty or unknown exporters {skipped:?}"));
    if none && !got.0.is_empty() {
        return (
            def.clone(),
            Some(format!("{env} {value:?} lists none with an exporter")),
        );
    }
    if none {
        return (Exporters::default(), skip_err);
    }
    if got.0.is_empty() {
        return (
            def.clone(),
            Some(format!("{env} {value:?} names no known exporter")),
        );
    }
    (got, skip_err)
}

/// Describes the process: `service.name` and the SDK attributes, then `attrs`,
/// with `OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME` on top.
pub fn resource(service_name: &str, attrs: impl IntoIterator<Item = KeyValue>) -> Resource {
    let mut b = Resource::builder_empty()
        .with_detector(Box::new(TelemetryResourceDetector))
        .with_schema_url(
            [KeyValue::new(SERVICE_NAME, service_name.to_string())],
            opentelemetry_semantic_conventions::SCHEMA_URL,
        )
        .with_attributes(attrs)
        .with_detector(Box::new(EnvResourceDetector::new()));
    if let Ok(name) = std::env::var("OTEL_SERVICE_NAME")
        && !name.is_empty()
    {
        b = b.with_attribute(KeyValue::new(SERVICE_NAME, name));
    }
    b.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_exporters() {
        let known = [EXPORTER_OTLP, EXPORTER_CONSOLE];
        let def = Exporters::of(&[EXPORTER_OTLP]);
        for (val, want, want_err) in [
            ("", "otlp", false),
            (" ", "otlp", false),
            ("otlp", "otlp", false),
            (" OTLP ", "otlp", false),
            ("console", "console", false),
            ("otlp,console", "console,otlp", false),
            ("none", "none", false),
            ("none,", "none", true),
            ("otlp,none", "otlp", true), // contradictory: keep the default
            ("otpl", "otlp", true),      // typo: keep the default
            ("zipkin", "otlp", true),
            ("zipkin,console", "console", true),
        ] {
            let (got, err) = resolve_exporters("OTEL_TEST_EXPORTER", val, &known, &def);
            assert_eq!(
                (got.to_string().as_str(), err.is_some()),
                (want, want_err),
                "resolve_exporters({val:?}): {err:?}"
            );
        }
    }
}
