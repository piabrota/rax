//! The `OTEL_*` settings rax-server reads itself rather than leaving to the
//! SDK, so that a value it does not understand keeps rax-server's default
//! instead of, say, sampling every trace or turning export off.

use std::fmt;

pub use rax_otelenv::{
    EXPORTER_CONSOLE, EXPORTER_NONE, EXPORTER_OTLP, EXPORTER_PROMETHEUS, Exporters,
    resolve_exporters,
};

use opentelemetry_sdk::trace::Sampler;

pub(crate) const TRACES_EXPORTER_ENV: &str = "OTEL_TRACES_EXPORTER";
pub(crate) const METRICS_EXPORTER_ENV: &str = "OTEL_METRICS_EXPORTER";
pub(crate) const LOGS_EXPORTER_ENV: &str = "OTEL_LOGS_EXPORTER";
pub(crate) const TRACES_SAMPLER_ENV: &str = "OTEL_TRACES_SAMPLER";
pub(crate) const TRACES_SAMPLER_ARG_ENV: &str = "OTEL_TRACES_SAMPLER_ARG";

/// Reads `env` and applies [`resolve_exporters`]. A problem with the value is
/// logged, not returned: a telemetry setting never stops startup.
pub(crate) fn resolve_exporters_env(env: &str, known: &[&str], def: &Exporters) -> Exporters {
    let value = std::env::var(env).unwrap_or_default();
    let (got, err) = resolve_exporters(env, &value, known, def);
    if let Some(err) = err {
        tracing::warn!(variable = env, value, using = %got, error = err, "invalid OpenTelemetry exporter setting");
    }
    got
}

/// A trace sampler, as a value that can be compared and described. The SDK's
/// own `Sampler` cannot be inspected once built.
#[derive(Clone, Debug, PartialEq)]
pub enum SamplerSpec {
    AlwaysOn,
    AlwaysOff,
    TraceIdRatio(f64),
    /// Follows the parent's decision and uses the root sampler for new traces.
    ParentBased(Box<SamplerSpec>),
}

impl SamplerSpec {
    pub fn to_sdk(&self) -> Sampler {
        match self {
            SamplerSpec::AlwaysOn => Sampler::AlwaysOn,
            SamplerSpec::AlwaysOff => Sampler::AlwaysOff,
            SamplerSpec::TraceIdRatio(r) => Sampler::TraceIdRatioBased(*r),
            SamplerSpec::ParentBased(root) => Sampler::ParentBased(Box::new(root.to_sdk())),
        }
    }
}

/// The description Go's SDK gives the same sampler, so logs read alike.
impl fmt::Display for SamplerSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SamplerSpec::AlwaysOn => f.write_str("AlwaysOnSampler"),
            SamplerSpec::AlwaysOff => f.write_str("AlwaysOffSampler"),
            SamplerSpec::TraceIdRatio(r) => write!(f, "TraceIDRatioBased{{{r}}}"),
            SamplerSpec::ParentBased(root) => write!(
                f,
                "ParentBased{{root:{root},remoteParentSampled:AlwaysOnSampler,remoteParentNotSampled:AlwaysOffSampler,localParentSampled:AlwaysOnSampler,localParentNotSampled:AlwaysOffSampler}}"
            ),
        }
    }
}

/// Applies `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG` on top of
/// `def`. rax-server reads them itself because the SDK falls back to
/// sampling every trace on a value it does not know. That would multiply the
/// trace volume by ten, so rax-server keeps `def` instead and logs a warning.
pub(crate) fn resolve_sampler(def: SamplerSpec) -> SamplerSpec {
    let name = std::env::var(TRACES_SAMPLER_ENV).ok();
    let arg = std::env::var(TRACES_SAMPLER_ARG_ENV).ok();
    let (s, err) = parse_sampler(name.as_deref(), arg.as_deref(), &def);
    if let Some(err) = err {
        tracing::warn!(
            sampler = name.unwrap_or_default(),
            arg = arg.unwrap_or_default(),
            default = %def,
            error = err,
            "invalid trace sampler setting, keeping the default"
        );
    }
    s
}

/// Accepts the sampler names the OpenTelemetry SDKs know, `None` meaning the
/// variable is unset. Any error means `def` was kept.
pub fn parse_sampler(
    name: Option<&str>,
    arg: Option<&str>,
    def: &SamplerSpec,
) -> (SamplerSpec, Option<String>) {
    let name = name.unwrap_or_default().trim().to_lowercase();
    // Unlike the SDK, an empty value reads as unset.
    if name.is_empty() {
        return (def.clone(), None);
    }
    let mut ratio = 0.0;
    if name == "traceidratio" || name == "parentbased_traceidratio" {
        let trimmed = arg.unwrap_or_default().trim();
        if trimmed.is_empty() {
            return (
                def.clone(),
                Some(format!(
                    "{TRACES_SAMPLER_ENV} {name:?} requires {TRACES_SAMPLER_ARG_ENV} to be a ratio in [0, 1]"
                )),
            );
        }
        match trimmed.parse::<f64>() {
            Err(e) => {
                return (
                    def.clone(),
                    Some(format!(
                        "parsing {TRACES_SAMPLER_ARG_ENV} {:?}: {e}",
                        arg.unwrap_or_default()
                    )),
                );
            }
            Ok(r) if !(0.0..=1.0).contains(&r) => {
                return (
                    def.clone(),
                    Some(format!(
                        "{TRACES_SAMPLER_ARG_ENV} {:?} is outside [0, 1]",
                        arg.unwrap_or_default()
                    )),
                );
            }
            Ok(r) => ratio = r,
        }
    }
    let s = match name.as_str() {
        "always_on" => SamplerSpec::AlwaysOn,
        "always_off" => SamplerSpec::AlwaysOff,
        "traceidratio" => SamplerSpec::TraceIdRatio(ratio),
        "parentbased_always_on" => SamplerSpec::ParentBased(Box::new(SamplerSpec::AlwaysOn)),
        "parentbased_always_off" => SamplerSpec::ParentBased(Box::new(SamplerSpec::AlwaysOff)),
        "parentbased_traceidratio" => {
            SamplerSpec::ParentBased(Box::new(SamplerSpec::TraceIdRatio(ratio)))
        }
        _ => {
            return (
                def.clone(),
                Some(format!("unsupported {TRACES_SAMPLER_ENV} {name:?}")),
            );
        }
    };
    (s, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_samplers() {
        let def = SamplerSpec::ParentBased(Box::new(SamplerSpec::TraceIdRatio(0.1)));
        let pb = |s| SamplerSpec::ParentBased(Box::new(s)).to_string();
        for (name, arg, want, want_err) in [
            (None, None, def.to_string(), false),
            (Some(" "), None, def.to_string(), false),
            (Some("always_on"), None, "AlwaysOnSampler".into(), false),
            (Some("ALWAYS_OFF"), None, "AlwaysOffSampler".into(), false),
            (
                Some("traceidratio"),
                Some("0.5"),
                "TraceIDRatioBased{0.5}".into(),
                false,
            ),
            (
                Some("parentbased_traceidratio"),
                Some(" 0.25 "),
                pb(SamplerSpec::TraceIdRatio(0.25)),
                false,
            ),
            (
                Some("parentbased_always_on"),
                None,
                pb(SamplerSpec::AlwaysOn),
                false,
            ),
            (
                Some("parentbased_always_off"),
                None,
                pb(SamplerSpec::AlwaysOff),
                false,
            ),
            (
                Some("parentbased_traceidratoi"),
                Some("0.1"),
                def.to_string(),
                true,
            ),
            (Some("jaeger_remote"), None, def.to_string(), true),
            (Some("traceidratio"), None, def.to_string(), true),
            (Some("traceidratio"), Some("half"), def.to_string(), true),
            (Some("traceidratio"), Some("-0.1"), def.to_string(), true),
        ] {
            let (got, err) = parse_sampler(name, arg, &def);
            assert_eq!(
                (got.to_string(), err.is_some()),
                (want, want_err),
                "parse_sampler({name:?}, {arg:?}): {err:?}"
            );
        }
        assert_eq!(
            def.to_string(),
            "ParentBased{root:TraceIDRatioBased{0.1},remoteParentSampled:AlwaysOnSampler,remoteParentNotSampled:AlwaysOffSampler,localParentSampled:AlwaysOnSampler,localParentNotSampled:AlwaysOffSampler}"
        );
    }
}
