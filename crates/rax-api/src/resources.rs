//! Resource requirements.
//!
//! A profile's `resources.limits` is copied onto the Substrate ActorTemplate,
//! which enforces the same rules as below: cpu and memory only, each quantity
//! greater than zero, and a cpu limit below 1000 cores. Checking them here
//! means a bad value fails at apply time instead of when the template is
//! created.

use std::sync::LazyLock;

use num_bigint::BigInt;
use num_rational::BigRational;
use num_traits::{Signed, ToPrimitive, Zero};
use regex::Regex;

use crate::ValidationError;
use crate::v1alpha1::ResourcesSpec;

/// The exclusive upper bound Substrate places on a cpu limit.
pub const MAX_CPU_LIMIT_CORES: i64 = 1000;

/// Reports the first problem with a profile's resource requirements. A
/// missing or empty value is valid: omitted cpu and memory limits default to 1
/// CPU (1000m) and 512Mi when the ActorTemplate is built.
pub fn validate_resources(reqs: Option<&ResourcesSpec>) -> Result<(), ValidationError> {
    validate_resources_at("spec.profile.resources", reqs)
}

pub(crate) fn validate_resources_at(
    field: &str,
    reqs: Option<&ResourcesSpec>,
) -> Result<(), ValidationError> {
    let Some(limits) = reqs.and_then(|r| r.limits.as_ref()) else {
        return Ok(());
    };
    if !limits.cpu.is_empty() {
        parse_cpu_millicores(&limits.cpu)
            .map_err(|e| e.prefixed(&format!("{field}.limits.cpu")))?;
    }
    if !limits.memory.is_empty() {
        parse_memory_bytes(&limits.memory)
            .map_err(|e| e.prefixed(&format!("{field}.limits.memory")))?;
    }
    Ok(())
}

/// Parses a Kubernetes CPU quantity into millicores, rounding up sub-millicore
/// fractions.
pub fn parse_cpu_millicores(cpu: &str) -> Result<i64, ValidationError> {
    let q = parse_quantity(cpu)
        .map_err(|e| ValidationError::new(format!("invalid quantity {cpu:?}: {e}")))?;
    if !q.is_positive() {
        return Err(ValidationError::new(format!(
            "{cpu:?} must be greater than zero"
        )));
    }
    let too_big = || {
        ValidationError::new(format!(
            "{cpu:?} must be less than {MAX_CPU_LIMIT_CORES} cores"
        ))
    };
    if q >= BigRational::from_integer(MAX_CPU_LIMIT_CORES.into()) {
        return Err(too_big());
    }
    let milli = q * BigRational::from_integer(1000.into());
    match milli.ceil().to_integer().to_i64() {
        Some(v) if v < MAX_CPU_LIMIT_CORES * 1000 => Ok(v),
        _ => Err(too_big()),
    }
}

/// Parses a Kubernetes memory quantity into bytes, rounding up fractional
/// bytes.
pub fn parse_memory_bytes(memory: &str) -> Result<i64, ValidationError> {
    let q = parse_quantity(memory)
        .map_err(|e| ValidationError::new(format!("invalid quantity {memory:?}: {e}")))?;
    if !q.is_positive() {
        return Err(ValidationError::new(format!(
            "{memory:?} must be greater than zero"
        )));
    }
    q.ceil().to_integer().to_i64().ok_or_else(|| {
        ValidationError::new(format!(
            "invalid quantity {memory:?}: value overflows int64"
        ))
    })
}

/// Follows the Kubernetes resource.Quantity grammar: a decimal number followed
/// by an optional binary SI suffix (Ki..Ei), decimal SI suffix (n, u, m, k, M,
/// G, T, P, E) or decimal exponent (e3, E-2).
static QUANTITY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))([eE][+-]?[0-9]+|Ki|Mi|Gi|Ti|Pi|Ei|[numkMGTPE])?$")
        .unwrap()
});

/// Evaluates a Kubernetes quantity to its exact value in base units (cores for
/// cpu, bytes for memory).
fn parse_quantity(s: &str) -> Result<BigRational, &'static str> {
    let m = QUANTITY
        .captures(s)
        .ok_or("must be a number with an optional suffix such as 500m, 2, 4Gi or 1e3")?;
    let mut value = parse_decimal(&m[1]).ok_or("not a number")?;
    let ten = || BigInt::from(10);
    let multiplier = match m.get(2).map(|s| s.as_str()) {
        None => return Ok(value),
        Some(suffix) if suffix.starts_with(['e', 'E']) => {
            let exp: i32 = suffix[1..].parse().map_err(|_| "exponent out of range")?;
            if !(-64..=64).contains(&exp) {
                return Err("exponent out of range");
            }
            let pow =
                BigRational::from_integer(num_traits::pow(ten(), exp.unsigned_abs() as usize));
            if exp < 0 { pow.recip() } else { pow }
        }
        Some(suffix) => suffix_multiplier(suffix),
    };
    value *= multiplier;
    Ok(value)
}

fn suffix_multiplier(suffix: &str) -> BigRational {
    let dec = |exp: i32| {
        let p = BigRational::from_integer(num_traits::pow(
            BigInt::from(10),
            exp.unsigned_abs() as usize,
        ));
        if exp < 0 { p.recip() } else { p }
    };
    let bin = |shift: u32| BigRational::from_integer(BigInt::from(1u64 << shift));
    match suffix {
        "n" => dec(-9),
        "u" => dec(-6),
        "m" => dec(-3),
        "k" => dec(3),
        "M" => dec(6),
        "G" => dec(9),
        "T" => dec(12),
        "P" => dec(15),
        "E" => dec(18),
        "Ki" => bin(10),
        "Mi" => bin(20),
        "Gi" => bin(30),
        "Ti" => bin(40),
        "Pi" => bin(50),
        "Ei" => bin(60),
        _ => unreachable!("suffix {suffix} is excluded by the quantity grammar"),
    }
}

/// Parses a signed decimal such as `-1.5`, `.5` or `2.` exactly.
fn parse_decimal(s: &str) -> Option<BigRational> {
    let (negative, digits) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    let all = format!("{int}{frac}");
    let num: BigInt = if all.is_empty() {
        BigInt::zero()
    } else {
        all.parse().ok()?
    };
    let den = num_traits::pow(BigInt::from(10), frac.len());
    let v = BigRational::new(num, den);
    Some(if negative { -v } else { v })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1alpha1::{ObjectMeta, Profile, ResourceList, Task, TaskSpec};

    fn limits(cpu: &str, memory: &str) -> ResourcesSpec {
        ResourcesSpec {
            limits: Some(ResourceList {
                cpu: cpu.into(),
                memory: memory.into(),
            }),
        }
    }

    #[test]
    fn validate_resources_cases() {
        let ok: Vec<(&str, Option<ResourcesSpec>)> = vec![
            ("nil", None),
            ("empty", Some(ResourcesSpec::default())),
            ("empty limits", Some(limits("", ""))),
            ("docs example", Some(limits("2", "4Gi"))),
            ("millicores", Some(limits("500m", ""))),
            ("fractional cores", Some(limits("1.5", ""))),
            ("leading dot", Some(limits(".5", ""))),
            ("exponent", Some(limits("2e0", "1e9"))),
            ("largest cpu", Some(limits("999999m", ""))),
            ("decimal SI memory", Some(limits("", "512M"))),
            ("binary SI memory", Some(limits("", "1Ti"))),
            ("small units", Some(limits("1n", "1u"))),
        ];
        for (name, reqs) in ok {
            assert!(validate_resources(reqs.as_ref()).is_ok(), "{name}");
        }

        let bad: Vec<(&str, ResourcesSpec, &str)> = vec![
            (
                "cpu zero",
                limits("0", ""),
                r#"spec.profile.resources.limits.cpu: "0" must be greater than zero"#,
            ),
            (
                "cpu zero millicores",
                limits("0m", ""),
                "must be greater than zero",
            ),
            (
                "cpu negative",
                limits("-1", ""),
                "must be greater than zero",
            ),
            (
                "cpu at bound",
                limits("1000", ""),
                r#"spec.profile.resources.limits.cpu: "1000" must be less than 1000 cores"#,
            ),
            (
                "cpu over bound via suffix",
                limits("1k", ""),
                "must be less than 1000 cores",
            ),
            (
                "cpu over bound via exponent",
                limits("1e3", ""),
                "must be less than 1000 cores",
            ),
            (
                "cpu over bound via binary suffix",
                limits("1Ki", ""),
                "must be less than 1000 cores",
            ),
            (
                "cpu not a quantity",
                limits("two", ""),
                r#"spec.profile.resources.limits.cpu: invalid quantity "two""#,
            ),
            ("cpu bad suffix", limits("2cores", ""), "invalid quantity"),
            ("cpu double dot", limits("1.2.3", ""), "invalid quantity"),
            ("cpu whitespace", limits(" 2", ""), "invalid quantity"),
            (
                "memory zero",
                limits("", "0Gi"),
                r#"spec.profile.resources.limits.memory: "0Gi" must be greater than zero"#,
            ),
            (
                "memory negative",
                limits("", "-4Gi"),
                "must be greater than zero",
            ),
            (
                "memory lowercase suffix",
                limits("", "4gi"),
                r#"spec.profile.resources.limits.memory: invalid quantity "4gi""#,
            ),
            (
                "memory bytes suffix",
                limits("", "4GiB"),
                "invalid quantity",
            ),
            (
                "memory exponent out of range",
                limits("", "1e100"),
                "exponent out of range",
            ),
        ];
        for (name, reqs, want) in bad {
            let err = validate_resources(Some(&reqs)).expect_err(name).to_string();
            assert!(
                err.contains(want),
                "{name}: error = {err}, want containing {want:?}"
            );
        }
    }

    #[test]
    fn validate_task_checks_resources() {
        let task = |r: ResourcesSpec| Task {
            metadata: Some(ObjectMeta {
                name: "task".into(),
                atespace: "default".into(),
                ..Default::default()
            }),
            spec: Some(TaskSpec {
                profile: Some(Profile {
                    resources: Some(r),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = crate::validate_task(&task(limits("abc", "")))
            .unwrap_err()
            .to_string();
        assert!(err.contains("spec.profile.resources.limits.cpu"), "{err}");
        crate::validate_task(&task(limits("2", "4Gi"))).unwrap();
    }

    #[test]
    fn cpu_millicores() {
        for (input, want) in [
            ("2", Some(2000)),
            ("500m", Some(500)),
            ("1.5", Some(1500)),
            ("1n", Some(1)),
            ("0", None),
            ("1000", None),
        ] {
            assert_eq!(parse_cpu_millicores(input).ok(), want, "{input}");
        }
    }

    #[test]
    fn memory_bytes() {
        for (input, want) in [
            ("4Gi", Some(4i64 << 30)),
            ("512Mi", Some(512 << 20)),
            ("500M", Some(500_000_000)),
            ("1u", Some(1)),
            ("0Gi", None),
        ] {
            assert_eq!(parse_memory_bytes(input).ok(), want, "{input}");
        }
    }
}
