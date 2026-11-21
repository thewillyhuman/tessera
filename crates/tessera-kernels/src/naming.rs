// SPDX-License-Identifier: Apache-2.0

//! Names across languages (section 9.8): the Prometheus name of a
//! metric, following the OpenTelemetry-to-Prometheus rules for
//! characters, unit suffixes and `_total` (QL-19).
//!
//! A name that already is a Prometheus name, as remote write delivers
//! them, maps to itself: the unit is empty and the suffixes are only
//! added where missing.

/// The Prometheus name of a metric named `name` with unit `unit`: the
/// characters outside `[a-zA-Z0-9_:]` become `_`, the unit's name is
/// appended for the units Prometheus spells out (`ms` is
/// `_milliseconds`, `By` is `_bytes`), a dimensionless gauge gets
/// `_ratio`, and a monotonic sum gets `_total`.
#[must_use]
pub fn prometheus_name(name: &str, unit: &str, metric_type: &str, is_monotonic: bool) -> String {
    let mut out = sanitize(name);
    let unit = unit.trim();
    let suffix = unit_suffix(unit);
    // A counter named with its unit and `_total` already keeps both.
    let bare = out.strip_suffix("_total").unwrap_or(&out).to_owned();
    if !suffix.is_empty() && !bare.ends_with(&format!("_{suffix}")) {
        out = bare;
        out.push('_');
        out.push_str(&suffix);
    }
    if unit == "1" && metric_type == "gauge" && !out.ends_with("_ratio") {
        out.push_str("_ratio");
    }
    if is_monotonic && !out.ends_with("_total") {
        out.push_str("_total");
    }
    out
}

/// `name` with every character outside `[a-zA-Z0-9_:]` replaced by `_`,
/// runs of `_` collapsed, and a leading digit prefixed by `_`.
fn sanitize(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut last_underscore = false;
    for c in name.chars() {
        let c = if c.is_ascii_alphanumeric() || c == ':' {
            c
        } else {
            '_'
        };
        if c == '_' && last_underscore {
            continue;
        }
        last_underscore = c == '_';
        out.push(c);
    }
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

/// The Prometheus spelling of a UCUM unit, empty for the units that add
/// no suffix (`1`, annotations in braces) and unknown units.
fn unit_suffix(unit: &str) -> String {
    let unit = strip_annotation(unit);
    if let Some((numerator, denominator)) = unit.split_once('/') {
        // The denominator is singular: `By/s` is `bytes_per_second`.
        let n = unit_word(numerator);
        let d = unit_word(denominator).trim_end_matches('s');
        return match (n.is_empty(), d.is_empty()) {
            (false, false) => format!("{n}_per_{d}"),
            (true, false) => format!("per_{d}"),
            (false, true) => n.to_owned(),
            (true, true) => String::new(),
        };
    }
    unit_word(unit).to_owned()
}

fn strip_annotation(unit: &str) -> &str {
    match unit.find('{') {
        Some(start) => unit[..start].trim(),
        None => unit,
    }
}

fn unit_word(unit: &str) -> &'static str {
    match unit.trim() {
        "s" => "seconds",
        "ms" => "milliseconds",
        "us" => "microseconds",
        "ns" => "nanoseconds",
        "min" => "minutes",
        "h" => "hours",
        "d" => "days",
        "By" => "bytes",
        "KiBy" => "kibibytes",
        "MiBy" => "mebibytes",
        "GiBy" => "gibibytes",
        "TiBy" => "tibibytes",
        "KBy" => "kilobytes",
        "MBy" => "megabytes",
        "GBy" => "gigabytes",
        "TBy" => "terabytes",
        "m" => "meters",
        "V" => "volts",
        "A" => "amperes",
        "J" => "joules",
        "W" => "watts",
        "g" => "grams",
        "Cel" => "celsius",
        "Hz" => "hertz",
        "%" => "percent",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn otel_names_get_prometheus_suffixes() {
        assert_eq!(
            prometheus_name("http.server.duration", "ms", "histogram", false),
            "http_server_duration_milliseconds"
        );
        assert_eq!(
            prometheus_name("http.requests", "{requests}", "sum", true),
            "http_requests_total"
        );
        assert_eq!(
            prometheus_name("system.memory.usage", "By", "sum", false),
            "system_memory_usage_bytes"
        );
        assert_eq!(
            prometheus_name("cpu.utilization", "1", "gauge", false),
            "cpu_utilization_ratio"
        );
        assert_eq!(
            prometheus_name("network.rate", "By/s", "gauge", false),
            "network_rate_bytes_per_second"
        );
        assert_eq!(
            prometheus_name("1st-metric", "", "gauge", false),
            "_1st_metric"
        );
        assert_eq!(prometheus_name("a..b", "", "gauge", false), "a_b");
    }

    #[test]
    fn prometheus_names_map_to_themselves() {
        assert_eq!(
            prometheus_name("http_requests_total", "", "sum", true),
            "http_requests_total"
        );
        assert_eq!(prometheus_name("up", "", "gauge", false), "up");
        assert_eq!(
            prometheus_name("process_cpu_seconds_total", "s", "sum", true),
            "process_cpu_seconds_total"
        );
        assert_eq!(
            prometheus_name("node:cpu:ratio", "", "gauge", false),
            "node:cpu:ratio"
        );
    }
}
