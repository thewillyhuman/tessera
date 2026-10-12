// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `PromQL` functions, with Prometheus' algorithms: the rate family
//! with counter resets and extrapolation, the `_over_time` family,
//! arithmetic and rounding, label rewriting, `histogram_quantile` and
//! `histogram_fraction` over native histograms and classic buckets,
//! the `histogram_*` readers of a native histogram, and the calendar
//! functions. A function of floats leaves histogram samples out, as
//! Prometheus does with a note.

use std::collections::BTreeMap;

use tessera_ir::promql::PromExpr;

use super::{
    Evaluator, Histogram, Labels, Point, Sample, Series, Value, quantile, unsupported, without_name,
};
use crate::KernelError;

/// `function(args)` at `t`.
pub(super) fn call(
    evaluator: &Evaluator<'_>,
    function: &str,
    args: &[PromExpr],
    t: i64,
) -> Result<Value, KernelError> {
    if let Some(value) = call_range(evaluator, function, args, t)? {
        return Ok(value);
    }
    if let Some(value) = call_rounding(evaluator, function, args, t)? {
        return Ok(value);
    }
    if let Some(value) = call_histogram(evaluator, function, args, t)? {
        return Ok(value);
    }
    match function {
        "absent" => {
            let Value::Vector(vector) = evaluator.eval(&args[0], t)? else {
                return Err(unsupported("absent needs an instant vector"));
            };
            Ok(Value::Vector(if vector.is_empty() {
                vec![(absent_labels(&args[0]), Point::Float(1.0))]
            } else {
                Vec::new()
            }))
        }
        "time" => Ok(Value::Scalar(seconds(t))),
        "pi" => Ok(Value::Scalar(std::f64::consts::PI)),
        "scalar" => {
            let Value::Vector(vector) = evaluator.eval(&args[0], t)? else {
                return Err(unsupported("scalar needs an instant vector"));
            };
            // The one float of the vector; histograms do not count.
            let mut floats = vector.iter().filter_map(|(_, v)| v.float());
            Ok(Value::Scalar(match (floats.next(), floats.next()) {
                (Some(value), None) => value,
                _ => f64::NAN,
            }))
        }
        "vector" => {
            let value = scalar_argument(evaluator, function, args, 0, t)?;
            Ok(Value::Vector(vec![(Labels::new(), Point::Float(value))]))
        }
        "timestamp" => {
            let series = vector_argument(evaluator, function, args, 0, t)?;
            // Only a selector has sample times; functions report the step.
            Ok(Value::Vector(
                series
                    .into_iter()
                    .map(|(labels, _)| (without_name(labels), Point::Float(seconds(t))))
                    .collect(),
            ))
        }
        "sort" | "sort_desc" => {
            let mut series = float_samples(vector_argument(evaluator, function, args, 0, t)?);
            series.sort_by(|a, b| a.1.total_cmp(&b.1));
            if function == "sort_desc" {
                series.reverse();
            }
            Ok(Value::Vector(
                series
                    .into_iter()
                    .map(|(labels, v)| (labels, Point::Float(v)))
                    .collect(),
            ))
        }
        "label_replace" => label_replace(evaluator, args, t),
        "label_join" => label_join(evaluator, args, t),
        "day_of_month" | "day_of_week" | "day_of_year" | "days_in_month" | "hour" | "minute"
        | "month" | "year" => {
            let series = if args.is_empty() {
                vec![(Labels::new(), Point::Float(seconds(t)))]
            } else {
                vector_argument(evaluator, function, args, 0, t)?
            };
            Ok(map_floats(series, |v| calendar(function, v)))
        }
        _ => {
            let Some(math) = math_function(function) else {
                return Err(unsupported(format!("unknown function {function}")));
            };
            let series = vector_argument(evaluator, function, args, 0, t)?;
            Ok(map_floats(series, math))
        }
    }
}

/// The functions over a range vector, or `None` for any other name.
fn call_range(
    evaluator: &Evaluator<'_>,
    function: &str,
    args: &[PromExpr],
    t: i64,
) -> Result<Option<Value>, KernelError> {
    Ok(Some(match function {
        "rate" | "increase" | "delta" | "irate" | "idelta" | "changes" | "resets" | "deriv"
        | "avg_over_time" | "min_over_time" | "max_over_time" | "sum_over_time"
        | "count_over_time" | "last_over_time" | "present_over_time" | "stddev_over_time"
        | "stdvar_over_time" | "mad_over_time" => {
            let series = range_argument(evaluator, function, args, 0, t)?;
            let end = range_end(&args[0], t);
            let range = range_of(&args[0]);
            Value::Vector(
                series
                    .into_iter()
                    .filter_map(|s| {
                        let value = range_function(function, &s.samples, end, range)?;
                        // Prometheus keeps the metric name only where the
                        // value is a sample's own: last_over_time.
                        let labels = if function == "last_over_time" {
                            s.labels
                        } else {
                            without_name(s.labels)
                        };
                        Some((labels, value))
                    })
                    .collect(),
            )
        }
        "quantile_over_time" => {
            let q = scalar_argument(evaluator, function, args, 0, t)?;
            let series = range_argument(evaluator, function, args, 1, t)?;
            Value::Vector(
                series
                    .into_iter()
                    .filter_map(|s| {
                        let mut values: Vec<f64> =
                            s.samples.iter().filter_map(|(_, v)| v.float()).collect();
                        (!values.is_empty()).then(|| {
                            (
                                without_name(s.labels),
                                Point::Float(quantile(q, &mut values)),
                            )
                        })
                    })
                    .collect(),
            )
        }
        "predict_linear" => {
            let series = range_argument(evaluator, function, args, 0, t)?;
            let seconds = scalar_argument(evaluator, function, args, 1, t)?;
            let end = range_end(&args[0], t);
            Value::Vector(
                series
                    .into_iter()
                    .filter_map(|s| {
                        let (slope, intercept) = linear_regression(&floats_of(&s.samples), end)?;
                        Some((
                            without_name(s.labels),
                            Point::Float(intercept + slope * seconds),
                        ))
                    })
                    .collect(),
            )
        }
        "holt_winters" | "double_exponential_smoothing" => {
            let series = range_argument(evaluator, function, args, 0, t)?;
            let sf = scalar_argument(evaluator, function, args, 1, t)?;
            let tf = scalar_argument(evaluator, function, args, 2, t)?;
            Value::Vector(
                series
                    .into_iter()
                    .filter_map(|s| {
                        let value = holt_winters(&floats_of(&s.samples), sf, tf)?;
                        Some((without_name(s.labels), Point::Float(value)))
                    })
                    .collect(),
            )
        }
        "absent_over_time" => {
            let series = range_argument(evaluator, function, args, 0, t)?;
            Value::Vector(if series.is_empty() {
                vec![(absent_labels(&args[0]), Point::Float(1.0))]
            } else {
                Vec::new()
            })
        }
        _ => return Ok(None),
    }))
}

/// `round` and the `clamp` family, or `None` for any other name.
fn call_rounding(
    evaluator: &Evaluator<'_>,
    function: &str,
    args: &[PromExpr],
    t: i64,
) -> Result<Option<Value>, KernelError> {
    Ok(Some(match function {
        "round" => {
            let series = vector_argument(evaluator, function, args, 0, t)?;
            let to_nearest = if args.len() > 1 {
                scalar_argument(evaluator, function, args, 1, t)?
            } else {
                1.0
            };
            map_floats(series, |v| (v / to_nearest).round() * to_nearest)
        }
        "clamp" => {
            let series = vector_argument(evaluator, function, args, 0, t)?;
            let min = scalar_argument(evaluator, function, args, 1, t)?;
            let max = scalar_argument(evaluator, function, args, 2, t)?;
            if min > max {
                return Ok(Some(Value::Vector(Vec::new())));
            }
            map_floats(series, |v| v.max(min).min(max))
        }
        "clamp_min" => {
            let series = vector_argument(evaluator, function, args, 0, t)?;
            let min = scalar_argument(evaluator, function, args, 1, t)?;
            map_floats(series, |v| v.max(min))
        }
        "clamp_max" => {
            let series = vector_argument(evaluator, function, args, 0, t)?;
            let max = scalar_argument(evaluator, function, args, 1, t)?;
            map_floats(series, |v| v.min(max))
        }
        _ => return Ok(None),
    }))
}

/// The functions over native histograms (QL-5), or `None` for any
/// other name. `histogram_quantile` and `histogram_fraction` read
/// classic `le` buckets as well.
fn call_histogram(
    evaluator: &Evaluator<'_>,
    function: &str,
    args: &[PromExpr],
    t: i64,
) -> Result<Option<Value>, KernelError> {
    let reader: Option<fn(&Histogram) -> f64> = match function {
        "histogram_count" => Some(|h| h.count),
        "histogram_sum" => Some(|h| h.sum),
        "histogram_avg" => Some(|h| h.sum / h.count),
        "histogram_stddev" => Some(|h| h.variance().sqrt()),
        "histogram_stdvar" => Some(Histogram::variance),
        _ => None,
    };
    Ok(Some(match function {
        _ if reader.is_some() => {
            let read = reader.unwrap_or(|h| h.count);
            let series = vector_argument(evaluator, function, args, 0, t)?;
            Value::Vector(
                series
                    .into_iter()
                    .filter_map(|(labels, point)| {
                        let histogram = point.histogram()?;
                        Some((without_name(labels), Point::Float(read(histogram))))
                    })
                    .collect(),
            )
        }
        "histogram_quantile" => {
            let q = scalar_argument(evaluator, function, args, 0, t)?;
            let vector = vector_argument(evaluator, function, args, 1, t)?;
            Value::Vector(histogram_quantile(q, vector))
        }
        "histogram_fraction" => {
            let lower = scalar_argument(evaluator, function, args, 0, t)?;
            let upper = scalar_argument(evaluator, function, args, 1, t)?;
            let vector = vector_argument(evaluator, function, args, 2, t)?;
            Value::Vector(histogram_fraction(lower, upper, vector))
        }
        _ => return Ok(None),
    }))
}

fn seconds(t: i64) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let s = t as f64 / 1e9;
    s
}

/// The float samples of `series`, names dropped, mapped through `f`;
/// histogram samples drop out.
fn map_floats(series: Vec<(Labels, Point)>, f: impl Fn(f64) -> f64) -> Value {
    Value::Vector(
        float_samples(series)
            .into_iter()
            .map(|(labels, v)| (without_name(labels), Point::Float(f(v))))
            .collect(),
    )
}

/// The float samples of a vector.
fn float_samples(series: Vec<(Labels, Point)>) -> Vec<(Labels, f64)> {
    series
        .into_iter()
        .filter_map(|(labels, v)| Some((labels, v.float()?)))
        .collect()
}

/// The float samples of a series, in time order.
fn floats_of(samples: &[Sample]) -> Vec<(i64, f64)> {
    samples
        .iter()
        .filter_map(|(t, v)| Some((*t, v.float()?)))
        .collect()
}

fn math_function(name: &str) -> Option<fn(f64) -> f64> {
    Some(match name {
        "abs" => f64::abs,
        "ceil" => f64::ceil,
        "floor" => f64::floor,
        "exp" => f64::exp,
        "sqrt" => f64::sqrt,
        "ln" => f64::ln,
        "log2" => f64::log2,
        "log10" => f64::log10,
        "sgn" => |v: f64| {
            if v > 0.0 {
                1.0
            } else if v < 0.0 {
                -1.0
            } else {
                v
            }
        },
        "sin" => f64::sin,
        "cos" => f64::cos,
        "tan" => f64::tan,
        "asin" => f64::asin,
        "acos" => f64::acos,
        "atan" => f64::atan,
        "sinh" => f64::sinh,
        "cosh" => f64::cosh,
        "tanh" => f64::tanh,
        "asinh" => f64::asinh,
        "acosh" => f64::acosh,
        "atanh" => f64::atanh,
        "deg" => f64::to_degrees,
        "rad" => f64::to_radians,
        _ => return None,
    })
}

fn argument<'a>(
    function: &str,
    args: &'a [PromExpr],
    index: usize,
) -> Result<&'a PromExpr, KernelError> {
    args.get(index)
        .ok_or_else(|| unsupported(format!("{function} needs {} arguments", index + 1)))
}

fn range_argument(
    evaluator: &Evaluator<'_>,
    function: &str,
    args: &[PromExpr],
    index: usize,
    t: i64,
) -> Result<Vec<Series>, KernelError> {
    let arg = argument(function, args, index)?;
    evaluator
        .range(arg, t)
        .map_err(|_| unsupported(format!("{function} needs a range vector")))
}

fn vector_argument(
    evaluator: &Evaluator<'_>,
    function: &str,
    args: &[PromExpr],
    index: usize,
    t: i64,
) -> Result<Vec<(Labels, Point)>, KernelError> {
    match evaluator.eval(argument(function, args, index)?, t)? {
        Value::Vector(vector) => Ok(vector),
        other => Err(unsupported(format!(
            "{function} needs an instant vector, found {}",
            super::kind_of(&other)
        ))),
    }
}

fn scalar_argument(
    evaluator: &Evaluator<'_>,
    function: &str,
    args: &[PromExpr],
    index: usize,
    t: i64,
) -> Result<f64, KernelError> {
    match evaluator.eval(argument(function, args, index)?, t)? {
        Value::Scalar(v) => Ok(v),
        other => Err(unsupported(format!(
            "{function} needs a number, found {}",
            super::kind_of(&other)
        ))),
    }
}

/// The end of the window a range argument covers at `t`.
fn range_end(expr: &PromExpr, t: i64) -> i64 {
    match expr {
        PromExpr::Selector { offset, at, .. } | PromExpr::Subquery { offset, at, .. } => {
            at.unwrap_or(t) - offset
        }
        _ => t,
    }
}

/// The length of a range argument's window in nanoseconds.
fn range_of(expr: &PromExpr) -> i64 {
    match expr {
        PromExpr::Selector {
            range: Some(range), ..
        }
        | PromExpr::Subquery { range, .. } => i64::try_from(range.as_nanos()).unwrap_or(i64::MAX),
        _ => 0,
    }
}

/// The labels `absent` reports: a selector's equality matchers.
fn absent_labels(expr: &PromExpr) -> Labels {
    match expr {
        PromExpr::Selector { equal, .. } => equal.iter().cloned().collect(),
        _ => Labels::new(),
    }
}

/// A function over the samples of one series in the window ending at
/// `end` and `range` long, reading the float samples.
fn range_function(function: &str, samples: &[Sample], end: i64, range: i64) -> Option<Point> {
    float_range_function(function, &floats_of(samples), end, range).map(Point::Float)
}

/// A function over the float samples of one series in the window
/// ending at `end` and `range` long. `changes` compares values
/// exactly, as Prometheus does.
#[allow(clippy::cast_precision_loss, clippy::float_cmp)]
fn float_range_function(
    function: &str,
    samples: &[(i64, f64)],
    end: i64,
    range: i64,
) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let values = || samples.iter().map(|(_, v)| *v);
    let n = samples.len() as f64;
    Some(match function {
        "rate" => extrapolated_rate(samples, end - range, end, true, true)?,
        "increase" => extrapolated_rate(samples, end - range, end, true, false)?,
        "delta" => extrapolated_rate(samples, end - range, end, false, false)?,
        "irate" | "idelta" => {
            let (prev, last) = (
                samples[samples.len().checked_sub(2)?],
                samples[samples.len() - 1],
            );
            let mut change = last.1 - prev.1;
            if function == "irate" && last.1 < prev.1 {
                change = last.1;
            }
            if function == "irate" {
                let seconds = (last.0 - prev.0) as f64 / 1e9;
                if seconds <= 0.0 {
                    return None;
                }
                change / seconds
            } else {
                change
            }
        }
        "changes" => samples
            .windows(2)
            .filter(|w| w[0].1 != w[1].1 && !(w[0].1.is_nan() && w[1].1.is_nan()))
            .count() as f64,
        "resets" => samples.windows(2).filter(|w| w[1].1 < w[0].1).count() as f64,
        "deriv" => linear_regression(samples, samples[0].0)?.0,
        "avg_over_time" => values().sum::<f64>() / n,
        "min_over_time" => values().fold(f64::NAN, super::nan_min),
        "max_over_time" => values().fold(f64::NAN, super::nan_max),
        "sum_over_time" => values().sum(),
        "count_over_time" => n,
        "last_over_time" => samples[samples.len() - 1].1,
        "present_over_time" => 1.0,
        "stddev_over_time" | "stdvar_over_time" => {
            let mean = values().sum::<f64>() / n;
            let variance = values().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
            if function == "stddev_over_time" {
                variance.sqrt()
            } else {
                variance
            }
        }
        "mad_over_time" => {
            let mut all: Vec<f64> = values().collect();
            let median = quantile(0.5, &mut all);
            let mut deviations: Vec<f64> = all.iter().map(|v| (v - median).abs()).collect();
            quantile(0.5, &mut deviations)
        }
        _ => return None,
    })
}

/// Prometheus' `extrapolatedRate`: the change over the window, counter
/// resets added back, extrapolated to the window's edges unless the
/// samples sit far from them, and divided by the window for a rate.
#[allow(clippy::cast_precision_loss)]
fn extrapolated_rate(
    samples: &[(i64, f64)],
    start: i64,
    end: i64,
    counter: bool,
    rate: bool,
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let (first, last) = (samples[0], samples[samples.len() - 1]);
    let mut result = last.1 - first.1;
    if counter {
        let mut previous = first.1;
        for &(_, value) in &samples[1..] {
            if value < previous {
                result += previous;
            }
            previous = value;
        }
    }
    // The operations and their order are Prometheus' own
    // (extrapolatedRate), so that the last bit of a rate agrees with it:
    // a quantile whose rank falls on a bucket boundary is decided by it.
    let seconds = |nanos: i64| nanos as f64 / 1e9;
    let mut duration_to_start = seconds(first.0 - start);
    let mut duration_to_end = seconds(end - last.0);
    let sampled_interval = seconds(last.0 - first.0);
    let average_between_samples = sampled_interval / (samples.len() - 1) as f64;
    let threshold = average_between_samples * 1.1;
    let mut extrapolate_to = sampled_interval;
    if duration_to_start >= threshold {
        duration_to_start = average_between_samples / 2.0;
    }
    if counter && result > 0.0 && first.1 >= 0.0 {
        // A counter cannot be negative: the start is at most where the
        // slope reaches zero.
        let duration_to_zero = sampled_interval * (first.1 / result);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }
    extrapolate_to += duration_to_start;
    if duration_to_end >= threshold {
        duration_to_end = average_between_samples / 2.0;
    }
    extrapolate_to += duration_to_end;
    let mut factor = extrapolate_to / sampled_interval;
    if rate {
        factor /= seconds(end - start);
    }
    Some(result * factor)
}

/// The slope and intercept of the least-squares line through `samples`,
/// time measured in seconds from `intercept_time`.
#[allow(clippy::cast_precision_loss, clippy::float_cmp)]
fn linear_regression(samples: &[(i64, f64)], intercept_time: i64) -> Option<(f64, f64)> {
    if samples.len() < 2 {
        return None;
    }
    let n = samples.len() as f64;
    let (mut times, mut values, mut cross, mut squares) = (0.0, 0.0, 0.0, 0.0);
    for &(t, v) in samples {
        let x = (t - intercept_time) as f64 / 1e9;
        times += x;
        values += v;
        cross += x * v;
        squares += x * x;
    }
    let cov = cross - times * values / n;
    let var = squares - times * times / n;
    if var == 0.0 {
        return Some((0.0, values / n));
    }
    let slope = cov / var;
    let intercept = values / n - slope * times / n;
    Some((slope, intercept))
}

/// Holt-Winters double exponential smoothing with `sf` for the level
/// and `tf` for the trend.
fn holt_winters(samples: &[(i64, f64)], sf: f64, tf: f64) -> Option<f64> {
    if samples.len() < 2 || !(0.0..=1.0).contains(&sf) || !(0.0..=1.0).contains(&tf) {
        return None;
    }
    let mut level = samples[0].1;
    let mut trend = samples[1].1 - samples[0].1;
    for &(_, x) in &samples[1..] {
        let next = sf * x + (1.0 - sf) * (level + trend);
        trend = tf * (next - level) + (1.0 - tf) * trend;
        level = next;
    }
    Some(level)
}

fn label_replace(
    evaluator: &Evaluator<'_>,
    args: &[PromExpr],
    t: i64,
) -> Result<Value, KernelError> {
    let series = vector_argument(evaluator, "label_replace", args, 0, t)?;
    let text = |i: usize| -> Result<String, KernelError> {
        match args.get(i) {
            Some(PromExpr::Text(s)) => Ok(s.clone()),
            _ => Err(unsupported(
                "label_replace needs strings for its labels and pattern",
            )),
        }
    };
    let (dst, replacement, src, pattern) = (text(1)?, text(2)?, text(3)?, text(4)?);
    if !valid_label_name(&dst) {
        return Err(unsupported(format!(
            "label_replace: invalid destination label name: {dst}"
        )));
    }
    let regex = regex::Regex::new(&format!("^(?:{pattern})$"))
        .map_err(|e| unsupported(format!("label_replace: invalid regular expression: {e}")))?;
    let replaced = series
        .into_iter()
        .map(|(mut labels, v)| {
            let value = labels.get(&src).cloned().unwrap_or_default();
            if let Some(captures) = regex.captures(&value) {
                let mut out = String::new();
                captures.expand(&replacement, &mut out);
                if out.is_empty() {
                    labels.remove(&dst);
                } else {
                    labels.insert(dst.clone(), out);
                }
            }
            (labels, v)
        })
        .collect();
    Ok(Value::Vector(unique(replaced)?))
}

/// A label name as Prometheus validates one: `[a-zA-Z_][a-zA-Z0-9_]*`.
fn valid_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// `series` as a vector: every label set once, or the error Prometheus
/// gives when a label function makes two series alike.
fn unique(series: Vec<(Labels, Point)>) -> Result<Vec<(Labels, Point)>, KernelError> {
    let mut seen = std::collections::BTreeSet::new();
    for (labels, _) in &series {
        if !seen.insert(labels) {
            return Err(unsupported(
                "vector cannot contain metrics with the same labelset",
            ));
        }
    }
    Ok(series)
}

fn label_join(evaluator: &Evaluator<'_>, args: &[PromExpr], t: i64) -> Result<Value, KernelError> {
    let series = vector_argument(evaluator, "label_join", args, 0, t)?;
    let text = |i: usize| -> Result<String, KernelError> {
        match args.get(i) {
            Some(PromExpr::Text(s)) => Ok(s.clone()),
            _ => Err(unsupported("label_join needs strings for its labels")),
        }
    };
    let dst = text(1)?;
    let separator = text(2)?;
    let sources: Vec<String> = (3..args.len()).map(text).collect::<Result<_, _>>()?;
    let joined = series
        .into_iter()
        .map(|(mut labels, v)| {
            let joined: Vec<&str> = sources
                .iter()
                .map(|s| labels.get(s).map_or("", String::as_str))
                .collect();
            let joined = joined.join(&separator);
            if joined.is_empty() {
                labels.remove(&dst);
            } else {
                labels.insert(dst.clone(), joined);
            }
            (labels, v)
        })
        .collect();
    Ok(Value::Vector(unique(joined)?))
}

/// The classic buckets of a label set: upper bound and cumulative count.
type ClassicBuckets = Vec<(f64, f64)>;
/// A vector split into its native histogram samples and its classic
/// buckets by label set.
type Split = (
    Vec<(Labels, Box<Histogram>)>,
    BTreeMap<Labels, ClassicBuckets>,
);

/// The native histogram samples of `vector` and its classic buckets
/// grouped by label set without `le`. A label set that has both at
/// once is dropped from both sides, as Prometheus does.
fn split_histograms(vector: Vec<(Labels, Point)>) -> Split {
    let mut natives = Vec::new();
    let mut classics: BTreeMap<Labels, ClassicBuckets> = BTreeMap::new();
    for (mut labels, point) in vector {
        match point {
            Point::Histogram(histogram) => natives.push((labels, histogram)),
            Point::Float(count) => {
                let Some(le) = labels.remove("le") else {
                    continue;
                };
                let Ok(upper) = le.parse::<f64>() else {
                    continue;
                };
                classics.entry(labels).or_default().push((upper, count));
            }
        }
    }
    let kept = natives
        .into_iter()
        .filter(|(labels, _)| classics.remove(labels).is_none())
        .collect();
    (kept, classics)
}

/// `histogram_quantile(q, v)`: of each native histogram sample, and of
/// each classic histogram's `le` buckets with Prometheus' interpolation
/// inside the bucket that holds the rank.
fn histogram_quantile(q: f64, vector: Vec<(Labels, Point)>) -> Vec<(Labels, Point)> {
    let (natives, classics) = split_histograms(vector);
    let mut out: Vec<(Labels, Point)> = natives
        .into_iter()
        .map(|(labels, histogram)| (without_name(labels), Point::Float(histogram.quantile(q))))
        .collect();
    out.extend(classics.into_iter().map(|(labels, mut buckets)| {
        buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
        (
            without_name(labels),
            Point::Float(bucket_quantile(q, &mut buckets)),
        )
    }));
    out
}

/// `histogram_fraction(lower, upper, v)`: the share of observations
/// between the bounds, of each native histogram sample and of each
/// classic histogram's `le` buckets.
fn histogram_fraction(
    lower: f64,
    upper: f64,
    vector: Vec<(Labels, Point)>,
) -> Vec<(Labels, Point)> {
    let (natives, classics) = split_histograms(vector);
    let mut out: Vec<(Labels, Point)> = natives
        .into_iter()
        .map(|(labels, histogram)| {
            (
                without_name(labels),
                Point::Float(histogram.fraction(lower, upper)),
            )
        })
        .collect();
    out.extend(classics.into_iter().map(|(labels, mut buckets)| {
        (
            without_name(labels),
            Point::Float(bucket_fraction(lower, upper, &mut buckets)),
        )
    }));
    out
}

fn bucket_quantile(q: f64, buckets: &mut [(f64, f64)]) -> f64 {
    if q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    if buckets.len() < 2 || buckets[buckets.len() - 1].0 != f64::INFINITY {
        return f64::NAN;
    }
    // Cumulative counts never decrease; a lower one is a float artefact.
    let mut highest = 0.0f64;
    for bucket in buckets.iter_mut() {
        if bucket.1 < highest {
            bucket.1 = highest;
        } else {
            highest = bucket.1;
        }
    }
    let observations = buckets[buckets.len() - 1].1;
    if observations == 0.0 {
        return f64::NAN;
    }
    let mut rank = q * observations;
    let b = buckets.partition_point(|(_, count)| *count < rank);
    if b == buckets.len() - 1 {
        return buckets[buckets.len() - 2].0;
    }
    if b == 0 && buckets[0].0 <= 0.0 {
        return buckets[0].0;
    }
    let (mut bucket_start, mut count) = (0.0, buckets[b].1);
    if b > 0 {
        bucket_start = buckets[b - 1].0;
        count -= buckets[b - 1].1;
        rank -= buckets[b - 1].1;
    }
    let bucket_end = buckets[b].0;
    bucket_start + (bucket_end - bucket_start) * (rank / count)
}

/// Prometheus' `BucketFraction`: the share of a classic histogram's
/// observations between `lower` and `upper`, interpolated linearly
/// inside a bucket; the lowest bucket starts at 0 unless it ends at or
/// below 0. `NaN` without observations, for a `NaN` bound or without a
/// `+Inf` bucket; 0 when `lower` is not below `upper`.
#[allow(clippy::float_cmp)]
fn bucket_fraction(lower: f64, upper: f64, buckets: &mut Vec<(f64, f64)>) -> f64 {
    buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
    if buckets.last().is_none_or(|last| last.0 != f64::INFINITY) {
        return f64::NAN;
    }
    buckets.dedup_by(|next, kept| {
        let same = next.0 == kept.0;
        if same {
            kept.1 += next.1;
        }
        same
    });
    let count = buckets[buckets.len() - 1].1;
    if count == 0.0 || lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }
    let (mut rank, mut lower_rank, mut upper_rank) = (0.0, None, None);
    let mut lower_bound = if buckets[0].0 <= 0.0 {
        f64::NEG_INFINITY
    } else {
        0.0
    };
    for (i, &(upper_bound, cumulative)) in buckets.iter().enumerate() {
        if i > 0 {
            lower_bound = buckets[i - 1].0;
        }
        // A bucket from -Inf cannot be interpolated into and counts
        // whole; the +Inf one adds nothing, as the formula gives.
        let interpolate = |v: f64| {
            if lower_bound == f64::NEG_INFINITY {
                cumulative
            } else {
                rank + (cumulative - rank) * (v - lower_bound) / (upper_bound - lower_bound)
            }
        };
        if lower_rank.is_none() && lower_bound >= lower {
            lower_rank = Some(rank);
        }
        if upper_rank.is_none() && lower_bound >= upper {
            upper_rank = Some(rank);
        }
        if lower_rank.is_some() && upper_rank.is_some() {
            break;
        }
        if lower_rank.is_none() && lower_bound < lower && upper_bound > lower {
            lower_rank = Some(interpolate(lower));
        }
        if upper_rank.is_none() && lower_bound < upper && upper_bound > upper {
            upper_rank = Some(interpolate(upper));
        }
        if lower_rank.is_some() && upper_rank.is_some() {
            break;
        }
        rank = cumulative;
    }
    let lower_rank = lower_rank.filter(|r| *r <= count).unwrap_or(count);
    let upper_rank = upper_rank.filter(|r| *r <= count).unwrap_or(count);
    (upper_rank - lower_rank) / count
}

/// The calendar functions over a Unix time in seconds, in UTC.
#[allow(clippy::cast_possible_truncation)]
fn calendar(function: &str, seconds: f64) -> f64 {
    let total = seconds.floor() as i64;
    let days = total.div_euclid(86_400);
    let second_of_day = total.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    #[allow(clippy::cast_precision_loss)]
    let out = match function {
        "day_of_month" => day as f64,
        "day_of_week" => (days + 4).rem_euclid(7) as f64,
        "day_of_year" => (days - days_from_civil(year, 1, 1) + 1) as f64,
        "days_in_month" => {
            let (next_year, next_month) = if month == 12 {
                (year + 1, 1)
            } else {
                (year, month + 1)
            };
            (days_from_civil(next_year, next_month, 1) - days_from_civil(year, month, 1)) as f64
        }
        "hour" => (second_of_day / 3600) as f64,
        "minute" => (second_of_day % 3600 / 60) as f64,
        "month" => month as f64,
        _ => year as f64,
    };
    out
}

/// The civil date of a day count since the epoch (Howard Hinnant).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// The day count since the epoch of a civil date (Howard Hinnant).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_quantile_interpolates_within_the_bucket() {
        let mut buckets = vec![(0.1, 1.0), (0.5, 3.0), (1.0, 4.0), (f64::INFINITY, 4.0)];
        // Rank 2 of 4 falls in the second bucket, halfway: 0.1 + 0.4 * 0.5.
        assert!((bucket_quantile(0.5, &mut buckets) - 0.3).abs() < 1e-9);
        assert!((bucket_quantile(1.0, &mut buckets) - 1.0).abs() < 1e-9);
        assert!(
            bucket_quantile(0.5, &mut [(1.0, 1.0)]).is_nan(),
            "no +Inf bucket"
        );
        assert!(bucket_quantile(2.0, &mut buckets).is_infinite());
    }

    /// Prometheus' `BucketFraction` over classic buckets.
    #[test]
    fn bucket_fraction_interpolates_linearly() {
        let buckets = || vec![(0.1, 1.0), (0.5, 3.0), (1.0, 4.0), (f64::INFINITY, 4.0)];
        let near = |actual: f64, expected: f64| (actual - expected).abs() < 1e-9;
        assert!(near(bucket_fraction(0.1, 0.5, &mut buckets()), 0.5));
        assert!(near(bucket_fraction(0.0, 0.1, &mut buckets()), 0.25));
        // Halfway into (0.1, 0.5]: 1 + 2 * 0.5 = 2 of 4 observations.
        assert!(near(bucket_fraction(0.0, 0.3, &mut buckets()), 0.5));
        assert!(near(bucket_fraction(0.3, 1.0, &mut buckets()), 0.5));
        assert!(near(
            bucket_fraction(f64::NEG_INFINITY, f64::INFINITY, &mut buckets()),
            1.0
        ));
        assert!(
            near(bucket_fraction(1.0, 2.0, &mut buckets()), 0.0),
            "nothing above 1 but +Inf"
        );
        assert!(near(bucket_fraction(0.5, 0.1, &mut buckets()), 0.0));
        assert!(bucket_fraction(f64::NAN, 1.0, &mut buckets()).is_nan());
        assert!(bucket_fraction(0.0, 1.0, &mut vec![(1.0, 1.0)]).is_nan());
        assert!(bucket_fraction(0.0, 1.0, &mut vec![(1.0, 0.0), (f64::INFINITY, 0.0)]).is_nan());
        let mut negative = vec![(-1.0, 2.0), (1.0, 3.0), (f64::INFINITY, 4.0)];
        assert!(
            near(bucket_fraction(-5.0, -1.0, &mut negative), 0.0),
            "nothing of a bucket from -Inf lies within a finite range"
        );
        assert!(near(bucket_fraction(-5.0, 0.0, &mut negative), 0.125));
        let mut doubled = vec![(0.5, 1.0), (0.5, 1.0), (f64::INFINITY, 4.0)];
        assert!(
            near(bucket_fraction(0.0, 0.5, &mut doubled), 0.5),
            "same bounds coalesce"
        );
    }

    #[test]
    fn calendar_functions_read_utc() {
        // 2023-11-14T22:13:20Z, a Tuesday.
        let t = 1_700_000_000.0;
        assert!((calendar("year", t) - 2023.0).abs() < f64::EPSILON);
        assert!((calendar("month", t) - 11.0).abs() < f64::EPSILON);
        assert!((calendar("day_of_month", t) - 14.0).abs() < f64::EPSILON);
        assert!((calendar("day_of_week", t) - 2.0).abs() < f64::EPSILON);
        assert!((calendar("day_of_year", t) - 318.0).abs() < f64::EPSILON);
        assert!((calendar("days_in_month", t) - 30.0).abs() < f64::EPSILON);
        assert!((calendar("hour", t) - 22.0).abs() < f64::EPSILON);
        assert!((calendar("minute", t) - 13.0).abs() < f64::EPSILON);
    }

    #[test]
    fn regression_and_smoothing() {
        let samples: Vec<(i64, f64)> = (0..5i32)
            .map(|i| (i64::from(i) * 1_000_000_000, 2.0 * f64::from(i) + 1.0))
            .collect();
        let (slope, intercept) = linear_regression(&samples, 0).unwrap();
        assert!((slope - 2.0).abs() < 1e-9 && (intercept - 1.0).abs() < 1e-9);
        assert!(holt_winters(&samples, 0.5, 0.5).unwrap() > 8.0);
        assert!(extrapolated_rate(&samples[..1], 0, 10, true, true).is_none());
    }

    #[test]
    fn label_functions_validate_names_and_keep_label_sets_apart() {
        assert!(valid_label_name("job_1"));
        assert!(valid_label_name("_x"));
        assert!(!valid_label_name("~invalid"));
        assert!(!valid_label_name("1a"));
        assert!(!valid_label_name(""));
        let labels = |job: &str| Labels::from([("job".to_owned(), job.to_owned())]);
        assert!(
            unique(vec![
                (labels("a"), Point::Float(1.0)),
                (labels("b"), Point::Float(2.0))
            ])
            .is_ok()
        );
        assert!(matches!(
            unique(vec![(labels("a"), Point::Float(1.0)), (labels("a"), Point::Float(2.0))]),
            Err(KernelError::Unsupported(message)) if message.contains("same labelset")
        ));
    }

    /// Prometheus caps the start extrapolation at half a sample interval
    /// before it considers the counter's zero point; a counter that
    /// started late in the range gets the zero point only when it is
    /// nearer than that cap.
    #[test]
    fn rate_takes_the_zero_point_after_the_extrapolation_cap() {
        // Samples 10 s apart from t = 30 s, values 5, 15: the slope reaches
        // zero 5 s before the first sample; the range starts at 0.
        let samples: Vec<(i64, f64)> = vec![(30_000_000_000, 5.0), (40_000_000_000, 15.0)];
        let increase = extrapolated_rate(&samples, 0, 50_000_000_000, true, false).unwrap();
        // 30 s to the start is past the 11 s threshold: the cap of 5 s
        // applies, and the zero point, 5 s, is not nearer. End: 10 s to
        // the end is under the threshold. Interval 10 + 5 + 10 = 25 s.
        assert!((increase - 10.0 * 25.0 / 10.0).abs() < 1e-9, "{increase}");
        let rate = extrapolated_rate(&samples, 0, 50_000_000_000, true, true).unwrap();
        assert!((rate - 25.0 / 50.0).abs() < 1e-12, "{rate}");
    }
}
