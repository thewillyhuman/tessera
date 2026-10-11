// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `PromQL` evaluation over series in memory (section 9.3, QL-5): the
//! semantics of Prometheus 3, step by step.
//!
//! An instant selector takes the last sample of each series within the
//! lookback delta, a range selector the samples in `(t - range, t]`,
//! both left-open; staleness markers end a series. Functions over range
//! vectors follow Prometheus' algorithms, counter resets and the
//! extrapolation of `rate` included; aggregations and binary operators
//! follow its label rules; a subquery evaluates its expression at the
//! substeps of its range. The evaluator is pure: the operator that
//! calls it turns rows into series and series back into rows.

mod functions;

use std::collections::{BTreeMap, BTreeSet};

use tessera_ir::promql::{Group, Matching, PromAggregate, PromExpr, PromOp};
use tessera_ir::{LOOKBACK, SeriesGrouping};

use crate::KernelError;

/// The labels of a series, `__name__` among them.
pub type Labels = BTreeMap<String, String>;
/// A sample: time in nanoseconds and value.
pub type Sample = (i64, f64);

/// The label that names a metric.
pub const NAME_LABEL: &str = "__name__";
/// The bit pattern of Prometheus' staleness marker.
pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;
/// The default resolution of a subquery without a step.
pub const DEFAULT_SUBQUERY_STEP: i64 = 60_000_000_000;

/// A series: its labels and its samples in time order.
#[derive(Clone, Debug, PartialEq)]
pub struct Series {
    /// The labels.
    pub labels: Labels,
    /// The samples, sorted by time.
    pub samples: Vec<Sample>,
}

/// The steps of an evaluation, in nanoseconds; `step` zero evaluates
/// once at `start`.
#[derive(Clone, Copy, Debug)]
pub struct Params {
    /// The first step.
    pub start: i64,
    /// The last step, inclusive.
    pub end: i64,
    /// The distance between steps.
    pub step: i64,
}

/// A value during evaluation.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// A number.
    Scalar(f64),
    /// A string.
    Text(String),
    /// Series with one value each.
    Vector(Vec<(Labels, f64)>),
    /// Series with samples over a range.
    Matrix(Vec<Series>),
}

/// One output row: a step, a series' labels (none for a scalar) and the
/// value.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    /// The step.
    pub time: i64,
    /// The labels; `None` for a scalar.
    pub labels: Option<Labels>,
    /// The value.
    pub value: f64,
}

/// Whether `value` is a staleness marker.
#[must_use]
pub fn is_stale(value: f64) -> bool {
    value.to_bits() == STALE_NAN_BITS
}

/// Evaluates `expr` over `inputs` at every step of `params`.
///
/// # Errors
///
/// `Unsupported` for a function the evaluator does not know, a wrong
/// argument, or a vector match that is not explicit.
pub fn evaluate(
    expr: &PromExpr,
    inputs: &[Vec<Series>],
    params: Params,
) -> Result<Vec<Row>, KernelError> {
    let evaluator = Evaluator { inputs };
    let mut rows = Vec::new();
    let mut t = params.start;
    loop {
        match evaluator.eval(expr, t)? {
            Value::Scalar(value) => rows.push(Row {
                time: t,
                labels: None,
                value,
            }),
            Value::Vector(vector) => rows.extend(vector.into_iter().map(|(labels, value)| Row {
                time: t,
                labels: Some(labels),
                value,
            })),
            // A range vector at an instant lists its samples at their own
            // times; a string has no rows, the API renders it.
            Value::Matrix(series) => {
                for s in series {
                    rows.extend(s.samples.into_iter().map(|(time, value)| Row {
                        time,
                        labels: Some(s.labels.clone()),
                        value,
                    }));
                }
            }
            Value::Text(_) => {}
        }
        if params.step <= 0 || t >= params.end {
            break;
        }
        t = t.saturating_add(params.step);
        if t > params.end {
            break;
        }
    }
    Ok(rows)
}

fn unsupported(message: impl Into<String>) -> KernelError {
    KernelError::Unsupported(message.into())
}

struct Evaluator<'a> {
    inputs: &'a [Vec<Series>],
}

impl Evaluator<'_> {
    /// The value of `expr` at `t`.
    fn eval(&self, expr: &PromExpr, t: i64) -> Result<Value, KernelError> {
        match expr {
            PromExpr::Number(n) => Ok(Value::Scalar(*n)),
            PromExpr::Text(s) => Ok(Value::Text(s.clone())),
            PromExpr::Selector {
                input,
                range: None,
                offset,
                at,
                ..
            } => {
                let t = at.unwrap_or(t) - offset;
                Ok(Value::Vector(self.instant(*input, t)))
            }
            PromExpr::Selector { .. } | PromExpr::Subquery { .. } => {
                Ok(Value::Matrix(self.range(expr, t)?))
            }
            PromExpr::Negate(inner) => Ok(match self.eval(inner, t)? {
                Value::Scalar(v) => Value::Scalar(-v),
                Value::Vector(vector) => Value::Vector(
                    vector
                        .into_iter()
                        .map(|(labels, v)| (without_name(labels), -v))
                        .collect(),
                ),
                other => return Err(unsupported(format!("cannot negate {}", kind_of(&other)))),
            }),
            PromExpr::Binary {
                op,
                left,
                right,
                matching,
                bool_result,
            } => {
                let (l, r) = (self.eval(left, t)?, self.eval(right, t)?);
                binary(*op, l, r, matching.as_ref(), *bool_result)
            }
            PromExpr::Aggregate {
                op,
                input,
                parameter,
                grouping,
            } => {
                let parameter = match parameter {
                    Some(p) => Some(self.eval(p, t)?),
                    None => None,
                };
                let Value::Vector(vector) = self.eval(input, t)? else {
                    return Err(unsupported(format!(
                        "{} needs an instant vector",
                        op.name()
                    )));
                };
                aggregate(*op, vector, parameter.as_ref(), grouping.as_ref())
            }
            PromExpr::Call { function, args } => functions::call(self, function, args, t),
        }
    }

    /// The samples of `expr`, a range selector or subquery, at `t`.
    fn range(&self, expr: &PromExpr, t: i64) -> Result<Vec<Series>, KernelError> {
        match expr {
            PromExpr::Selector {
                input,
                range: Some(range),
                offset,
                at,
                ..
            } => {
                let end = at.unwrap_or(t) - offset;
                let start = end - i64::try_from(range.as_nanos()).unwrap_or(i64::MAX);
                Ok(self.window(*input, start, end))
            }
            PromExpr::Subquery {
                input,
                range,
                step,
                offset,
                at,
            } => {
                let end = at.unwrap_or(t) - offset;
                let range = i64::try_from(range.as_nanos()).unwrap_or(i64::MAX);
                let step = step
                    .map_or(DEFAULT_SUBQUERY_STEP, |s| {
                        i64::try_from(s.as_nanos()).unwrap_or(i64::MAX)
                    })
                    .max(1);
                // Substeps sit on multiples of the step, inside (end - range, end].
                let mut sub = (end - range).div_euclid(step) * step;
                if sub <= end - range {
                    sub += step;
                }
                let mut series: BTreeMap<Labels, Vec<Sample>> = BTreeMap::new();
                while sub <= end {
                    match self.eval(input, sub)? {
                        Value::Vector(vector) => {
                            for (labels, value) in vector {
                                series.entry(labels).or_default().push((sub, value));
                            }
                        }
                        Value::Scalar(value) => {
                            series.entry(Labels::new()).or_default().push((sub, value));
                        }
                        other => {
                            return Err(unsupported(format!(
                                "a subquery needs an instant vector, not {}",
                                kind_of(&other)
                            )));
                        }
                    }
                    sub += step;
                }
                Ok(series
                    .into_iter()
                    .map(|(labels, samples)| Series { labels, samples })
                    .collect())
            }
            other => Err(unsupported(format!(
                "expected a range vector, found {}",
                kind_of_expr(other)
            ))),
        }
    }

    /// The last sample of each series of `input` within the lookback
    /// delta before `t`, staleness honoured.
    fn instant(&self, input: usize, t: i64) -> Vec<(Labels, f64)> {
        let lookback = i64::try_from(LOOKBACK.as_nanos()).unwrap_or(i64::MAX);
        let since = t - lookback;
        self.inputs
            .get(input)
            .map(|series| {
                series
                    .iter()
                    .filter_map(|s| {
                        let index = s.samples.partition_point(|(ts, _)| *ts <= t);
                        let (ts, value) = *s.samples.get(index.checked_sub(1)?)?;
                        (ts > since && !is_stale(value)).then(|| (s.labels.clone(), value))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The samples of each series of `input` in `(start, end]`, stale
    /// markers left out.
    fn window(&self, input: usize, start: i64, end: i64) -> Vec<Series> {
        self.inputs
            .get(input)
            .map(|series| {
                series
                    .iter()
                    .filter_map(|s| {
                        let from = s.samples.partition_point(|(ts, _)| *ts <= start);
                        let to = s.samples.partition_point(|(ts, _)| *ts <= end);
                        let samples: Vec<Sample> = s.samples[from..to]
                            .iter()
                            .copied()
                            .filter(|(_, v)| !is_stale(*v))
                            .collect();
                        (!samples.is_empty()).then(|| Series {
                            labels: s.labels.clone(),
                            samples,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Scalar(_) => "a scalar",
        Value::Text(_) => "a string",
        Value::Vector(_) => "an instant vector",
        Value::Matrix(_) => "a range vector",
    }
}

fn kind_of_expr(expr: &PromExpr) -> &'static str {
    match expr {
        PromExpr::Number(_) => "a number",
        PromExpr::Text(_) => "a string",
        PromExpr::Selector { range: Some(_), .. } | PromExpr::Subquery { .. } => "a range vector",
        _ => "an instant vector",
    }
}

/// `labels` without `__name__`.
fn without_name(mut labels: Labels) -> Labels {
    labels.remove(NAME_LABEL);
    labels
}

/// The labels that identify a series for matching: all but `__name__`,
/// narrowed by `on` or widened by `ignoring`.
fn signature(labels: &Labels, matching: Option<&Matching>) -> Labels {
    match matching {
        Some(Matching {
            on: true,
            labels: on,
            ..
        }) => labels
            .iter()
            .filter(|(k, _)| on.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        Some(Matching {
            on: false,
            labels: ignoring,
            ..
        }) => labels
            .iter()
            .filter(|(k, _)| k.as_str() != NAME_LABEL && !ignoring.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        None => labels
            .iter()
            .filter(|(k, _)| k.as_str() != NAME_LABEL)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}

/// `left op right` on numbers; comparisons yield 1 or 0. `==` and `!=`
/// are exact, as `PromQL` defines them.
#[allow(clippy::float_cmp)]
fn arithmetic(op: PromOp, left: f64, right: f64) -> f64 {
    match op {
        PromOp::Add => left + right,
        PromOp::Sub => left - right,
        PromOp::Mul => left * right,
        PromOp::Div => left / right,
        PromOp::Mod => left % right,
        PromOp::Pow => left.powf(right),
        PromOp::Atan2 => left.atan2(right),
        PromOp::Eq => f64::from(u8::from(left == right)),
        PromOp::NotEq => f64::from(u8::from(left != right)),
        PromOp::Gt => f64::from(u8::from(left > right)),
        PromOp::Lt => f64::from(u8::from(left < right)),
        PromOp::GtEq => f64::from(u8::from(left >= right)),
        PromOp::LtEq => f64::from(u8::from(left <= right)),
        PromOp::And | PromOp::Or | PromOp::Unless => f64::NAN,
    }
}

/// Whether a comparison keeps the sample: its result is 1.
#[allow(clippy::float_cmp)]
fn holds(op: PromOp, left: f64, right: f64) -> bool {
    arithmetic(op, left, right) == 1.0
}

/// A binary operation between two values.
fn binary(
    op: PromOp,
    left: Value,
    right: Value,
    matching: Option<&Matching>,
    bool_result: bool,
) -> Result<Value, KernelError> {
    if op.is_set() {
        return match (left, right) {
            (Value::Vector(l), Value::Vector(r)) => {
                Ok(Value::Vector(set_operation(op, l, r, matching)))
            }
            (l, r) => Err(unsupported(format!(
                "{} needs two instant vectors, found {} and {}",
                op.symbol(),
                kind_of(&l),
                kind_of(&r)
            ))),
        };
    }
    let drop_name = !op.is_comparison() || bool_result;
    let keep_or_mark = |value: f64, other: f64, swapped: bool| -> Option<f64> {
        let (l, r) = if swapped {
            (other, value)
        } else {
            (value, other)
        };
        if !op.is_comparison() {
            return Some(arithmetic(op, l, r));
        }
        let held = holds(op, l, r);
        if bool_result {
            Some(f64::from(u8::from(held)))
        } else {
            held.then_some(value)
        }
    };
    Ok(match (left, right) {
        (Value::Scalar(l), Value::Scalar(r)) => {
            if op.is_comparison() && !bool_result {
                return Err(unsupported(
                    "a comparison between scalars needs the bool modifier",
                ));
            }
            Value::Scalar(arithmetic(op, l, r))
        }
        (Value::Vector(vector), Value::Scalar(scalar)) => Value::Vector(
            vector
                .into_iter()
                .filter_map(|(labels, v)| {
                    let labels = if drop_name {
                        without_name(labels)
                    } else {
                        labels
                    };
                    keep_or_mark(v, scalar, false).map(|v| (labels, v))
                })
                .collect(),
        ),
        (Value::Scalar(scalar), Value::Vector(vector)) => Value::Vector(
            vector
                .into_iter()
                .filter_map(|(labels, v)| {
                    let labels = if drop_name {
                        without_name(labels)
                    } else {
                        labels
                    };
                    keep_or_mark(v, scalar, true).map(|v| (labels, v))
                })
                .collect(),
        ),
        (Value::Vector(l), Value::Vector(r)) => {
            Value::Vector(vector_match(op, l, r, matching, bool_result)?)
        }
        (l, r) => {
            return Err(unsupported(format!(
                "{} is not defined between {} and {}",
                op.symbol(),
                kind_of(&l),
                kind_of(&r)
            )));
        }
    })
}

/// `and`, `or` and `unless` on the series' signatures.
fn set_operation(
    op: PromOp,
    left: Vec<(Labels, f64)>,
    right: Vec<(Labels, f64)>,
    matching: Option<&Matching>,
) -> Vec<(Labels, f64)> {
    let right_signatures: BTreeSet<Labels> =
        right.iter().map(|(l, _)| signature(l, matching)).collect();
    match op {
        PromOp::And => left
            .into_iter()
            .filter(|(l, _)| right_signatures.contains(&signature(l, matching)))
            .collect(),
        PromOp::Unless => left
            .into_iter()
            .filter(|(l, _)| !right_signatures.contains(&signature(l, matching)))
            .collect(),
        _ => {
            let left_signatures: BTreeSet<Labels> =
                left.iter().map(|(l, _)| signature(l, matching)).collect();
            let mut out = left;
            out.extend(
                right
                    .into_iter()
                    .filter(|(l, _)| !left_signatures.contains(&signature(l, matching))),
            );
            out
        }
    }
}

/// The labels of a one-to-one result: the `on` labels, or the left
/// labels without the ignored ones, `__name__` dropped unless a
/// comparison keeps it.
fn result_labels(left: &Labels, matching: Option<&Matching>, drop_name: bool) -> Labels {
    let mut labels = match matching {
        Some(Matching { on: true, .. }) => signature(left, matching),
        Some(Matching {
            on: false,
            labels: ignoring,
            ..
        }) => left
            .iter()
            .filter(|(k, _)| !ignoring.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        None => left.clone(),
    };
    if drop_name {
        labels.remove(NAME_LABEL);
    }
    labels
}

/// Vector matching: one-to-one, or many-to-one with `group_left` and
/// one-to-many with `group_right`.
fn vector_match(
    op: PromOp,
    left: Vec<(Labels, f64)>,
    right: Vec<(Labels, f64)>,
    matching: Option<&Matching>,
    bool_result: bool,
) -> Result<Vec<(Labels, f64)>, KernelError> {
    let drop_name = !op.is_comparison() || bool_result;
    let group = matching.and_then(|m| m.group.as_ref());
    // The "one" side is indexed by signature; with group_right the sides
    // swap so that the many side is always iterated.
    let swapped = matches!(group, Some(Group { left: false, .. }));
    let (many, one) = if swapped {
        (right, left)
    } else {
        (left, right)
    };
    let mut by_signature: BTreeMap<Labels, (Labels, f64)> = BTreeMap::new();
    for (labels, value) in one {
        let key = signature(&labels, matching);
        if by_signature.insert(key, (labels, value)).is_some() {
            return Err(unsupported(
                "found duplicate series for the match group; many-to-many matching is not allowed",
            ));
        }
    }
    let mut seen: BTreeSet<Labels> = BTreeSet::new();
    let mut out = Vec::new();
    for (labels, value) in many {
        let key = signature(&labels, matching);
        let Some((one_labels, one_value)) = by_signature.get(&key) else {
            continue;
        };
        if group.is_none() && !seen.insert(key) {
            return Err(unsupported(
                "found duplicate series for the match group; many-to-one matching must be explicit (group_left/group_right)",
            ));
        }
        let (l, r) = if swapped {
            (*one_value, value)
        } else {
            (value, *one_value)
        };
        let result = if op.is_comparison() {
            let held = holds(op, l, r);
            if bool_result {
                f64::from(u8::from(held))
            } else if held {
                if swapped { r } else { l }
            } else {
                continue;
            }
        } else {
            arithmetic(op, l, r)
        };
        let mut result_labels = if swapped {
            // group_right keeps the right (many) side's labels.
            let mut labels = labels.clone();
            if drop_name {
                labels.remove(NAME_LABEL);
            }
            labels
        } else {
            result_labels(&labels, matching, drop_name)
        };
        if let Some(Group { include, .. }) = group {
            if !swapped {
                result_labels = labels.clone();
                if drop_name {
                    result_labels.remove(NAME_LABEL);
                }
            }
            for name in include {
                match one_labels.get(name) {
                    Some(v) => {
                        result_labels.insert(name.clone(), v.clone());
                    }
                    None => {
                        result_labels.remove(name);
                    }
                }
            }
        }
        out.push((result_labels, result));
    }
    Ok(out)
}

/// The labels a group keeps under `grouping`.
fn group_labels(labels: &Labels, grouping: Option<&SeriesGrouping>) -> Labels {
    match grouping {
        Some(SeriesGrouping {
            by: true,
            labels: by,
        }) => labels
            .iter()
            .filter(|(k, _)| by.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        Some(SeriesGrouping {
            by: false,
            labels: without,
        }) => labels
            .iter()
            .filter(|(k, _)| k.as_str() != NAME_LABEL && !without.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        None => Labels::new(),
    }
}

/// Prometheus' quantile of `values`: linear interpolation between the
/// ranks around `q * (n - 1)`.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub(crate) fn quantile(q: f64, values: &mut [f64]) -> f64 {
    if values.is_empty() || q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    values.sort_by(f64::total_cmp);
    let n = values.len();
    let rank = q * (n - 1) as f64;
    let lower = rank.floor().max(0.0) as usize;
    let upper = (lower + 1).min(n - 1);
    let weight = rank - lower as f64;
    values[lower] * (1.0 - weight) + values[upper] * weight
}

/// An aggregation over the series of `vector`.
#[allow(clippy::cast_precision_loss)]
fn aggregate(
    op: PromAggregate,
    vector: Vec<(Labels, f64)>,
    parameter: Option<&Value>,
    grouping: Option<&SeriesGrouping>,
) -> Result<Value, KernelError> {
    let scalar_parameter = || match parameter {
        Some(Value::Scalar(v)) => Ok(*v),
        _ => Err(unsupported(format!("{} needs a number", op.name()))),
    };
    let mut groups: BTreeMap<Labels, Vec<(Labels, f64)>> = BTreeMap::new();
    for (labels, value) in vector {
        groups
            .entry(group_labels(&labels, grouping))
            .or_default()
            .push((labels, value));
    }
    let mut out = Vec::new();
    for (key, members) in groups {
        let values: Vec<f64> = members.iter().map(|(_, v)| *v).collect();
        let n = values.len() as f64;
        match op {
            PromAggregate::Sum => out.push((key, values.iter().sum())),
            PromAggregate::Min => out.push((key, values.iter().copied().fold(f64::NAN, nan_min))),
            PromAggregate::Max => out.push((key, values.iter().copied().fold(f64::NAN, nan_max))),
            PromAggregate::Avg => out.push((key, values.iter().sum::<f64>() / n)),
            PromAggregate::Count => out.push((key, n)),
            PromAggregate::Group => out.push((key, 1.0)),
            PromAggregate::Stddev | PromAggregate::Stdvar => {
                let mean = values.iter().sum::<f64>() / n;
                let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
                let value = if op == PromAggregate::Stddev {
                    variance.sqrt()
                } else {
                    variance
                };
                out.push((key, value));
            }
            PromAggregate::Quantile => {
                let q = scalar_parameter()?;
                let mut values = values;
                out.push((key, quantile(q, &mut values)));
            }
            PromAggregate::CountValues => {
                let Some(Value::Text(name)) = parameter else {
                    return Err(unsupported("count_values needs a label name"));
                };
                let mut counts: BTreeMap<String, f64> = BTreeMap::new();
                for value in &values {
                    *counts.entry(format_value(*value)).or_default() += 1.0;
                }
                for (text, count) in counts {
                    let mut labels = key.clone();
                    labels.insert(name.clone(), text);
                    out.push((labels, count));
                }
            }
            PromAggregate::Topk | PromAggregate::Bottomk => {
                let k = scalar_parameter()?;
                if k.is_nan() || k < 1.0 {
                    continue;
                }
                let mut members = members;
                members.sort_by(|a, b| {
                    let order = a.1.total_cmp(&b.1);
                    if op == PromAggregate::Topk {
                        order.reverse()
                    } else {
                        order
                    }
                });
                // NaN ranks last for both.
                members.sort_by_key(|(_, v)| v.is_nan());
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let k = k as usize;
                out.extend(members.into_iter().take(k));
            }
            PromAggregate::Limitk => {
                let k = scalar_parameter()?;
                if k.is_nan() || k < 1.0 {
                    continue;
                }
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let k = k as usize;
                out.extend(members.into_iter().take(k));
            }
            PromAggregate::LimitRatio => {
                let ratio = scalar_parameter()?;
                if ratio.is_nan() {
                    continue;
                }
                let ratio = ratio.clamp(-1.0, 1.0);
                out.extend(members.into_iter().filter(|(labels, _)| {
                    let share = series_share(labels);
                    if ratio >= 0.0 {
                        share < ratio
                    } else {
                        share >= 1.0 + ratio
                    }
                }));
            }
        }
    }
    Ok(Value::Vector(out))
}

/// Where a series falls in `[0, 1)` by a stable hash of its labels, so
/// that `limit_ratio(r)` and `limit_ratio(r - 1)` split a vector into
/// complementary parts, the same on every evaluation.
fn series_share(labels: &Labels) -> f64 {
    let text: Vec<String> = labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
    #[allow(clippy::cast_precision_loss)]
    let share = tessera_types::stable_hash(text.join(",").as_bytes()) as f64 / u64::MAX as f64;
    share.min(1.0 - f64::EPSILON)
}

fn nan_min(acc: f64, v: f64) -> f64 {
    if acc.is_nan() || v < acc { v } else { acc }
}

fn nan_max(acc: f64, v: f64) -> f64 {
    if acc.is_nan() || v > acc { v } else { acc }
}

/// A value as Prometheus formats it in labels: the shortest decimal
/// that reads back exactly.
#[allow(clippy::float_cmp)]
#[must_use]
pub fn format_value(value: f64) -> String {
    if value.is_nan() {
        "NaN".into()
    } else if value.is_infinite() {
        if value > 0.0 {
            "+Inf".into()
        } else {
            "-Inf".into()
        }
    } else if value == value.trunc() && value.abs() < 1e15 {
        format!("{value:.0}")
    } else {
        format!("{value}")
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const S: i64 = 1_000_000_000;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    fn series(pairs: &[(&str, &str)], samples: &[(i64, f64)]) -> Series {
        Series {
            labels: labels(pairs),
            samples: samples.iter().map(|(t, v)| (t * S, *v)).collect(),
        }
    }

    fn selector(input: usize, range: Option<u64>) -> PromExpr {
        PromExpr::Selector {
            input,
            range: range.map(Duration::from_secs),
            offset: 0,
            at: None,
            equal: Vec::new(),
        }
    }

    /// Two counters `requests` (jobs api and web) and a gauge `up`.
    fn inputs() -> Vec<Vec<Series>> {
        vec![
            vec![
                series(
                    &[("__name__", "requests"), ("job", "api")],
                    &[(0, 0.0), (15, 10.0), (30, 20.0), (45, 30.0), (60, 40.0)],
                ),
                series(
                    &[("__name__", "requests"), ("job", "web")],
                    &[(0, 0.0), (15, 5.0), (30, 2.0), (45, 4.0), (60, 6.0)],
                ),
            ],
            vec![series(
                &[("__name__", "up"), ("job", "api")],
                &[(0, 1.0), (30, 1.0)],
            )],
        ]
    }

    fn at(expr: &PromExpr, t: i64) -> Vec<Row> {
        evaluate(
            expr,
            &inputs(),
            Params {
                start: t * S,
                end: t * S,
                step: 0,
            },
        )
        .unwrap()
    }

    fn value_of(rows: &[Row], job: &str) -> f64 {
        rows.iter()
            .find(|r| {
                r.labels
                    .as_ref()
                    .and_then(|l| l.get("job"))
                    .map(String::as_str)
                    == Some(job)
            })
            .map_or(f64::NAN, |r| r.value)
    }

    #[test]
    fn instant_selectors_look_back_five_minutes_and_honour_staleness() {
        let rows = at(&selector(1, None), 100);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].labels.as_ref().unwrap()["__name__"], "up");
        assert!((rows[0].value - 1.0).abs() < f64::EPSILON);
        assert!(
            at(&selector(1, None), 331).is_empty(),
            "301s after the last sample"
        );
        assert!(!at(&selector(1, None), 329).is_empty());
        let stale = vec![vec![series(
            &[("__name__", "x")],
            &[(0, 1.0), (10, f64::from_bits(STALE_NAN_BITS))],
        )]];
        let rows = evaluate(
            &selector(0, None),
            &stale,
            Params {
                start: 20 * S,
                end: 20 * S,
                step: 0,
            },
        )
        .unwrap();
        assert!(rows.is_empty(), "a stale marker ends the series");
    }

    #[test]
    fn rate_extrapolates_and_handles_resets() {
        let rate = PromExpr::Call {
            function: "rate".into(),
            args: vec![selector(0, Some(60))],
        };
        let rows = at(&rate, 60);
        // The window (0s, 60s] holds the samples from 15s on: api grows by
        // 30 over 45s and extrapolates to the window's start, 15s away,
        // which is within 1.1 sample intervals: 40 / 60.
        assert!(
            (value_of(&rows, "api") - 40.0 / 60.0).abs() < 1e-9,
            "{rows:?}"
        );
        // web resets at 30s: (6 - 5) + 5 = 6 over 45s, extrapolated to 8.
        assert!(
            (value_of(&rows, "web") - 8.0 / 60.0).abs() < 1e-9,
            "{rows:?}"
        );
        assert!(
            rows.iter()
                .all(|r| !r.labels.as_ref().unwrap().contains_key("__name__"))
        );
        let increase = PromExpr::Call {
            function: "increase".into(),
            args: vec![selector(0, Some(60))],
        };
        assert!((value_of(&at(&increase, 60), "api") - 40.0).abs() < 1e-9);
        let irate = PromExpr::Call {
            function: "irate".into(),
            args: vec![selector(0, Some(60))],
        };
        assert!((value_of(&at(&irate, 60), "api") - 10.0 / 15.0).abs() < 1e-9);
    }

    /// `limitk` keeps the first k series of a group; `limit_ratio`
    /// keeps a stable share, complementary for `r` and `r - 1`.
    #[test]
    fn limitk_and_limit_ratio_cut_a_vector() {
        let limited = |op, parameter: f64| PromExpr::Aggregate {
            op,
            input: Box::new(selector(0, None)),
            parameter: Some(Box::new(PromExpr::Number(parameter))),
            grouping: None,
        };
        assert_eq!(at(&limited(PromAggregate::Limitk, 1.0), 60).len(), 1);
        assert_eq!(at(&limited(PromAggregate::Limitk, 5.0), 60).len(), 2);
        assert_eq!(at(&limited(PromAggregate::Limitk, 0.0), 60).len(), 0);
        assert_eq!(at(&limited(PromAggregate::LimitRatio, 1.0), 60).len(), 2);
        assert_eq!(at(&limited(PromAggregate::LimitRatio, 0.0), 60).len(), 0);
        let kept = at(&limited(PromAggregate::LimitRatio, 0.5), 60);
        let rest = at(&limited(PromAggregate::LimitRatio, -0.5), 60);
        assert_eq!(kept.len() + rest.len(), 2, "complementary shares");
        assert_eq!(
            at(&limited(PromAggregate::LimitRatio, 0.5), 60).len(),
            kept.len(),
            "stable across evaluations"
        );
    }

    #[test]
    fn aggregations_group_by_and_without() {
        let sum_by = PromExpr::Aggregate {
            op: PromAggregate::Sum,
            input: Box::new(selector(0, None)),
            parameter: None,
            grouping: Some(SeriesGrouping {
                by: true,
                labels: vec!["job".into()],
            }),
        };
        let rows = at(&sum_by, 60);
        assert_eq!(rows.len(), 2);
        assert!((value_of(&rows, "api") - 40.0).abs() < f64::EPSILON);
        let total = PromExpr::Aggregate {
            op: PromAggregate::Sum,
            input: Box::new(selector(0, None)),
            parameter: None,
            grouping: None,
        };
        let rows = at(&total, 60);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].labels.as_ref().unwrap().is_empty());
        assert!((rows[0].value - 46.0).abs() < f64::EPSILON);
        let topk = PromExpr::Aggregate {
            op: PromAggregate::Topk,
            input: Box::new(selector(0, None)),
            parameter: Some(Box::new(PromExpr::Number(1.0))),
            grouping: None,
        };
        let rows = at(&topk, 60);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].labels.as_ref().unwrap()["job"], "api");
        assert_eq!(rows[0].labels.as_ref().unwrap()["__name__"], "requests");
        let count_values = PromExpr::Aggregate {
            op: PromAggregate::CountValues,
            input: Box::new(selector(0, None)),
            parameter: Some(Box::new(PromExpr::Text("v".into()))),
            grouping: None,
        };
        let rows = at(&count_values, 60);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|r| r.labels.as_ref().unwrap()["v"] == "40"));
    }

    #[test]
    fn binary_operators_match_on_labels() {
        let ratio = PromExpr::Binary {
            op: PromOp::Div,
            left: Box::new(selector(0, None)),
            right: Box::new(selector(1, None)),
            matching: None,
            bool_result: false,
        };
        let rows = at(&ratio, 60);
        assert_eq!(rows.len(), 1, "only api has an up series");
        assert!((rows[0].value - 40.0).abs() < f64::EPSILON);
        assert!(!rows[0].labels.as_ref().unwrap().contains_key("__name__"));
        let gt = PromExpr::Binary {
            op: PromOp::Gt,
            left: Box::new(selector(0, None)),
            right: Box::new(PromExpr::Number(10.0)),
            matching: None,
            bool_result: false,
        };
        let rows = at(&gt, 60);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].labels.as_ref().unwrap()["__name__"],
            "requests",
            "a filter keeps the name"
        );
        let gt_bool = PromExpr::Binary {
            op: PromOp::Gt,
            left: Box::new(selector(0, None)),
            right: Box::new(PromExpr::Number(10.0)),
            matching: None,
            bool_result: true,
        };
        let rows = at(&gt_bool, 60);
        assert_eq!(rows.len(), 2);
        assert!((value_of(&rows, "web") - 0.0).abs() < f64::EPSILON);
        let and = PromExpr::Binary {
            op: PromOp::And,
            left: Box::new(selector(0, None)),
            right: Box::new(selector(1, None)),
            matching: None,
            bool_result: false,
        };
        assert_eq!(at(&and, 60).len(), 1);
        let unless = PromExpr::Binary {
            op: PromOp::Unless,
            left: Box::new(selector(0, None)),
            right: Box::new(selector(1, None)),
            matching: None,
            bool_result: false,
        };
        let rows = at(&unless, 60);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].labels.as_ref().unwrap()["job"], "web");
        let on_group = PromExpr::Binary {
            op: PromOp::Mul,
            left: Box::new(selector(0, None)),
            right: Box::new(PromExpr::Aggregate {
                op: PromAggregate::Count,
                input: Box::new(selector(0, None)),
                parameter: None,
                grouping: None,
            }),
            matching: Some(Matching {
                on: true,
                labels: vec![],
                group: Some(Group {
                    left: true,
                    include: vec![],
                }),
            }),
            bool_result: false,
        };
        let rows = at(&on_group, 60);
        assert_eq!(
            rows.len(),
            2,
            "group_left pairs every series with the one count"
        );
        assert!((value_of(&rows, "api") - 80.0).abs() < f64::EPSILON);
    }

    #[test]
    fn subqueries_and_steps() {
        let max = PromExpr::Call {
            function: "max_over_time".into(),
            args: vec![PromExpr::Subquery {
                input: Box::new(PromExpr::Call {
                    function: "rate".into(),
                    args: vec![selector(0, Some(30))],
                }),
                range: Duration::from_secs(60),
                step: Some(Duration::from_secs(15)),
                offset: 0,
                at: None,
            }],
        };
        let rows = at(&max, 60);
        assert_eq!(rows.len(), 2);
        assert!(value_of(&rows, "api") > 0.0);
        let rows = evaluate(
            &selector(1, None),
            &inputs(),
            Params {
                start: 0,
                end: 60 * S,
                step: 30 * S,
            },
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|r| r.time / S).collect::<Vec<_>>(),
            vec![0, 30, 60]
        );
    }

    #[test]
    fn a_range_vector_at_an_instant_lists_its_samples() {
        let rows = at(&selector(1, Some(60)), 60);
        assert_eq!(
            rows.len(),
            1,
            "(0s, 60s] holds the sample at 30s, not the one at 0s"
        );
        assert_eq!(rows[0].time / S, 30);
        assert_eq!(rows[0].labels.as_ref().unwrap()["__name__"], "up");
        assert_eq!(
            at(&selector(1, Some(60)), 0).len(),
            1,
            "right-closed: the sample at 0s is in (-60s, 0s]"
        );
    }

    #[test]
    fn quantiles_follow_prometheus() {
        assert!((quantile(0.5, &mut [1.0, 2.0, 3.0, 4.0]) - 2.5).abs() < f64::EPSILON);
        assert!((quantile(0.9, &mut [1.0, 2.0, 3.0]) - 2.8).abs() < 1e-9);
        assert!(quantile(1.5, &mut [1.0]).is_infinite());
        assert_eq!(format_value(3.0), "3");
        assert_eq!(format_value(2.5), "2.5");
    }
}
