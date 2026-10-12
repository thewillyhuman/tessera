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
//!
//! A sample is a float or a native histogram ([`Point`]). A histogram
//! takes part where Prometheus lets it: scaled by a float, added to or
//! subtracted from another, summed or averaged, compared for equality,
//! counted, and read by the `histogram_*` functions; elsewhere the
//! sample drops out, as it does where Prometheus warns.

mod functions;
pub mod histogram;

use std::collections::{BTreeMap, BTreeSet};

use tessera_ir::promql::{Group, Matching, PromAggregate, PromExpr, PromOp};
use tessera_ir::{LOOKBACK, SeriesGrouping};

use crate::KernelError;

pub use histogram::{Bucket, Histogram, ResetHint};

/// The labels of a series, `__name__` among them.
pub type Labels = BTreeMap<String, String>;

/// The value of one sample: a float, or a native histogram (QL-5).
#[derive(Clone, Debug, PartialEq)]
pub enum Point {
    /// A float sample.
    Float(f64),
    /// A native histogram sample.
    Histogram(Box<Histogram>),
}

impl Point {
    /// The float, if the sample is one.
    #[must_use]
    pub fn float(&self) -> Option<f64> {
        match self {
            Point::Float(value) => Some(*value),
            Point::Histogram(_) => None,
        }
    }

    /// The histogram, if the sample is one.
    #[must_use]
    pub fn histogram(&self) -> Option<&Histogram> {
        match self {
            Point::Float(_) => None,
            Point::Histogram(histogram) => Some(histogram),
        }
    }

    /// Whether the sample is Prometheus' staleness marker: its `NaN`
    /// as the value, or as a histogram's sum.
    #[must_use]
    pub fn is_stale(&self) -> bool {
        match self {
            Point::Float(value) => is_stale(*value),
            Point::Histogram(histogram) => is_stale(histogram.sum),
        }
    }

    /// The sample as text, as `count_values` labels it.
    fn text(&self) -> String {
        match self {
            Point::Float(value) => format_value(*value),
            Point::Histogram(histogram) => histogram.to_string(),
        }
    }
}

impl From<f64> for Point {
    fn from(value: f64) -> Self {
        Point::Float(value)
    }
}

impl From<Histogram> for Point {
    fn from(histogram: Histogram) -> Self {
        Point::Histogram(Box::new(histogram))
    }
}

/// A sample: time in nanoseconds and value.
pub type Sample = (i64, Point);

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
    Vector(Vec<(Labels, Point)>),
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
    pub value: Point,
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
                value: Point::Float(value),
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
                        .map(|(labels, v)| (without_name(labels), negated(v)))
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
                            series
                                .entry(Labels::new())
                                .or_default()
                                .push((sub, Point::Float(value)));
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
    fn instant(&self, input: usize, t: i64) -> Vec<(Labels, Point)> {
        let lookback = i64::try_from(LOOKBACK.as_nanos()).unwrap_or(i64::MAX);
        let since = t - lookback;
        self.inputs
            .get(input)
            .map(|series| {
                series
                    .iter()
                    .filter_map(|s| {
                        let index = s.samples.partition_point(|(ts, _)| *ts <= t);
                        let (ts, value) = s.samples.get(index.checked_sub(1)?)?;
                        (*ts > since && !value.is_stale())
                            .then(|| (s.labels.clone(), value.clone()))
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
                            .filter(|(_, v)| !v.is_stale())
                            .cloned()
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

/// `-point`: a histogram negated becomes a gauge.
fn negated(point: Point) -> Point {
    match point {
        Point::Float(value) => Point::Float(-value),
        Point::Histogram(mut histogram) => {
            histogram.mul(-1.0);
            Point::Histogram(histogram)
        }
    }
}

/// The floats among `members`; histogram samples have no part.
fn floats(members: &[(Labels, Point)]) -> Vec<f64> {
    members.iter().filter_map(|(_, v)| v.float()).collect()
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
                    with_scalar(op, v, scalar, false, bool_result).map(|v| (labels, v))
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
                    with_scalar(op, v, scalar, true, bool_result).map(|v| (labels, v))
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

/// `value op scalar`, or `scalar op value` when `swapped`: the result,
/// or none when the sample drops out, as a comparison that does not
/// hold does. A histogram is scaled by a float and divided by one;
/// nothing else, a comparison included, has a result.
fn with_scalar(
    op: PromOp,
    value: Point,
    scalar: f64,
    swapped: bool,
    bool_result: bool,
) -> Option<Point> {
    let histogram = match value {
        Point::Float(v) => {
            let (l, r) = if swapped { (scalar, v) } else { (v, scalar) };
            if !op.is_comparison() {
                return Some(Point::Float(arithmetic(op, l, r)));
            }
            let held = holds(op, l, r);
            return if bool_result {
                Some(Point::Float(f64::from(u8::from(held))))
            } else {
                held.then_some(Point::Float(v))
            };
        }
        Point::Histogram(histogram) => histogram,
    };
    scaled(op, histogram, scalar, swapped).map(Point::Histogram)
}

/// A histogram times a float, or divided by a float on its right.
fn scaled(
    op: PromOp,
    mut histogram: Box<Histogram>,
    scalar: f64,
    float_on_left: bool,
) -> Option<Box<Histogram>> {
    match op {
        PromOp::Mul => {
            histogram.mul(scalar);
        }
        PromOp::Div if !float_on_left => {
            histogram.div(scalar);
        }
        _ => return None,
    }
    histogram.compact();
    Some(histogram)
}

/// `l op r` between the samples of two matched series: the result, or
/// none when the pair drops out. A comparison keeps the left sample,
/// whichever side is the many one; `bool` gives 1 or 0 instead. Two
/// histograms add, subtract into a gauge, and compare for equality.
fn combined(op: PromOp, l: &Point, r: &Point, bool_result: bool) -> Option<Point> {
    let kept = || l.clone();
    let held = |held: bool| {
        if bool_result {
            Some(Point::Float(f64::from(u8::from(held))))
        } else {
            held.then(kept)
        }
    };
    match (l, r) {
        (Point::Float(a), Point::Float(b)) => {
            if op.is_comparison() {
                held(holds(op, *a, *b))
            } else {
                Some(Point::Float(arithmetic(op, *a, *b)))
            }
        }
        (Point::Float(f), Point::Histogram(h)) => {
            scaled(op, h.clone(), *f, true).map(Point::Histogram)
        }
        (Point::Histogram(h), Point::Float(f)) => {
            scaled(op, h.clone(), *f, false).map(Point::Histogram)
        }
        (Point::Histogram(a), Point::Histogram(b)) => match op {
            PromOp::Add | PromOp::Sub => {
                let mut result = a.clone();
                if op == PromOp::Add {
                    result.add(b).ok()?;
                } else {
                    result.sub(b).ok()?;
                    result.hint = ResetHint::Gauge;
                }
                result.compact();
                Some(Point::Histogram(result))
            }
            PromOp::Eq => held(a.equals(b)),
            PromOp::NotEq => held(!a.equals(b)),
            _ => None,
        },
    }
}

/// `and`, `or` and `unless` on the series' signatures.
fn set_operation(
    op: PromOp,
    left: Vec<(Labels, Point)>,
    right: Vec<(Labels, Point)>,
    matching: Option<&Matching>,
) -> Vec<(Labels, Point)> {
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
    left: Vec<(Labels, Point)>,
    right: Vec<(Labels, Point)>,
    matching: Option<&Matching>,
    bool_result: bool,
) -> Result<Vec<(Labels, Point)>, KernelError> {
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
    let mut by_signature: BTreeMap<Labels, (Labels, Point)> = BTreeMap::new();
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
            (one_value, &value)
        } else {
            (&value, one_value)
        };
        let Some(result) = combined(op, l, r, bool_result) else {
            continue;
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

/// An aggregation over the series of `vector`. `sum` and `avg` take
/// histograms as well as floats, but not both in one group; `count`,
/// `group`, `count_values`, `limitk` and `limit_ratio` take any
/// sample; the others read floats only.
#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
fn aggregate(
    op: PromAggregate,
    vector: Vec<(Labels, Point)>,
    parameter: Option<&Value>,
    grouping: Option<&SeriesGrouping>,
) -> Result<Value, KernelError> {
    let scalar_parameter = || match parameter {
        Some(Value::Scalar(v)) => Ok(*v),
        _ => Err(unsupported(format!("{} needs a number", op.name()))),
    };
    let mut groups: BTreeMap<Labels, Vec<(Labels, Point)>> = BTreeMap::new();
    for (labels, value) in vector {
        groups
            .entry(group_labels(&labels, grouping))
            .or_default()
            .push((labels, value));
    }
    let mut out = Vec::new();
    for (key, members) in groups {
        let values = floats(&members);
        let n = values.len() as f64;
        let push = |out: &mut Vec<(Labels, Point)>, value: f64| {
            out.push((key.clone(), Point::Float(value)));
        };
        match op {
            PromAggregate::Sum | PromAggregate::Avg => {
                if let Some(value) = sum_or_avg(op, &members) {
                    out.push((key, value));
                }
            }
            PromAggregate::Min if !values.is_empty() => {
                push(&mut out, values.iter().copied().fold(f64::NAN, nan_min));
            }
            PromAggregate::Max if !values.is_empty() => {
                push(&mut out, values.iter().copied().fold(f64::NAN, nan_max));
            }
            PromAggregate::Min | PromAggregate::Max => {}
            PromAggregate::Count => push(&mut out, members.len() as f64),
            PromAggregate::Group => push(&mut out, 1.0),
            PromAggregate::Stddev | PromAggregate::Stdvar => {
                if values.is_empty() {
                    continue;
                }
                let mean = values.iter().sum::<f64>() / n;
                let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
                let value = if op == PromAggregate::Stddev {
                    variance.sqrt()
                } else {
                    variance
                };
                push(&mut out, value);
            }
            PromAggregate::Quantile => {
                let q = scalar_parameter()?;
                if values.is_empty() {
                    continue;
                }
                let mut values = values;
                push(&mut out, quantile(q, &mut values));
            }
            PromAggregate::CountValues => {
                let Some(Value::Text(name)) = parameter else {
                    return Err(unsupported("count_values needs a label name"));
                };
                let mut counts: BTreeMap<String, f64> = BTreeMap::new();
                for (_, value) in &members {
                    *counts.entry(value.text()).or_default() += 1.0;
                }
                for (text, count) in counts {
                    let mut labels = key.clone();
                    labels.insert(name.clone(), text);
                    out.push((labels, Point::Float(count)));
                }
            }
            PromAggregate::Topk | PromAggregate::Bottomk => {
                let k = scalar_parameter()?;
                if k.is_nan() || k < 1.0 {
                    continue;
                }
                let mut members: Vec<(Labels, f64)> = members
                    .into_iter()
                    .filter_map(|(labels, v)| Some((labels, v.float()?)))
                    .collect();
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
                out.extend(
                    members
                        .into_iter()
                        .take(k)
                        .map(|(labels, v)| (labels, Point::Float(v))),
                );
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

/// The `sum` or `avg` of a group: of its floats, or of its histograms
/// added at the coarser schema; a group with both has no result, as
/// Prometheus warns, nor one whose histograms cannot be combined.
#[allow(clippy::cast_precision_loss)]
fn sum_or_avg(op: PromAggregate, members: &[(Labels, Point)]) -> Option<Point> {
    let n = members.len() as f64;
    let histograms: Vec<&Histogram> = members.iter().filter_map(|(_, v)| v.histogram()).collect();
    if histograms.is_empty() {
        let sum: f64 = floats(members).iter().sum();
        return Some(Point::Float(if op == PromAggregate::Avg {
            sum / n
        } else {
            sum
        }));
    }
    let (first, rest) = histograms.split_first()?;
    if rest.len() + 1 != members.len() {
        return None;
    }
    let mut total = (*first).clone();
    for histogram in rest {
        total.add(histogram).ok()?;
    }
    if op == PromAggregate::Avg {
        total.div(n);
    }
    total.compact();
    Some(Point::from(total))
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
            samples: samples
                .iter()
                .map(|(t, v)| (t * S, Point::Float(*v)))
                .collect(),
        }
    }

    /// A schema 0 histogram with `buckets` from index 0 upward.
    fn hist(count: f64, sum: f64, buckets: &[f64]) -> Point {
        exp(0, count, sum, (0.0, 0.0), buckets, &[])
    }

    /// A histogram as Prometheus' test format writes one: `buckets`
    /// from index 0 upward, `n_buckets` likewise on the negative side.
    fn exp(
        schema: i32,
        count: f64,
        sum: f64,
        zero: (f64, f64),
        buckets: &[f64],
        n_buckets: &[f64],
    ) -> Point {
        let side = |counts: &[f64]| {
            counts
                .iter()
                .enumerate()
                .map(|(i, c)| (i32::try_from(i).unwrap(), *c))
                .collect()
        };
        Point::from(Histogram {
            schema,
            zero_count: zero.0,
            zero_threshold: zero.1,
            count,
            sum,
            positive: side(buckets),
            negative: side(n_buckets),
            ..Histogram::default()
        })
    }

    /// A histogram with custom buckets.
    fn custom(count: f64, sum: f64, bounds: &[f64], buckets: &[f64]) -> Point {
        let mut histogram = Histogram {
            schema: histogram::CUSTOM_SCHEMA,
            count,
            sum,
            custom_values: bounds.to_vec(),
            ..Histogram::default()
        };
        for (i, c) in buckets.iter().enumerate() {
            histogram.positive.insert(i32::try_from(i).unwrap(), *c);
        }
        Point::from(histogram)
    }

    /// A series `name` with one sample per entry of `points`, `every`
    /// seconds apart from 0.
    fn timed(name: &str, every: i64, points: &[Point]) -> Series {
        Series {
            labels: labels(&[("__name__", name)]),
            samples: points
                .iter()
                .enumerate()
                .map(|(i, p)| (i64::try_from(i).unwrap() * every * S, p.clone()))
                .collect(),
        }
    }

    /// A histogram series `name{job=...}` with one sample per entry of
    /// `points`, 15 s apart from 0.
    fn histogram_series(name: &str, job: &str, points: &[Point]) -> Series {
        Series {
            labels: labels(&[("__name__", name), ("job", job)]),
            samples: points
                .iter()
                .enumerate()
                .map(|(i, p)| (i64::try_from(i).unwrap() * 15 * S, p.clone()))
                .collect(),
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
            .map_or(f64::NAN, |r| r.value.float().unwrap_or(f64::NAN))
    }

    fn histogram_of(rows: &[Row], job: &str) -> Option<Histogram> {
        rows.iter()
            .find(|r| {
                r.labels
                    .as_ref()
                    .and_then(|l| l.get("job"))
                    .map(String::as_str)
                    == Some(job)
            })
            .and_then(|r| r.value.histogram().cloned())
    }

    fn call(function: &str, args: Vec<PromExpr>) -> PromExpr {
        PromExpr::Call {
            function: function.into(),
            args,
        }
    }

    #[test]
    fn instant_selectors_look_back_five_minutes_and_honour_staleness() {
        let rows = at(&selector(1, None), 100);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].labels.as_ref().unwrap()["__name__"], "up");
        assert!((rows[0].value.float().unwrap() - 1.0).abs() < f64::EPSILON);
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
        assert!((rows[0].value.float().unwrap() - 46.0).abs() < f64::EPSILON);
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
        assert!((rows[0].value.float().unwrap() - 40.0).abs() < f64::EPSILON);
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
        // A comparison keeps the left operand's value with the many
        // side's labels, as Prometheus does: up (1) < requests (40).
        let lt_right = PromExpr::Binary {
            op: PromOp::Lt,
            left: Box::new(selector(1, None)),
            right: Box::new(selector(0, None)),
            matching: Some(Matching {
                on: true,
                labels: vec!["job".into()],
                group: Some(Group {
                    left: false,
                    include: vec![],
                }),
            }),
            bool_result: false,
        };
        let rows = at(&lt_right, 60);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].labels.as_ref().unwrap()["__name__"], "requests");
        assert!(
            (value_of(&rows, "api") - 1.0).abs() < f64::EPSILON,
            "{rows:?}"
        );
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

    /// QL-5: a histogram sample takes part where Prometheus lets it:
    /// scaled by a float, added, subtracted into a gauge, negated,
    /// summed, averaged, counted, compared for equality; and drops out
    /// elsewhere, as it does where Prometheus warns.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn histogram_samples_flow_through_operators_and_aggregations() {
        let inputs = vec![
            vec![
                histogram_series("latency", "api", &[hist(4.0, 5.0, &[1.0, 2.0, 1.0])]),
                histogram_series("latency", "web", &[hist(2.0, 3.0, &[0.0, 1.0, 1.0])]),
            ],
            vec![series(&[("__name__", "up"), ("job", "api")], &[(0, 3.0)])],
        ];
        let eval = |expr: &PromExpr| {
            evaluate(
                expr,
                &inputs,
                Params {
                    start: 0,
                    end: 0,
                    step: 0,
                },
            )
            .unwrap()
        };
        let binary = |op, left, right| PromExpr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
            matching: None,
            bool_result: false,
        };
        let aggregate = |op, parameter: Option<PromExpr>| PromExpr::Aggregate {
            op,
            input: Box::new(selector(0, None)),
            parameter: parameter.map(Box::new),
            grouping: None,
        };
        let rows = eval(&selector(0, None));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].labels.as_ref().unwrap()["__name__"], "latency");
        let doubled = eval(&binary(
            PromOp::Mul,
            selector(0, None),
            PromExpr::Number(2.0),
        ));
        let api = histogram_of(&doubled, "api").unwrap();
        assert!((api.count - 8.0).abs() < f64::EPSILON);
        assert!((api.positive[&1] - 4.0).abs() < f64::EPSILON);
        assert!(!doubled[0].labels.as_ref().unwrap().contains_key("__name__"));
        let halved = eval(&binary(
            PromOp::Div,
            selector(0, None),
            PromExpr::Number(2.0),
        ));
        assert!((histogram_of(&halved, "api").unwrap().sum - 2.5).abs() < f64::EPSILON);
        let scaled = eval(&binary(
            PromOp::Mul,
            PromExpr::Number(3.0),
            selector(0, None),
        ));
        assert!((histogram_of(&scaled, "web").unwrap().count - 6.0).abs() < f64::EPSILON);
        assert!(
            eval(&binary(
                PromOp::Div,
                PromExpr::Number(3.0),
                selector(0, None)
            ))
            .is_empty(),
            "a float divided by a histogram has no result"
        );
        assert!(
            eval(&binary(
                PromOp::Add,
                selector(0, None),
                PromExpr::Number(1.0)
            ))
            .is_empty()
        );
        assert!(
            eval(&binary(
                PromOp::Gt,
                selector(0, None),
                PromExpr::Number(1.0)
            ))
            .is_empty()
        );
        let by_up = eval(&binary(PromOp::Mul, selector(0, None), selector(1, None)));
        assert_eq!(by_up.len(), 1, "only api has an up series");
        assert!((histogram_of(&by_up, "api").unwrap().count - 12.0).abs() < f64::EPSILON);
        assert!(eval(&binary(PromOp::Add, selector(0, None), selector(1, None))).is_empty());
        let total = eval(&aggregate(PromAggregate::Sum, None));
        assert_eq!(total.len(), 1);
        let sum = total[0].value.histogram().unwrap();
        assert!((sum.count - 6.0).abs() < f64::EPSILON);
        assert_eq!(sum.positive, BTreeMap::from([(0, 1.0), (1, 3.0), (2, 2.0)]));
        let mean = eval(&aggregate(PromAggregate::Avg, None));
        assert!((mean[0].value.histogram().unwrap().count - 3.0).abs() < f64::EPSILON);
        let count = eval(&aggregate(PromAggregate::Count, None));
        assert!((count[0].value.float().unwrap() - 2.0).abs() < f64::EPSILON);
        assert!(eval(&aggregate(PromAggregate::Max, None)).is_empty());
        assert!(eval(&aggregate(PromAggregate::Topk, Some(PromExpr::Number(1.0)))).is_empty());
        assert_eq!(
            eval(&aggregate(
                PromAggregate::Limitk,
                Some(PromExpr::Number(1.0))
            ))
            .len(),
            1
        );
        let counted = eval(&aggregate(
            PromAggregate::CountValues,
            Some(PromExpr::Text("v".into())),
        ));
        assert_eq!(counted.len(), 2);
        assert!(
            counted
                .iter()
                .any(|r| r.labels.as_ref().unwrap()["v"].starts_with("{count:4"))
        );
        let doubled_sum = eval(&binary(PromOp::Add, selector(0, None), selector(0, None)));
        assert!((histogram_of(&doubled_sum, "api").unwrap().count - 8.0).abs() < f64::EPSILON);
        let nothing = eval(&binary(PromOp::Sub, selector(0, None), selector(0, None)));
        let zero = histogram_of(&nothing, "api").unwrap();
        assert!(zero.count == 0.0 && zero.positive.is_empty(), "{zero:?}");
        assert_eq!(zero.hint, ResetHint::Gauge, "a difference is a gauge");
        let negated = eval(&PromExpr::Negate(Box::new(selector(0, None))));
        let negative = histogram_of(&negated, "api").unwrap();
        assert!((negative.count + 4.0).abs() < f64::EPSILON);
        assert_eq!(negative.hint, ResetHint::Gauge);
        let same = eval(&binary(PromOp::Eq, selector(0, None), selector(0, None)));
        assert_eq!(same.len(), 2, "a histogram equals itself");
        assert!(eval(&binary(PromOp::NotEq, selector(0, None), selector(0, None))).is_empty());
        assert!(eval(&binary(PromOp::Gt, selector(0, None), selector(0, None))).is_empty());
        let mixed = vec![vec![
            histogram_series("m", "api", &[hist(4.0, 5.0, &[1.0, 2.0, 1.0])]),
            series(&[("__name__", "m"), ("job", "web")], &[(0, 1.0)]),
        ]];
        let total = evaluate(
            &aggregate(PromAggregate::Sum, None),
            &mixed,
            Params {
                start: 0,
                end: 0,
                step: 0,
            },
        )
        .unwrap();
        assert!(total.is_empty(), "floats and histograms do not add up");
    }

    /// QL-5: the `histogram_*` functions read native histograms, and
    /// `histogram_quantile` and `histogram_fraction` classic buckets
    /// too; functions of floats leave histograms out.
    #[test]
    fn histogram_functions_read_native_and_classic_samples() {
        let bucket = |le: &str, count: f64| {
            series(
                &[("__name__", "classic_bucket"), ("job", "api"), ("le", le)],
                &[(0, count)],
            )
        };
        let inputs = vec![
            vec![histogram_series(
                "latency",
                "api",
                &[hist(4.0, 5.0, &[1.0, 2.0, 1.0])],
            )],
            vec![
                bucket("0.1", 1.0),
                bucket("0.5", 3.0),
                bucket("1", 4.0),
                bucket("+Inf", 4.0),
            ],
        ];
        let eval = |expr: &PromExpr| {
            evaluate(
                expr,
                &inputs,
                Params {
                    start: 0,
                    end: 0,
                    step: 0,
                },
            )
            .unwrap()
        };
        let one = |rows: Vec<Row>| {
            assert_eq!(rows.len(), 1, "{rows:?}");
            assert!(!rows[0].labels.as_ref().unwrap().contains_key("__name__"));
            rows[0].value.float().unwrap()
        };
        let read = |function: &str| one(eval(&call(function, vec![selector(0, None)])));
        assert!((read("histogram_count") - 4.0).abs() < f64::EPSILON);
        assert!((read("histogram_sum") - 5.0).abs() < f64::EPSILON);
        assert!((read("histogram_avg") - 1.25).abs() < f64::EPSILON);
        assert!(read("histogram_stdvar") > 0.0);
        assert!((read("histogram_stddev").powi(2) - read("histogram_stdvar")).abs() < 1e-12);
        let median = one(eval(&call(
            "histogram_quantile",
            vec![PromExpr::Number(0.5), selector(0, None)],
        )));
        assert!((median - std::f64::consts::SQRT_2).abs() < 1e-12);
        let half = one(eval(&call(
            "histogram_fraction",
            vec![
                PromExpr::Number(1.0),
                PromExpr::Number(2.0),
                selector(0, None),
            ],
        )));
        assert!((half - 0.5).abs() < f64::EPSILON);
        let classic_median = one(eval(&call(
            "histogram_quantile",
            vec![PromExpr::Number(0.5), selector(1, None)],
        )));
        assert!((classic_median - 0.3).abs() < 1e-9);
        let classic_half = one(eval(&call(
            "histogram_fraction",
            vec![
                PromExpr::Number(0.1),
                PromExpr::Number(0.5),
                selector(1, None),
            ],
        )));
        assert!((classic_half - 0.5).abs() < 1e-9);
        assert!(eval(&call("histogram_count", vec![selector(1, None)])).is_empty());
        assert!(eval(&call("abs", vec![selector(0, None)])).is_empty());
        assert!(eval(&call("sort", vec![selector(0, None)])).is_empty());
        assert!(eval(&call("round", vec![selector(0, None)])).is_empty());
        assert!(eval(&call("hour", vec![selector(0, None)])).is_empty());
        let scalar = eval(&call("scalar", vec![selector(0, None)]));
        assert!(scalar[0].value.float().unwrap().is_nan());
        assert!(
            (one(eval(&call("timestamp", vec![selector(0, None)]))) - 0.0).abs() < f64::EPSILON
        );
        assert!(eval(&call("absent", vec![selector(0, None)])).is_empty());
        let replaced = eval(&call(
            "label_replace",
            vec![
                selector(0, None),
                PromExpr::Text("site".into()),
                PromExpr::Text("$1".into()),
                PromExpr::Text("job".into()),
                PromExpr::Text("(.*)".into()),
            ],
        ));
        assert_eq!(replaced[0].labels.as_ref().unwrap()["site"], "api");
        assert!(replaced[0].value.histogram().is_some());
    }

    /// QL-5: the range functions over native histograms, with the
    /// cases of Prometheus' `native_histograms.test`.
    #[test]
    #[allow(clippy::too_many_lines, clippy::unreadable_literal)]
    fn range_functions_read_histogram_samples() {
        const M: i64 = 60;
        let near = |actual: f64, expected: f64| {
            assert!(
                (actual - expected).abs() <= 1e-9 * expected.abs().max(1.0),
                "expected {expected}, got {actual}"
            );
        };
        let incr: Vec<Point> = (0..=10)
            .map(|i| {
                hist(
                    4.0 + f64::from(i),
                    4.0 + 2.0 * f64::from(i),
                    &[1.0, 2.0 + f64::from(i), 1.0],
                )
            })
            .collect();
        let reset_in_bucket = [
            hist(4.0, 5.0, &[1.0, 2.0, 1.0]),
            hist(5.0, 6.0, &[1.0, 1.0, 3.0]),
            hist(6.0, 7.0, &[1.0, 2.0, 3.0]),
        ];
        let mut long_lived = vec![hist(15.0, 25.0, &[5.0, 10.0])];
        long_lived.extend(std::iter::repeat_n(hist(2490.0, 75.0, &[15.0, 2475.0]), 55));
        let four = [
            exp(
                0,
                25.0,
                1234.5,
                (4.0, 0.001),
                &[1.0, 2.0, 0.0, 1.0, 1.0],
                &[2.0, 4.0, 0.0, 0.0, 1.0, 9.0],
            ),
            exp(
                0,
                41.0,
                2345.6,
                (5.0, 0.001),
                &[1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 1.0],
                &[0.0, 1.0, 4.0, 2.0, 7.0, 0.0, 0.0, 0.0, 0.0, 5.0, 5.0, 2.0],
            ),
            exp(
                0,
                41.0,
                1111.1,
                (5.0, 0.001),
                &[1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 1.0],
                &[0.0, 1.0, 4.0, 2.0, 7.0, 0.0, 0.0, 0.0, 0.0, 5.0, 5.0, 2.0],
            ),
            exp(1, 0.0, 0.0, (0.0, 0.0), &[], &[]),
        ];
        let mixed = [hist(1.0, 1.0, &[1.0]), Point::Float(1.0)];
        let nhcb = [
            custom(1.0, 1.0, &[5.0], &[1.0]),
            custom(1.0, 1.0, &[5.0], &[1.0]),
            custom(1.0, 1.0, &[5.0, 10.0], &[1.0]),
        ];
        let inputs = vec![
            vec![timed("incr", 5 * M, &incr)],
            vec![timed("reset", 5 * M, &reset_in_bucket)],
            vec![timed("long", M, &long_lived)],
            vec![timed("four", M, &four)],
            vec![timed("mixed", 15, &mixed)],
            vec![timed("nhcb", 6 * M, &nhcb)],
        ];
        let at = |expr: &PromExpr, t: i64| {
            evaluate(
                expr,
                &inputs,
                Params {
                    start: t * S,
                    end: t * S,
                    step: 0,
                },
            )
            .unwrap()
        };
        let range = |function: &str, input: usize, seconds: u64| {
            call(function, vec![selector(input, Some(seconds))])
        };
        let histogram = |rows: Vec<Row>| {
            assert_eq!(rows.len(), 1, "{rows:?}");
            rows[0].value.histogram().unwrap().clone()
        };
        let rate = at(&range("rate", 0, 600), 50 * M);
        assert!(!rate[0].labels.as_ref().unwrap().contains_key("__name__"));
        let rate = histogram(rate);
        near(rate.count, 1.0 / 300.0);
        near(rate.sum, 2.0 / 300.0);
        near(rate.positive[&1], 1.0 / 300.0);
        assert_eq!(rate.positive.len(), 1, "empty buckets are compacted away");
        assert_eq!(rate.hint, ResetHint::Gauge);
        let increase = histogram(at(&range("increase", 1, 900), 10 * M));
        near(increase.count, 9.0);
        near(increase.sum, 10.5);
        near(increase.positive[&0], 1.5);
        near(increase.positive[&1], 3.0);
        near(increase.positive[&2], 4.5);
        let bounded = histogram(at(&range("increase", 2, 5400), 55 * M));
        near(bounded.count, 2490.0);
        near(bounded.sum, 50.303030303030305);
        near(bounded.positive[&0], 10.06060606060606);
        near(bounded.positive[&1], 2479.939393939394);
        let delta = histogram(at(&range("delta", 1, 900), 10 * M));
        near(delta.count, 3.0);
        let total = histogram(at(&range("sum_over_time", 3, 240), 3 * M));
        near(total.count, 107.0);
        near(total.sum, 4691.2);
        near(total.zero_count, 14.0);
        assert_eq!(
            total.positive.values().copied().collect::<Vec<_>>(),
            vec![3.0, 8.0, 2.0, 5.0, 3.0, 2.0, 2.0]
        );
        assert_eq!(
            total.negative.values().copied().collect::<Vec<_>>(),
            vec![
                2.0, 6.0, 8.0, 4.0, 15.0, 9.0, 0.0, 0.0, 0.0, 10.0, 10.0, 4.0
            ]
        );
        let mean = histogram(at(&range("avg_over_time", 3, 240), 3 * M));
        near(mean.count, 26.75);
        near(mean.zero_count, 3.5);
        let irate = histogram(at(&range("irate", 0, 600), 50 * M));
        near(irate.count, 1.0 / 300.0);
        assert_eq!(irate.hint, ResetHint::Gauge);
        let idelta = histogram(at(&range("idelta", 0, 600), 50 * M));
        near(idelta.sum, 2.0);
        assert!(
            at(&range("rate", 4, 60), 15).is_empty(),
            "floats and histograms do not rate"
        );
        assert!(at(&range("sum_over_time", 4, 60), 15).is_empty());
        assert!(at(&range("irate", 4, 60), 15).is_empty());
        let count = at(&range("count_over_time", 4, 60), 15);
        near(count[0].value.float().unwrap(), 2.0);
        near(
            at(&range("changes", 4, 60), 15)[0].value.float().unwrap(),
            1.0,
        );
        near(
            at(&range("resets", 4, 60), 15)[0].value.float().unwrap(),
            1.0,
        );
        let last = at(&range("last_over_time", 4, 60), 15);
        assert_eq!(last[0].labels.as_ref().unwrap()["__name__"], "mixed");
        near(last[0].value.float().unwrap(), 1.0);
        let present = at(&range("present_over_time", 0, 60), 50 * M);
        near(present[0].value.float().unwrap(), 1.0);
        assert!(at(&range("max_over_time", 0, 600), 50 * M).is_empty());
        near(
            at(&range("resets", 1, 900), 10 * M)[0]
                .value
                .float()
                .unwrap(),
            1.0,
        );
        near(
            at(&range("changes", 1, 900), 10 * M)[0]
                .value
                .float()
                .unwrap(),
            2.0,
        );
        let nhcb_sum = histogram(at(&range("sum_over_time", 5, 780), 12 * M));
        assert_eq!(nhcb_sum.custom_values, vec![5.0]);
        near(nhcb_sum.count, 3.0);
        assert_eq!(nhcb_sum.positive, BTreeMap::from([(0, 3.0)]));
        near(
            at(&range("changes", 5, 780), 12 * M)[0]
                .value
                .float()
                .unwrap(),
            1.0,
        );
        near(
            at(&range("resets", 5, 780), 12 * M)[0]
                .value
                .float()
                .unwrap(),
            0.0,
        );
        let nhcb_rate = histogram(at(&range("rate", 5, 780), 12 * M));
        assert_eq!(nhcb_rate.custom_values, vec![5.0]);
        near(nhcb_rate.count, 0.0);
        assert!(nhcb_rate.positive.is_empty());
        let nhcb_last = histogram(at(&range("last_over_time", 5, 780), 12 * M));
        assert_eq!(nhcb_last.custom_values, vec![5.0, 10.0]);
    }
}
