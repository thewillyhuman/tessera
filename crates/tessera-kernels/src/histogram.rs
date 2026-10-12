// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Histogram points: the JSON a metric row stores for a classic
//! histogram (`bounds` and `counts`), an exponential one (`scale`,
//! `zero_count` and `positive` and `negative` buckets at an offset) or
//! a summary (`quantiles`).
//!
//! Merging (CO-8) folds the points of an interval so that downsampled
//! points still answer `histogram_quantile` and the rate functions.
//! Classic histograms merge when their bounds agree; exponential ones
//! are brought to the coarser scale first, halving buckets as the
//! `OpenTelemetry` mapping does.
//!
//! Reading as classic series (QL-5) gives `PromQL` a point's cumulative
//! `le` buckets, its count and sum, or a summary's quantiles, so that a
//! stored histogram answers `name_bucket`, `name_count` and `name_sum`
//! as a scraped one would. Reading as a native histogram gives the
//! point itself under its own name, and a native histogram writes back
//! in the stored shape, so that results carry one like a point.

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use crate::promql::STALE_NAN_BITS;
use crate::promql::histogram::{CUSTOM_SCHEMA, Histogram, MAX_SCHEMA, MIN_SCHEMA, ResetHint};

/// The histograms of `points`, delta points of one series, as one:
/// counts added, `count` and `sum` added, `min` and `max` kept.
///
/// `None` when there is nothing to merge, a point does not parse, or
/// classic histograms disagree on their bounds; the caller then keeps
/// the last point as it is.
#[must_use]
pub fn merge(points: &[&str]) -> Option<String> {
    let parsed: Vec<Map<String, Value>> = points
        .iter()
        .map(|point| {
            serde_json::from_str::<Value>(point)
                .ok()?
                .as_object()
                .cloned()
        })
        .collect::<Option<_>>()?;
    let (first, rest) = parsed.split_first()?;
    let mut merged = first.clone();
    for point in rest {
        if merged.contains_key("bounds") {
            merge_classic(&mut merged, point)?;
        } else {
            merge_exponential(&mut merged, point)?;
        }
    }
    Some(Value::Object(merged).to_string())
}

fn merge_classic(into: &mut Map<String, Value>, from: &Map<String, Value>) -> Option<()> {
    if into.get("bounds") != from.get("bounds") {
        return None;
    }
    let a = counts(into.get("counts"))?;
    let b = counts(from.get("counts"))?;
    if a.len() != b.len() {
        return None;
    }
    let summed: Vec<u64> = a
        .iter()
        .zip(&b)
        .map(|(x, y)| x.saturating_add(*y))
        .collect();
    into.insert("counts".into(), json!(summed));
    add(into, from, "count");
    add(into, from, "sum");
    keep(into, from, "min", f64::min);
    keep(into, from, "max", f64::max);
    Some(())
}

fn merge_exponential(into: &mut Map<String, Value>, from: &Map<String, Value>) -> Option<()> {
    let scale_a = into.get("scale").and_then(Value::as_i64)?;
    let scale_b = from.get("scale").and_then(Value::as_i64)?;
    let scale = scale_a.min(scale_b);
    for side in ["positive", "negative"] {
        let mut union = rescale(buckets(into.get(side)), scale_a, scale);
        for (index, count) in rescale(buckets(from.get(side)), scale_b, scale) {
            *union.entry(index).or_default() += count;
        }
        into.insert(side.into(), bucket_json(&union));
    }
    into.insert("scale".into(), json!(scale));
    add(into, from, "count");
    add(into, from, "sum");
    add(into, from, "zero_count");
    keep(into, from, "zero_threshold", f64::max);
    keep(into, from, "min", f64::min);
    keep(into, from, "max", f64::max);
    Some(())
}

/// Adds `from[key]` to `into[key]`: as integers while both are, as
/// floats otherwise; a value only `from` has is taken over.
fn add(into: &mut Map<String, Value>, from: &Map<String, Value>, key: &str) {
    let sum = match (into.get(key), from.get(key)) {
        (Some(a), Some(b)) => match (a.as_u64(), b.as_u64()) {
            (Some(x), Some(y)) => json!(x.saturating_add(y)),
            _ => match (a.as_f64(), b.as_f64()) {
                (Some(x), Some(y)) => json!(x + y),
                _ => return,
            },
        },
        (None, Some(b)) => b.clone(),
        _ => return,
    };
    into.insert(key.into(), sum);
}

/// Keeps `choose` of `into[key]` and `from[key]`, or whichever exists.
fn keep(
    into: &mut Map<String, Value>,
    from: &Map<String, Value>,
    key: &str,
    choose: fn(f64, f64) -> f64,
) {
    let kept = match (
        into.get(key).and_then(Value::as_f64),
        from.get(key).and_then(Value::as_f64),
    ) {
        (Some(a), Some(b)) => json!(choose(a, b)),
        (None, Some(b)) => json!(b),
        _ => return,
    };
    into.insert(key.into(), kept);
}

fn counts(value: Option<&Value>) -> Option<Vec<u64>> {
    value?.as_array()?.iter().map(Value::as_u64).collect()
}

/// The classic buckets of `point` as `(upper bound, cumulative count)`
/// in bound order, `+Inf` last. An exponential histogram's buckets are
/// the upper bounds of its positive buckets at its scale, the zero
/// bucket and every negative value counted below them; a point of
/// another shape, or one that does not parse, gives none.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn classic_buckets(point: &str) -> Option<Vec<(f64, f64)>> {
    let object = parse(point)?;
    let total = object.get("count").and_then(Value::as_f64);
    let mut out = Vec::new();
    let mut cumulative = 0.0;
    if let Some(bounds) = object.get("bounds").and_then(Value::as_array) {
        let counts = counts(object.get("counts"))?;
        for (bound, count) in bounds.iter().zip(&counts) {
            cumulative += *count as f64;
            out.push((bound.as_f64()?, cumulative));
        }
        cumulative += counts.get(bounds.len()).copied().unwrap_or(0) as f64;
    } else if let Some(scale) = object.get("scale").and_then(Value::as_i64) {
        let (negative_offset, negative) = buckets(object.get("negative"));
        let _ = negative_offset;
        cumulative += negative.iter().sum::<u64>() as f64;
        cumulative += object
            .get("zero_count")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let threshold = object
            .get("zero_threshold")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        out.push((threshold, cumulative));
        let (offset, positive) = buckets(object.get("positive"));
        let base = 2f64.powf(2f64.powi(-i32::try_from(scale).unwrap_or(0)));
        for (i, count) in positive.iter().enumerate() {
            cumulative += *count as f64;
            let index = offset + i64::try_from(i).unwrap_or(i64::MAX);
            out.push((
                base.powi(i32::try_from(index + 1).unwrap_or(i32::MAX)),
                cumulative,
            ));
        }
    } else {
        return None;
    }
    out.push((f64::INFINITY, total.unwrap_or(cumulative)));
    Some(out)
}

/// The count and the sum of `point`, the sum when it has one.
#[must_use]
pub fn count_and_sum(point: &str) -> Option<(f64, Option<f64>)> {
    let object = parse(point)?;
    Some((
        object.get("count").and_then(Value::as_f64)?,
        object.get("sum").and_then(Value::as_f64),
    ))
}

/// The quantiles of a summary `point` as `(quantile, value)`; none for
/// a point of another shape.
#[must_use]
pub fn quantiles(point: &str) -> Option<Vec<(f64, f64)>> {
    parse(point)?
        .get("quantiles")?
        .as_array()?
        .iter()
        .map(|pair| {
            let pair = pair.as_array()?;
            Some((pair.first()?.as_f64()?, pair.get(1)?.as_f64()?))
        })
        .collect()
}

/// The native histogram a stored point holds (QL-5): an exponential
/// point at its scale, reduced to Prometheus' finest schema when finer;
/// a classic point as custom buckets, its bounds the upper bounds and
/// its last count the `+Inf` bucket. A missing sum is zero, as
/// Prometheus reads one; `min` and `max` are not carried. None for a
/// summary, a point that does not parse, or a scale coarser than
/// Prometheus supports.
#[must_use]
pub fn native(point: &str) -> Option<Histogram> {
    let object = parse(point)?;
    let mut histogram = Histogram {
        hint: object
            .get("hint")
            .and_then(Value::as_str)
            .and_then(ResetHint::parse)
            .unwrap_or_default(),
        count: number(object.get("count")).unwrap_or(0.0),
        sum: number(object.get("sum")).unwrap_or(0.0),
        ..Histogram::default()
    };
    if let Some(bounds) = object.get("bounds").and_then(Value::as_array) {
        histogram.schema = CUSTOM_SCHEMA;
        histogram.custom_values = bounds
            .iter()
            .map(|b| number(Some(b)))
            .collect::<Option<_>>()?;
        for (i, count) in float_counts(object.get("counts"))?.into_iter().enumerate() {
            histogram.positive.insert(i32::try_from(i).ok()?, count);
        }
        return Some(histogram);
    }
    let scale = i32::try_from(object.get("scale")?.as_i64()?).ok()?;
    if scale < MIN_SCHEMA {
        return None;
    }
    histogram.schema = scale;
    histogram.zero_count = number(object.get("zero_count")).unwrap_or(0.0);
    histogram.zero_threshold = number(object.get("zero_threshold")).unwrap_or(0.0);
    histogram.positive = side(object.get("positive"))?;
    histogram.negative = side(object.get("negative"))?;
    histogram.reduce_resolution(MAX_SCHEMA);
    Some(histogram)
}

/// The stored shape of a native histogram, what [`native`] reads back:
/// a number JSON cannot hold (`NaN`, `+Inf`, `-Inf`) is written as its
/// text.
#[must_use]
pub fn point(histogram: &Histogram) -> String {
    let mut object = Map::new();
    object.insert("count".into(), json_number(histogram.count));
    object.insert("sum".into(), json_number(histogram.sum));
    if histogram.uses_custom_buckets() {
        let counts: Vec<Value> = (0..=histogram.custom_values.len())
            .map(|i| {
                let count = i32::try_from(i)
                    .ok()
                    .and_then(|i| histogram.positive.get(&i))
                    .copied()
                    .unwrap_or(0.0);
                json_number(count)
            })
            .collect();
        object.insert(
            "bounds".into(),
            histogram
                .custom_values
                .iter()
                .map(|b| json_number(*b))
                .collect(),
        );
        object.insert("counts".into(), Value::Array(counts));
    } else {
        object.insert("scale".into(), json!(histogram.schema));
        object.insert("zero_count".into(), json_number(histogram.zero_count));
        object.insert(
            "zero_threshold".into(),
            json_number(histogram.zero_threshold),
        );
        object.insert("positive".into(), dense(&histogram.positive));
        object.insert("negative".into(), dense(&histogram.negative));
    }
    if histogram.hint != ResetHint::Unknown {
        object.insert("hint".into(), json!(histogram.hint.as_str()));
    }
    Value::Object(object).to_string()
}

/// A side's buckets by Prometheus index, which is one above the
/// stored index: the stored offset names the bucket whose upper bound
/// is the first bucket's lower bound.
fn side(value: Option<&Value>) -> Option<BTreeMap<i32, f64>> {
    let mut out = BTreeMap::new();
    let Some(value) = value.filter(|v| !v.is_null()) else {
        return Some(out);
    };
    let offset = value.get("offset")?.as_i64()?;
    for (i, count) in float_counts(value.get("counts"))?.into_iter().enumerate() {
        let index = offset.checked_add(i64::try_from(i).ok()?)?.checked_add(1)?;
        out.insert(i32::try_from(index).ok()?, count);
    }
    Some(out)
}

/// Buckets by Prometheus index as the stored offset and a dense run of
/// counts.
fn dense(buckets: &BTreeMap<i32, f64>) -> Value {
    let (Some(first), Some(last)) = (buckets.keys().next(), buckets.keys().next_back()) else {
        return json!({ "offset": 0, "counts": [] });
    };
    let counts: Vec<Value> = (*first..=*last)
        .map(|i| json_number(buckets.get(&i).copied().unwrap_or(0.0)))
        .collect();
    json!({ "offset": i64::from(*first) - 1, "counts": counts })
}

fn float_counts(value: Option<&Value>) -> Option<Vec<f64>> {
    value?.as_array()?.iter().map(|v| number(Some(v))).collect()
}

/// A number, or the text of one JSON cannot hold; `StaleNaN` is the
/// staleness marker, its own bit pattern of `NaN`.
fn number(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) if s == "StaleNaN" => Some(f64::from_bits(STALE_NAN_BITS)),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn json_number(value: f64) -> Value {
    if value.is_finite() {
        json!(value)
    } else if value.to_bits() == STALE_NAN_BITS {
        json!("StaleNaN")
    } else if value.is_nan() {
        json!("NaN")
    } else if value > 0.0 {
        json!("+Inf")
    } else {
        json!("-Inf")
    }
}

fn parse(point: &str) -> Option<Map<String, Value>> {
    match serde_json::from_str::<Value>(point).ok()? {
        Value::Object(object) => Some(object),
        _ => None,
    }
}

/// A side's buckets as their offset and counts; none means empty.
fn buckets(value: Option<&Value>) -> (i64, Vec<u64>) {
    value
        .and_then(|v| Some((v.get("offset")?.as_i64()?, counts(v.get("counts"))?)))
        .unwrap_or((0, Vec::new()))
}

/// The non-empty buckets by index at scale `to`, from `from`: a bucket
/// at the finer scale falls into index `i >> (from - to)`.
fn rescale((offset, counts): (i64, Vec<u64>), from: i64, to: i64) -> BTreeMap<i64, u64> {
    let shift = u32::try_from((from - to).clamp(0, 62)).unwrap_or(0);
    let mut buckets = BTreeMap::new();
    for (i, count) in counts.into_iter().enumerate() {
        if count == 0 {
            continue;
        }
        let index = (offset + i64::try_from(i).unwrap_or(i64::MAX)) >> shift;
        *buckets.entry(index).or_default() += count;
    }
    buckets
}

/// Buckets by index as an offset and a dense run of counts.
fn bucket_json(buckets: &BTreeMap<i64, u64>) -> Value {
    let (Some(first), Some(last)) = (buckets.keys().next(), buckets.keys().next_back()) else {
        return json!({ "offset": 0, "counts": [] });
    };
    let counts: Vec<u64> = (*first..=*last)
        .map(|i| buckets.get(&i).copied().unwrap_or(0))
        .collect();
    json!({ "offset": first, "counts": counts })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(text: &str) -> Value {
        serde_json::from_str(text).unwrap()
    }

    /// QL-5: stored points read as the classic series Prometheus
    /// scrapes.
    #[test]
    fn points_read_as_classic_buckets_counts_sums_and_quantiles() {
        let classic = r#"{"count":3,"sum":30.0,"bounds":[10.0,20.0],"counts":[1,2,0]}"#;
        assert_eq!(
            classic_buckets(classic).unwrap(),
            vec![(10.0, 1.0), (20.0, 3.0), (f64::INFINITY, 3.0)]
        );
        assert_eq!(count_and_sum(classic).unwrap(), (3.0, Some(30.0)));
        let exponential = r#"{"count":7,"scale":0,"zero_count":1,"zero_threshold":0.0,"positive":{"offset":1,"counts":[2,3]},"negative":{"offset":0,"counts":[1]}}"#;
        assert_eq!(
            classic_buckets(exponential).unwrap(),
            vec![(0.0, 2.0), (4.0, 4.0), (8.0, 7.0), (f64::INFINITY, 7.0)],
            "scale 0 doubles per bucket: index 1 ends at 4, index 2 at 8"
        );
        let summary = r#"{"count":2,"sum":5.0,"quantiles":[[0.5,2.5],[0.99,4.0]]}"#;
        assert_eq!(quantiles(summary).unwrap(), vec![(0.5, 2.5), (0.99, 4.0)]);
        assert!(classic_buckets(summary).is_none());
        assert!(quantiles(classic).is_none());
        assert!(count_and_sum("not json").is_none());
    }

    /// QL-5: a stored point reads as a native histogram and writes back
    /// in the same shape.
    #[test]
    fn points_read_as_native_histograms_and_write_back() {
        let exponential = r#"{"count":7,"sum":3.5,"scale":1,"zero_count":1,"zero_threshold":0.001,"positive":{"offset":-1,"counts":[2,0,3]},"negative":{"offset":2,"counts":[1]},"min":0.1,"max":9.0}"#;
        let histogram = native(exponential).unwrap();
        assert_eq!(histogram.schema, 1);
        assert_eq!(
            histogram.positive,
            BTreeMap::from([(0, 2.0), (1, 0.0), (2, 3.0)]),
            "a stored offset of -1 starts at Prometheus index 0"
        );
        assert_eq!(histogram.negative, BTreeMap::from([(3, 1.0)]));
        assert!((histogram.zero_count - 1.0).abs() < f64::EPSILON);
        assert!((histogram.zero_threshold - 0.001).abs() < f64::EPSILON);
        assert!((histogram.count - 7.0).abs() < f64::EPSILON);
        assert!((histogram.sum - 3.5).abs() < f64::EPSILON);
        assert_eq!(histogram.hint, ResetHint::Unknown);
        assert_eq!(native(&point(&histogram)).unwrap(), histogram);
        let classic = r#"{"count":3,"sum":30.0,"bounds":[10.0,20.0],"counts":[1,2,0]}"#;
        let custom = native(classic).unwrap();
        assert_eq!(custom.schema, CUSTOM_SCHEMA);
        assert_eq!(custom.custom_values, vec![10.0, 20.0]);
        assert_eq!(
            custom.positive,
            BTreeMap::from([(0, 1.0), (1, 2.0), (2, 0.0)])
        );
        assert_eq!(native(&point(&custom)).unwrap(), custom);
        let fine = r#"{"count":2,"sum":2,"scale":10,"zero_count":0,"zero_threshold":0,"positive":{"offset":0,"counts":[1,1]}}"#;
        let reduced = native(fine).unwrap();
        assert_eq!(reduced.schema, MAX_SCHEMA, "finer than schema 8 is reduced");
        assert_eq!(reduced.positive, BTreeMap::from([(1, 2.0)]));
        assert!(native(r#"{"count":2,"sum":5.0,"quantiles":[[0.5,2.5]]}"#).is_none());
        assert!(native(r#"{"count":1,"scale":-5,"positive":null}"#).is_none());
        assert!(native("not json").is_none());
        let mut gauge = histogram.clone();
        gauge.sum = f64::NAN;
        gauge.hint = ResetHint::Gauge;
        let text = point(&gauge);
        assert!(
            text.contains(r#""sum":"NaN""#) && text.contains(r#""hint":"gauge""#),
            "{text}"
        );
        let back = native(&text).unwrap();
        assert!(back.sum.is_nan());
        assert_eq!(back.hint, ResetHint::Gauge);
        let stale = native(r#"{"count":"StaleNaN","sum":"StaleNaN","scale":0}"#).unwrap();
        assert_eq!(
            stale.sum.to_bits(),
            STALE_NAN_BITS,
            "a staleness marker keeps its bits"
        );
        assert!(point(&stale).contains(r#""sum":"StaleNaN""#));
        let missing_sum =
            native(r#"{"count":1,"sum":null,"scale":0,"positive":{"offset":0,"counts":[1]}}"#)
                .unwrap();
        assert!(missing_sum.sum == 0.0);
    }

    #[test]
    fn classic_histograms_add_bucket_by_bucket_when_bounds_agree() {
        let a =
            r#"{"count":3,"sum":30.0,"bounds":[10.0,20.0],"counts":[1,2,0],"min":4.0,"max":15.0}"#;
        let b =
            r#"{"count":2,"sum":50.0,"bounds":[10.0,20.0],"counts":[0,1,1],"min":12.0,"max":40.0}"#;
        let merged = parsed(&merge(&[a, b]).unwrap());
        assert_eq!(merged["counts"], json!([1, 3, 1]));
        assert_eq!(merged["count"], 5);
        assert_eq!(merged["sum"], 80.0);
        assert_eq!(merged["min"], 4.0);
        assert_eq!(merged["max"], 40.0);
        assert_eq!(merged["bounds"], json!([10.0, 20.0]));
    }

    #[test]
    fn classic_histograms_with_other_bounds_do_not_merge() {
        let a = r#"{"count":1,"bounds":[10.0],"counts":[1,0]}"#;
        let b = r#"{"count":1,"bounds":[5.0],"counts":[0,1]}"#;
        assert_eq!(merge(&[a, b]), None);
        assert_eq!(merge(&[]), None);
        assert_eq!(merge(&[a, "not json"]), None);
    }

    #[test]
    fn exponential_histograms_align_by_offset_at_one_scale() {
        let a = r#"{"count":3,"sum":3.0,"scale":2,"zero_count":1,"zero_threshold":0.0,"positive":{"offset":0,"counts":[1,2]},"negative":null}"#;
        let b = r#"{"count":4,"sum":4.0,"scale":2,"zero_count":2,"zero_threshold":0.5,"positive":{"offset":1,"counts":[3,1]},"negative":{"offset":-2,"counts":[1]}}"#;
        let merged = parsed(&merge(&[a, b]).unwrap());
        assert_eq!(merged["scale"], 2);
        assert_eq!(
            merged["positive"],
            json!({ "offset": 0, "counts": [1, 5, 1] })
        );
        assert_eq!(merged["negative"], json!({ "offset": -2, "counts": [1] }));
        assert_eq!(merged["count"], 7);
        assert_eq!(merged["zero_count"], 3);
        assert_eq!(merged["zero_threshold"], 0.5);
    }

    #[test]
    fn a_finer_exponential_histogram_is_halved_to_the_coarser_scale() {
        let fine = r#"{"count":4,"scale":1,"zero_count":0,"positive":{"offset":0,"counts":[1,1,1,1]},"negative":{"offset":-2,"counts":[2,2]}}"#;
        let coarse = r#"{"count":1,"scale":0,"zero_count":0,"positive":{"offset":1,"counts":[1]},"negative":null}"#;
        let merged = parsed(&merge(&[fine, coarse]).unwrap());
        assert_eq!(merged["scale"], 0);
        assert_eq!(
            merged["positive"],
            json!({ "offset": 0, "counts": [2, 3] }),
            "indices 0,1 fold into 0 and 2,3 into 1"
        );
        assert_eq!(
            merged["negative"],
            json!({ "offset": -1, "counts": [4] }),
            "indices -2,-1 fold into -1"
        );
    }
}
