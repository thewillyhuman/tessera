// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Merging histogram points (CO-8): the JSON a metric row stores for a
//! classic histogram (`bounds` and `counts`) or an exponential one
//! (`scale`, `zero_count` and `positive` and `negative` buckets at an
//! offset), folded over an interval so that downsampled points still
//! answer `histogram_quantile` and the rate functions. Classic
//! histograms merge when their bounds agree; exponential ones are
//! brought to the coarser scale first, halving buckets as the
//! `OpenTelemetry` mapping does.

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

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
