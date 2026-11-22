// SPDX-License-Identifier: Apache-2.0

//! Applying a retention policy at compaction (CO-9, CP-3): the rows a
//! clause's condition matches are removed, sampled or downsampled on
//! the way to an L3 file. Sampling keeps a deterministic share of the
//! rows and sets `sample_rate` on the survivors so weighted aggregates
//! stay unbiased (CO-6), choosing by trace for spans so a trace is kept
//! or dropped whole (CO-7). Downsampling keeps one row per series and
//! interval with count, sum, min, max and last, and the histograms of
//! an interval's delta points merged into the kept row (CO-8).

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt32Array,
};
use arrow::compute::{concat_batches, filter_record_batch, take};
use arrow::datatypes::{DataType, Field, Schema};
use tessera_ir::Expr;
use tessera_kernels::evaluate_predicate;
use tessera_kernels::sample::sample;
use tessera_types::Kind;
use tessera_types::event::{
    HISTOGRAM, SERIES_ID, STATUS_CODE, TEMPORALITY, TIMESTAMP, TRACE_ID, VALUE,
};

use crate::CompactError;
use crate::spared::Spared;

/// The downsample columns of an L3 metric file: the rows of the
/// interval behind each kept row.
pub const DS_COUNT: &str = "ds_count";
/// The sum of the interval's values.
pub const DS_SUM: &str = "ds_sum";
/// The least of the interval's values.
pub const DS_MIN: &str = "ds_min";
/// The greatest of the interval's values.
pub const DS_MAX: &str = "ds_max";

/// What a policy's due actions do to the rows its clause matches.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Retain {
    /// The clause's row condition; none matches every row.
    pub predicate: Option<Expr>,
    /// The share of matching rows kept, when sampling is due.
    pub sample: Option<f64>,
    /// Whether spans of a trace with an error span are kept whole
    /// rather than sampled (CO-7).
    pub except_errors: bool,
    /// The traces kept whole besides, when errors are spared: those
    /// whoever applies the policy found an error of, or an alert
    /// naming, outside the rows being sampled ([`crate::spared`]).
    pub spared: Arc<Spared>,
    /// The interval kept per series, when downsampling is due.
    pub downsample: Option<Duration>,
    /// Whether matching rows are removed, when a drop is due.
    pub remove: bool,
}

/// What applying a policy did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Applied {
    /// Rows removed by a drop.
    pub removed: u64,
    /// Rows dropped by sampling.
    pub sampled_out: u64,
    /// Rows folded into downsampled rows.
    pub downsampled: u64,
}

/// `rows` after `retain`.
///
/// # Errors
///
/// If the condition cannot be evaluated or a column cannot be built.
pub fn apply(
    rows: &RecordBatch,
    kind: Kind,
    retain: &Retain,
) -> Result<(RecordBatch, Applied), CompactError> {
    let mut applied = Applied::default();
    let matches: BooleanArray = match &retain.predicate {
        Some(predicate) => evaluate_predicate(predicate, rows)?,
        None => BooleanArray::from(vec![true; rows.num_rows()]),
    };
    let mut rows = rows.clone();
    if retain.remove {
        let keep = BooleanArray::from_unary(&matches, |m| !m);
        applied.removed = u64::try_from(rows.num_rows() - keep.true_count()).unwrap_or(0);
        rows = filter_record_batch(&rows, &keep)?;
        return Ok((rows, applied));
    }
    if let Some(rate) = retain.sample.filter(|r| *r < 1.0) {
        let matches = if retain.except_errors && kind == Kind::Span {
            without_errored_traces(&rows, &matches, &retain.spared)
        } else {
            matches.clone()
        };
        let sampled = sample(&rows, kind, &matches, rate)?;
        applied.sampled_out = sampled.dropped();
        rows = sampled.rows;
    }
    if let (Some(interval), Kind::Metric) = (retain.downsample, kind) {
        let matches: BooleanArray = match &retain.predicate {
            Some(predicate) => evaluate_predicate(predicate, &rows)?,
            None => BooleanArray::from(vec![true; rows.num_rows()]),
        };
        let (folded, count) = downsample(&rows, &matches, interval)?;
        applied.downsampled = count;
        rows = folded;
    }
    Ok((rows, applied))
}

/// `matches` with the spans of every trace that has an error span
/// among `rows`, or is one of `spared`, taken out (CO-7), so that
/// sampling leaves those traces whole. A trace whose error span sits
/// in another partition is not seen among the rows: it is spared when
/// it is among `spared`.
fn without_errored_traces(
    rows: &RecordBatch,
    matches: &BooleanArray,
    spared: &Spared,
) -> BooleanArray {
    let traces = rows
        .column_by_name(TRACE_ID)
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());
    let statuses = rows
        .column_by_name(STATUS_CODE)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let (Some(traces), Some(statuses)) = (traces, statuses) else {
        return matches.clone();
    };
    let errored: HashSet<&[u8]> = (0..rows.num_rows())
        .filter(|&i| !statuses.is_null(i) && statuses.value(i) == "error" && !traces.is_null(i))
        .map(|i| traces.value(i))
        .collect();
    if errored.is_empty() && spared.is_empty() {
        return matches.clone();
    }
    let is_kept_whole = |trace: &[u8]| {
        errored.contains(trace) || <[u8; 16]>::try_from(trace).is_ok_and(|id| spared.contains(&id))
    };
    (0..rows.num_rows())
        .map(|i| Some(matches.value(i) && (traces.is_null(i) || !is_kept_whole(traces.value(i)))))
        .collect()
}

/// One series in one interval, as it is folded.
struct Group {
    last: usize,
    last_time: i64,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    /// The rows with a histogram of delta temporality, which merge.
    histograms: Vec<usize>,
}

/// Folds the matching metric rows into one per series and interval
/// (CO-8): the last row of the interval, its timestamp aligned to the
/// interval, with the count, sum, min and max of the values alongside,
/// and the histograms of the interval's delta points merged into it.
/// Cumulative points already cover the interval, so the last one
/// stands as it is.
fn downsample(
    rows: &RecordBatch,
    matches: &BooleanArray,
    interval: Duration,
) -> Result<(RecordBatch, u64), CompactError> {
    let length = i64::try_from(interval.as_nanos())
        .unwrap_or(i64::MAX)
        .max(1);
    let (Some(times), Some(series)) = (
        rows.column_by_name(TIMESTAMP)
            .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>()),
        rows.column_by_name(SERIES_ID)
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>()),
    ) else {
        return Ok((with_downsample_columns(rows, &[])?, 0));
    };
    let values = rows
        .column_by_name(VALUE)
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
    let histograms = rows
        .column_by_name(HISTOGRAM)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let temporalities = rows
        .column_by_name(TEMPORALITY)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let mut groups: BTreeMap<(Vec<u8>, i64), Group> = BTreeMap::new();
    let mut untouched: Vec<u32> = Vec::new();
    for i in 0..rows.num_rows() {
        if !matches.value(i) || series.is_null(i) {
            untouched.push(u32::try_from(i).unwrap_or(u32::MAX));
            continue;
        }
        let at = times.value(i);
        let bucket = at.div_euclid(length) * length;
        let value = values.filter(|v| !v.is_null(i)).map(|v| v.value(i));
        let group = groups
            .entry((series.value(i).to_vec(), bucket))
            .or_insert(Group {
                last: i,
                last_time: at,
                count: 0,
                sum: 0.0,
                min: f64::INFINITY,
                max: f64::NEG_INFINITY,
                histograms: Vec::new(),
            });
        group.count += 1;
        let delta = temporalities.is_some_and(|t| t.is_valid(i) && t.value(i) == "delta");
        if delta && histograms.is_some_and(|h| h.is_valid(i)) {
            group.histograms.push(i);
        }
        if let Some(v) = value {
            group.sum += v;
            group.min = group.min.min(v);
            group.max = group.max.max(v);
        }
        if at >= group.last_time {
            group.last = i;
            group.last_time = at;
        }
    }
    let folded = u64::try_from(rows.num_rows() - untouched.len())
        .unwrap_or(0)
        .saturating_sub(u64::try_from(groups.len()).unwrap_or(0));
    let Kept {
        indices,
        stats,
        aligned,
        merged,
    } = kept_rows(untouched, &groups, histograms);
    let taken: Vec<ArrayRef> = rows
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None))
        .collect::<Result<_, _>>()?;
    let batch = RecordBatch::try_new(rows.schema(), taken)?;
    // Align the kept row's time to its interval.
    let batch = replace_column::<TimestampNanosecondArray>(batch, TIMESTAMP, |current| {
        Arc::new(
            (0..current.len())
                .map(|i| Some(aligned[i].unwrap_or_else(|| current.value(i))))
                .collect::<TimestampNanosecondArray>(),
        )
    })?;
    // Give the kept row the interval's merged histogram, where there is one.
    let batch = replace_column::<StringArray>(batch, HISTOGRAM, |current| {
        Arc::new(
            (0..current.len())
                .map(|i| {
                    merged[i]
                        .clone()
                        .or_else(|| current.is_valid(i).then(|| current.value(i).to_owned()))
                })
                .collect::<StringArray>(),
        )
    })?;
    Ok((with_downsample_columns(&batch, &stats)?, folded))
}

/// The rows a downsample keeps: the untouched ones, then one per
/// group, with what each kept group row carries.
struct Kept {
    indices: UInt32Array,
    stats: Vec<Option<(u64, f64, f64, f64)>>,
    aligned: Vec<Option<i64>>,
    merged: Vec<Option<String>>,
}

fn kept_rows(
    untouched: Vec<u32>,
    groups: &BTreeMap<(Vec<u8>, i64), Group>,
    histograms: Option<&StringArray>,
) -> Kept {
    let mut kept = untouched;
    let mut stats: Vec<Option<(u64, f64, f64, f64)>> = vec![None; kept.len()];
    let mut aligned: Vec<Option<i64>> = vec![None; kept.len()];
    let mut merged: Vec<Option<String>> = vec![None; kept.len()];
    for ((_, bucket), group) in groups {
        kept.push(u32::try_from(group.last).unwrap_or(u32::MAX));
        stats.push(Some((group.count, group.sum, group.min, group.max)));
        aligned.push(Some(*bucket));
        merged.push(
            histograms
                .filter(|_| group.histograms.len() > 1)
                .and_then(|h| {
                    let points: Vec<&str> = group.histograms.iter().map(|&i| h.value(i)).collect();
                    tessera_kernels::histogram::merge(&points)
                }),
        );
    }
    Kept {
        indices: UInt32Array::from(kept),
        stats,
        aligned,
        merged,
    }
}

/// `batch` with column `name` replaced by what `make` makes of it;
/// unchanged when the column is absent or not of type `A`.
fn replace_column<A: Array + 'static>(
    batch: RecordBatch,
    name: &str,
    make: impl FnOnce(&A) -> ArrayRef,
) -> Result<RecordBatch, CompactError> {
    let Ok(index) = batch.schema().index_of(name) else {
        return Ok(batch);
    };
    let Some(current) = batch.column(index).as_any().downcast_ref::<A>() else {
        return Ok(batch);
    };
    let mut columns = batch.columns().to_vec();
    columns[index] = make(current);
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// `rows` with the four downsample columns, from `stats` where given
/// and null elsewhere.
fn with_downsample_columns(
    rows: &RecordBatch,
    stats: &[Option<(u64, f64, f64, f64)>],
) -> Result<RecordBatch, CompactError> {
    if rows.schema().field_with_name(DS_COUNT).is_ok() {
        return Ok(rows.clone());
    }
    let n = rows.num_rows();
    let at = |i: usize| stats.get(i).copied().flatten();
    #[allow(clippy::cast_precision_loss)]
    let counts: Float64Array = (0..n).map(|i| at(i).map(|s| s.0 as f64)).collect();
    let sums: Float64Array = (0..n).map(|i| at(i).map(|s| s.1)).collect();
    let mins: Float64Array = (0..n).map(|i| at(i).map(|s| s.2)).collect();
    let maxs: Float64Array = (0..n).map(|i| at(i).map(|s| s.3)).collect();
    let mut fields: Vec<Field> = rows
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut columns = rows.columns().to_vec();
    for (name, array) in [
        (DS_COUNT, counts),
        (DS_SUM, sums),
        (DS_MIN, mins),
        (DS_MAX, maxs),
    ] {
        fields.push(Field::new(name, DataType::Float64, true));
        columns.push(Arc::new(array));
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// Concatenates batches of one schema, for callers that split rows.
#[must_use]
pub fn joined(batches: &[RecordBatch]) -> Option<RecordBatch> {
    let schema = batches.first()?.schema();
    concat_batches(&schema, batches).ok()
}

#[cfg(test)]
mod tests {
    use tessera_ir::BinaryOp;
    use tessera_types::event::{MetricType, SAMPLE_RATE, SEVERITY_NUMBER, Scalar, TRACE_ID};
    use tessera_types::{DatasetName, EventBatch, LogRow, MetricRow, SpanRow, TenantId, Timestamp};

    use super::*;

    fn logs(n: u8) -> RecordBatch {
        let rows: Vec<LogRow> = (0..n)
            .map(|i| LogRow {
                timestamp: Timestamp::from_nanos(i64::from(i)),
                event_id: [i; 16],
                severity_number: Some(if i % 2 == 0 { 9 } else { 17 }),
                body: Some("x".into()),
                attributes: std::collections::BTreeMap::from([(
                    "attr.i".to_owned(),
                    Scalar::Int(i64::from(i)),
                )]),
                ..LogRow::default()
            })
            .collect();
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("p").unwrap(), &rows)
            .unwrap()
            .rows()
            .clone()
    }

    #[test]
    fn a_drop_with_a_condition_removes_the_matching_rows() {
        let rows = logs(10);
        let retain = Retain {
            predicate: Some(Expr::column(SEVERITY_NUMBER).binary(BinaryOp::Lt, Expr::literal(13))),
            remove: true,
            ..Retain::default()
        };
        let (kept, applied) = apply(&rows, Kind::Log, &retain).unwrap();
        assert_eq!(kept.num_rows(), 5);
        assert_eq!(applied.removed, 5);
    }

    #[test]
    fn sampling_is_deterministic_and_scales_the_sample_rate() {
        let rows = logs(200);
        let retain = Retain {
            sample: Some(0.25),
            ..Retain::default()
        };
        let (kept, applied) = apply(&rows, Kind::Log, &retain).unwrap();
        let (again, _) = apply(&rows, Kind::Log, &retain).unwrap();
        assert_eq!(kept.num_rows(), again.num_rows(), "deterministic");
        assert!(
            kept.num_rows() > 20 && kept.num_rows() < 80,
            "{}",
            kept.num_rows()
        );
        assert_eq!(
            usize::try_from(applied.sampled_out).unwrap(),
            200 - kept.num_rows()
        );
        let rates = kept
            .column_by_name(SAMPLE_RATE)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .clone();
        assert!((0..kept.num_rows()).all(|i| (rates.value(i) - 0.25).abs() < 1e-12));
        let (twice, _) = apply(&kept, Kind::Log, &retain).unwrap();
        let rates = twice
            .column_by_name(SAMPLE_RATE)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .clone();
        assert!((0..twice.num_rows()).all(|i| (rates.value(i) - 0.0625).abs() < 1e-12));
    }

    #[test]
    fn spans_are_sampled_by_trace() {
        let spans: Vec<SpanRow> = (0..100u8)
            .map(|i| SpanRow {
                timestamp: Timestamp::from_nanos(i64::from(i)),
                event_id: [i; 16],
                trace_id: [i / 5; 16],
                span_id: [i; 8],
                name: "op".into(),
                ..SpanRow::default()
            })
            .collect();
        let rows =
            EventBatch::from_spans(TenantId::DEFAULT, DatasetName::new("p").unwrap(), &spans)
                .unwrap()
                .rows()
                .clone();
        let retain = Retain {
            sample: Some(0.5),
            ..Retain::default()
        };
        let (kept, _) = apply(&rows, Kind::Span, &retain).unwrap();
        let traces = kept
            .column_by_name(TRACE_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .clone();
        let mut per_trace: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
        for i in 0..traces.len() {
            *per_trace.entry(traces.value(i).to_vec()).or_default() += 1;
        }
        assert!(
            per_trace.values().all(|n| *n == 5),
            "a kept trace is whole: {per_trace:?}"
        );
        assert!(per_trace.len() < 20);
    }

    #[test]
    fn errored_traces_stay_whole_when_the_policy_says_so() {
        let spans: Vec<SpanRow> = (0..100u8)
            .map(|i| SpanRow {
                timestamp: Timestamp::from_nanos(i64::from(i)),
                event_id: [i; 16],
                trace_id: [i / 5; 16],
                span_id: [i; 8],
                name: "op".into(),
                // One span of trace 3 fails; the rest of its spans do not.
                status_code: if i == 17 {
                    tessera_types::StatusCode::Error
                } else {
                    tessera_types::StatusCode::Ok
                },
                ..SpanRow::default()
            })
            .collect();
        let rows =
            EventBatch::from_spans(TenantId::DEFAULT, DatasetName::new("p").unwrap(), &spans)
                .unwrap()
                .rows()
                .clone();
        let count_of = |kept: &RecordBatch, trace: u8| {
            let traces = kept
                .column_by_name(TRACE_ID)
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .clone();
            (0..traces.len())
                .filter(|&i| traces.value(i) == [trace; 16])
                .count()
        };
        let sampled = Retain {
            sample: Some(0.000_001),
            ..Retain::default()
        };
        let (kept, _) = apply(&rows, Kind::Span, &sampled).unwrap();
        assert_eq!(count_of(&kept, 3), 0, "a tiny rate drops the trace");
        let exempt = Retain {
            sample: Some(0.000_001),
            except_errors: true,
            ..Retain::default()
        };
        let (kept, applied) = apply(&rows, Kind::Span, &exempt).unwrap();
        assert_eq!(count_of(&kept, 3), 5, "the errored trace stays whole");
        assert_eq!(applied.sampled_out, 95);
        // A trace whose error is in another partition, or that an
        // alert names, has no error among these rows: it stays whole
        // when the policy is told to spare it.
        let told = Retain {
            spared: Arc::new(Spared::from([[7; 16]])),
            ..exempt.clone()
        };
        let (kept, applied) = apply(&rows, Kind::Span, &told).unwrap();
        assert_eq!((count_of(&kept, 3), count_of(&kept, 7)), (5, 5));
        assert_eq!(applied.sampled_out, 90);
        // Sparing is for a policy that spares errors.
        let not_asked = Retain {
            spared: Arc::new(Spared::from([[7; 16]])),
            ..sampled
        };
        let (kept, _) = apply(&rows, Kind::Span, &not_asked).unwrap();
        assert_eq!(count_of(&kept, 7), 0);
    }

    #[test]
    fn metrics_fold_into_one_row_per_series_and_interval() {
        let points: Vec<MetricRow> = (0..60u8)
            .map(|i| MetricRow {
                timestamp: Timestamp::from_nanos(i64::from(i) * 1_000_000_000),
                name: "cpu".into(),
                metric_type: MetricType::Gauge,
                value: Some(f64::from(i)),
                series_id: [i % 2; 16],
                ..MetricRow::default()
            })
            .collect();
        let rows =
            EventBatch::from_metrics(TenantId::DEFAULT, DatasetName::new("p").unwrap(), &points)
                .unwrap()
                .rows()
                .clone();
        let retain = Retain {
            downsample: Some(Duration::from_secs(30)),
            ..Retain::default()
        };
        let (kept, applied) = apply(&rows, Kind::Metric, &retain).unwrap();
        assert_eq!(kept.num_rows(), 4, "two series, two intervals");
        assert_eq!(applied.downsampled, 56);
        let counts = kept
            .column_by_name(DS_COUNT)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .clone();
        assert!((0..4).all(|i| (counts.value(i) - 15.0).abs() < f64::EPSILON));
        let times = kept
            .column_by_name(TIMESTAMP)
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .clone();
        assert!(
            (0..4).all(|i| times.value(i) % 30_000_000_000 == 0),
            "aligned"
        );
        let maxs = kept
            .column_by_name(DS_MAX)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .clone();
        assert!((0..4).any(|i| (maxs.value(i) - 59.0).abs() < f64::EPSILON));
    }

    /// CO-8: the delta histogram points of an interval merge into the
    /// kept row; cumulative points keep the last one as it is.
    #[test]
    fn delta_histograms_merge_into_the_kept_row() {
        use tessera_types::event::{HISTOGRAM, Temporality};

        let points = |temporality: Temporality| -> RecordBatch {
            let rows: Vec<MetricRow> = (0..6u8)
                .map(|i| MetricRow {
                    timestamp: Timestamp::from_nanos(i64::from(i) * 1_000_000_000),
                    name: "latency".into(),
                    metric_type: MetricType::Histogram,
                    temporality,
                    histogram: Some(format!(
                        r#"{{"count":1,"sum":{i}.0,"bounds":[10.0,20.0],"counts":[{},{},0]}}"#,
                        u8::from(i % 2 == 0),
                        u8::from(i % 2 == 1)
                    )),
                    series_id: [7; 16],
                    ..MetricRow::default()
                })
                .collect();
            EventBatch::from_metrics(TenantId::DEFAULT, DatasetName::new("p").unwrap(), &rows)
                .unwrap()
                .rows()
                .clone()
        };
        let retain = Retain {
            downsample: Some(Duration::from_secs(30)),
            ..Retain::default()
        };
        let histogram_of = |kept: &RecordBatch| -> serde_json::Value {
            let column = kept
                .column_by_name(HISTOGRAM)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            serde_json::from_str(column.value(0)).unwrap()
        };
        let (kept, _) = apply(&points(Temporality::Delta), Kind::Metric, &retain).unwrap();
        assert_eq!(kept.num_rows(), 1);
        let merged = histogram_of(&kept);
        assert_eq!(merged["counts"], serde_json::json!([3, 3, 0]));
        assert_eq!(merged["count"], 6);
        assert_eq!(merged["sum"], 15.0);
        let (kept, _) = apply(&points(Temporality::Cumulative), Kind::Metric, &retain).unwrap();
        let last = histogram_of(&kept);
        assert_eq!(
            last["count"], 1,
            "a cumulative point already covers the interval"
        );
        assert_eq!(last["sum"], 5.0, "the last point");
    }
}
