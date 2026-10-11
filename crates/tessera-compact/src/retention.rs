// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Applying a retention policy at compaction (CO-9, CP-3): the rows a
//! clause's condition matches are removed, sampled or downsampled on
//! the way to an L3 file. Sampling keeps a deterministic share of the
//! rows and sets `sample_rate` on the survivors so weighted aggregates
//! stay unbiased (CO-6), choosing by trace for spans so a trace is kept
//! or dropped whole (CO-7). Downsampling keeps one row per series and
//! interval with count, sum, min, max and last (CO-8).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, RecordBatch,
    TimestampNanosecondArray, UInt32Array,
};
use arrow::compute::{concat_batches, filter_record_batch, take};
use arrow::datatypes::{DataType, Field, Schema};
use tessera_ir::Expr;
use tessera_kernels::evaluate_predicate;
use tessera_kernels::sample::sample;
use tessera_types::Kind;
use tessera_types::event::{SERIES_ID, TIMESTAMP, VALUE};

use crate::CompactError;

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
        let (sampled, dropped) = sample(&rows, kind, &matches, rate)?;
        applied.sampled_out = dropped;
        rows = sampled;
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

/// One series in one interval, as it is folded.
struct Group {
    last: usize,
    last_time: i64,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

/// Folds the matching metric rows into one per series and interval
/// (CO-8): the last row of the interval, its timestamp aligned to the
/// interval, with the count, sum, min and max of the values alongside.
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
            });
        group.count += 1;
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
    let mut kept: Vec<u32> = untouched;
    let mut stats: Vec<Option<(u64, f64, f64, f64)>> = vec![None; kept.len()];
    let mut aligned: Vec<Option<i64>> = vec![None; kept.len()];
    for ((_, bucket), group) in &groups {
        kept.push(u32::try_from(group.last).unwrap_or(u32::MAX));
        stats.push(Some((group.count, group.sum, group.min, group.max)));
        aligned.push(Some(*bucket));
    }
    let indices = UInt32Array::from(kept);
    let taken: Vec<ArrayRef> = rows
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None))
        .collect::<Result<_, _>>()?;
    let mut batch = RecordBatch::try_new(rows.schema(), taken)?;
    // Align the kept row's time to its interval.
    if let Ok(index) = batch.schema().index_of(TIMESTAMP) {
        let current = batch
            .column(index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .cloned();
        if let Some(current) = current {
            let times: TimestampNanosecondArray = (0..batch.num_rows())
                .map(|i| Some(aligned[i].unwrap_or_else(|| current.value(i))))
                .collect();
            let mut columns = batch.columns().to_vec();
            columns[index] = Arc::new(times);
            batch = RecordBatch::try_new(batch.schema(), columns)?;
        }
    }
    Ok((with_downsample_columns(&batch, &stats)?, folded))
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
    fn metrics_fold_into_one_row_per_series_and_interval() {
        let points: Vec<MetricRow> = (0..60u8)
            .map(|i| MetricRow {
                timestamp: Timestamp::from_nanos(i64::from(i) * 1_000_000_000),
                event_id: [i; 16],
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
}
