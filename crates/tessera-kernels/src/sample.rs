// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Row sampling (CO-6, CO-7): keeps a deterministic share of the rows
//! and sets `sample_rate` on the survivors so weighted aggregates stay
//! unbiased, choosing by trace for spans so a trace is kept or dropped
//! whole. Retention uses it at compaction and budgets at ingest.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, RecordBatch,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use tessera_types::event::{EVENT_ID, SAMPLE_RATE, TRACE_ID};
use tessera_types::{Kind, stable_hash};

/// What sampling kept.
#[derive(Clone, Debug)]
pub struct Sampled {
    /// The kept rows, with `sample_rate` scaled on the sampled ones.
    pub rows: RecordBatch,
    /// Which of the input rows were kept, in input order.
    pub kept: BooleanArray,
}

impl Sampled {
    /// How many rows were dropped.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        u64::try_from(self.kept.false_count()).unwrap_or(0)
    }
}

/// Keeps `rate` of the rows `matches` selects, by trace for spans and by
/// event otherwise, and scales `sample_rate` on the kept matching rows;
/// the rows `matches` leaves out are kept untouched. The same rows and
/// rate always keep the same rows. The rate is a fraction of a million,
/// which a `u64` holds whole.
///
/// # Errors
///
/// If the rows cannot be filtered or rebuilt.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub fn sample(
    rows: &RecordBatch,
    kind: Kind,
    matches: &BooleanArray,
    rate: f64,
) -> Result<Sampled, ArrowError> {
    let identity = if kind == Kind::Span {
        TRACE_ID
    } else {
        EVENT_ID
    };
    let ids = rows
        .column_by_name(identity)
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());
    let threshold = (rate * 1_000_000.0) as u64;
    let keep: BooleanArray = (0..rows.num_rows())
        .map(|i| {
            if !matches.value(i) {
                return Some(true);
            }
            let key = ids
                .filter(|a| !a.is_null(i))
                .map_or(i as u64, |a| stable_hash(a.value(i)));
            Some(key % 1_000_000 < threshold)
        })
        .collect();
    let kept_matches = filter_record_batch(
        &RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("m", DataType::Boolean, false)])),
            vec![Arc::new(matches.clone()) as ArrayRef],
        )?,
        &keep,
    )?;
    let kept = filter_record_batch(rows, &keep)?;
    let kept_matches = kept_matches
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .cloned()
        .unwrap_or_else(|| BooleanArray::from(vec![true; kept.num_rows()]));
    let Some(index) = kept.schema().index_of(SAMPLE_RATE).ok() else {
        return Ok(Sampled {
            rows: kept,
            kept: keep,
        });
    };
    let current = kept
        .column(index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .cloned()
        .unwrap_or_else(|| Float64Array::from(vec![1.0; kept.num_rows()]));
    let scaled: Float64Array = (0..kept.num_rows())
        .map(|i| {
            let was = if current.is_null(i) {
                1.0
            } else {
                current.value(i)
            };
            Some(if kept_matches.value(i) {
                was * rate
            } else {
                was
            })
        })
        .collect();
    let mut columns = kept.columns().to_vec();
    columns[index] = Arc::new(scaled);
    Ok(Sampled {
        rows: RecordBatch::try_new(kept.schema(), columns)?,
        kept: keep,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tessera_types::{DatasetName, EventBatch, LogRow, SpanRow, TenantId, Timestamp};

    use super::*;

    fn logs(n: u8) -> RecordBatch {
        let rows: Vec<LogRow> = (0..n)
            .map(|i| LogRow {
                timestamp: Timestamp::from_nanos(i64::from(i)),
                event_id: [i; 16],
                body: Some("x".into()),
                ..LogRow::default()
            })
            .collect();
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("p").unwrap(), &rows)
            .unwrap()
            .rows()
            .clone()
    }

    fn rates(rows: &RecordBatch) -> Float64Array {
        rows.column_by_name(SAMPLE_RATE)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .clone()
    }

    #[test]
    fn keeps_the_same_rows_each_time_and_scales_the_rate() {
        let rows = logs(200);
        let all = BooleanArray::from(vec![true; 200]);
        let sampled = sample(&rows, Kind::Log, &all, 0.25).unwrap();
        let again = sample(&rows, Kind::Log, &all, 0.25).unwrap();
        let kept = &sampled.rows;
        assert_eq!(kept.num_rows(), again.rows.num_rows());
        assert!(
            kept.num_rows() > 20 && kept.num_rows() < 80,
            "{}",
            kept.num_rows()
        );
        assert_eq!(
            usize::try_from(sampled.dropped()).unwrap(),
            200 - kept.num_rows()
        );
        assert_eq!(sampled.kept.len(), 200, "one answer per input row");
        assert_eq!(sampled.kept.true_count(), kept.num_rows());
        let rates = rates(kept);
        assert!((0..kept.num_rows()).all(|i| (rates.value(i) - 0.25).abs() < 1e-12));
    }

    #[test]
    fn rows_outside_the_selection_are_kept_untouched() {
        let rows = logs(100);
        let none = BooleanArray::from(vec![false; 100]);
        let sampled = sample(&rows, Kind::Log, &none, 0.1).unwrap();
        assert_eq!(sampled.rows.num_rows(), 100);
        assert_eq!(sampled.dropped(), 0);
        let rates = rates(&sampled.rows);
        assert!((0..100).all(|i| (rates.value(i) - 1.0).abs() < 1e-12));
    }

    #[test]
    fn spans_are_kept_or_dropped_by_trace() {
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
        let all = BooleanArray::from(vec![true; 100]);
        let kept = sample(&rows, Kind::Span, &all, 0.5).unwrap().rows;
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
        assert!(per_trace.values().all(|n| *n == 5), "{per_trace:?}");
        assert!(per_trace.len() < 20);
    }
}
