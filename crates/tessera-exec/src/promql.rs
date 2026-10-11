// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `PromQL` node in the engine: the inputs' rows become series, the
//! kernels evaluate the expression step by step, and the result rows
//! take the range-vector schema (QL-5).
//!
//! A row's labels are its attributes under the label-name mapping of
//! section 9.8 plus `__name__` when the input projected one. Samples
//! of one series are sorted by time and a later row at the same time
//! replaces an earlier one, as deduplication would (section 7.5).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use tessera_ir::promql::PromExpr;
use tessera_ir::{VALUE, range_vector_schema};
use tessera_kernels::promql::{self, NAME_LABEL, Params, Series};
use tessera_kernels::row_labels;
use tessera_types::event::{OVERFLOW, TIMESTAMP};

use crate::ExecError;

/// The rows of `batches` as series, one per label set.
///
/// # Errors
///
/// `Unsupported` when a batch lacks its time or value column.
pub fn series_of(batches: &[RecordBatch]) -> Result<Vec<Series>, ExecError> {
    let mut by_labels: BTreeMap<BTreeMap<String, String>, BTreeMap<i64, f64>> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch
            .column_by_name(TIMESTAMP)
            .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
            .ok_or_else(|| ExecError::Unsupported("promql input has no timestamp".into()))?;
        let values = batch
            .column_by_name(VALUE)
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| ExecError::Unsupported("promql input has no value".into()))?;
        let overflow = batch
            .column_by_name(OVERFLOW)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let names = batch
            .column_by_name(NAME_LABEL)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        for i in 0..batch.num_rows() {
            if values.is_null(i) {
                continue;
            }
            let attributes = overflow.filter(|o| !o.is_null(i)).map(|o| o.value(i));
            let mut labels = row_labels(attributes, None, None);
            if let Some(name) = names.filter(|n| !n.is_null(i)) {
                labels.insert(NAME_LABEL.to_owned(), name.value(i).to_owned());
            }
            by_labels
                .entry(labels)
                .or_default()
                .insert(timestamps.value(i), values.value(i));
        }
    }
    Ok(by_labels
        .into_iter()
        .map(|(labels, samples)| Series {
            labels,
            samples: samples.into_iter().collect(),
        })
        .collect())
}

/// Evaluates `expr` over the series of each input and returns the rows
/// in the range-vector schema.
///
/// # Errors
///
/// If the expression cannot be evaluated or the rows cannot be built.
pub fn evaluate(
    expr: &PromExpr,
    inputs: &[Vec<Series>],
    params: Params,
) -> Result<RecordBatch, ExecError> {
    let rows = promql::evaluate(expr, inputs, params)?;
    let times: Vec<i64> = rows.iter().map(|r| r.time).collect();
    let labels: Vec<Option<String>> = rows
        .iter()
        .map(|r| {
            r.labels
                .as_ref()
                .map(|l| serde_json::to_string(l).unwrap_or_default())
        })
        .collect();
    let values: Vec<f64> = rows.iter().map(|r| r.value).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(times)),
        Arc::new(StringArray::from(labels)),
        Arc::new(Float64Array::from(values)),
    ];
    Ok(RecordBatch::try_new(range_vector_schema(), columns)?)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::*;

    #[test]
    fn rows_become_series_with_names_and_later_samples_win() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIMESTAMP,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(VALUE, DataType::Float64, true),
            Field::new(OVERFLOW, DataType::Utf8, true),
            Field::new(NAME_LABEL, DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![20, 10, 10, 10])),
                Arc::new(Float64Array::from(vec![
                    Some(2.0),
                    Some(1.0),
                    Some(5.0),
                    None,
                ])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"attr.job":"api"}"#),
                    Some(r#"{"attr.job":"api"}"#),
                    Some(r#"{"attr.job":"api"}"#),
                    Some(r#"{"attr.job":"web"}"#),
                ])),
                Arc::new(StringArray::from(vec![Some("up"); 4])),
            ],
        )
        .unwrap();
        let series = series_of(&[batch]).unwrap();
        assert_eq!(series.len(), 1, "a null value makes no series");
        assert_eq!(series[0].labels["__name__"], "up");
        assert_eq!(series[0].labels["job"], "api");
        assert_eq!(series[0].samples, vec![(10, 5.0), (20, 2.0)]);
        let out = evaluate(
            &PromExpr::Selector {
                input: 0,
                range: None,
                offset: 0,
                at: None,
                equal: Vec::new(),
            },
            &[series],
            Params {
                start: 20,
                end: 20,
                step: 0,
            },
        )
        .unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.schema(), range_vector_schema());
    }
}
