// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `PromQL` node in the engine: the inputs' rows become series, the
//! kernels evaluate the expression step by step, and the result rows
//! take the range-vector schema (QL-5).
//!
//! A row's labels are its attributes under the label-name mapping of
//! section 9.8 plus `__name__` when the input projected one. Samples
//! of one series are sorted by time and a later row at the same time
//! replaces an earlier one, as deduplication would (section 7.5).
//!
//! A row holding a histogram point becomes the classic series the
//! selector asked for through its [`EXPAND`] column: `bucket` gives one
//! series per `le`, `count` and `sum` the point's totals, and a plain
//! selector a summary's series per `quantile`.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use tessera_ir::promql::PromExpr;
use tessera_ir::{VALUE, range_vector_schema};
use tessera_kernels::promql::{self, NAME_LABEL, Params, Point, Series, format_value};
use tessera_kernels::{attributes_column, histogram, row_labels};
use tessera_types::event::{HISTOGRAM, TIMESTAMP};

use crate::ExecError;

/// The column a selector projects to say which classic series a
/// histogram row stands for: `bucket`, `count`, `sum` or `plain`.
pub const EXPAND: &str = "__expand__";

/// The samples one histogram row yields under `form`: the labels it
/// adds and the value of each.
fn expanded(point: &str, form: &str) -> Vec<(Option<(&'static str, String)>, f64)> {
    match form {
        "bucket" => histogram::classic_buckets(point)
            .unwrap_or_default()
            .into_iter()
            .map(|(le, count)| (Some(("le", format_value(le))), count))
            .collect(),
        "count" => histogram::count_and_sum(point)
            .map(|(count, _)| (None, count))
            .into_iter()
            .collect(),
        "sum" => histogram::count_and_sum(point)
            .and_then(|(_, sum)| sum.map(|sum| (None, sum)))
            .into_iter()
            .collect(),
        _ => histogram::quantiles(point)
            .unwrap_or_default()
            .into_iter()
            .map(|(quantile, value)| (Some(("quantile", format_value(quantile))), value))
            .collect(),
    }
}

/// The text in `column` at `i`, if any.
fn cell(column: Option<&StringArray>, i: usize) -> Option<&str> {
    column.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

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
        let merged = attributes_column(batch);
        let overflow = merged.as_ref();
        let text = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        };
        let (names, points, forms) = (text(NAME_LABEL), text(HISTOGRAM), text(EXPAND));
        for i in 0..batch.num_rows() {
            let samples: Vec<(Option<(&str, String)>, f64)> =
                match (cell(points, i), cell(forms, i), values.is_null(i)) {
                    (Some(point), Some(form), _) => expanded(point, form),
                    (_, _, true) => continue,
                    (_, _, false) => vec![(None, values.value(i))],
                };
            let attributes = overflow.filter(|o| !o.is_null(i)).map(|o| o.value(i));
            let mut labels = row_labels(attributes, None, None);
            if let Some(name) = cell(names, i) {
                labels.insert(NAME_LABEL.to_owned(), name.to_owned());
            }
            for (extra, value) in samples {
                let mut labels = labels.clone();
                if let Some((key, text)) = extra {
                    labels.insert(key.to_owned(), text);
                }
                by_labels
                    .entry(labels)
                    .or_default()
                    .insert(timestamps.value(i), value);
            }
        }
    }
    Ok(by_labels
        .into_iter()
        .map(|(labels, samples)| Series {
            labels,
            samples: samples
                .into_iter()
                .map(|(t, v)| (t, Point::Float(v)))
                .collect(),
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
    let values: Vec<f64> = rows
        .iter()
        .map(|r| r.value.float().unwrap_or(f64::NAN))
        .collect();
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
    use tessera_types::event::OVERFLOW;

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
        assert_eq!(
            series[0].samples,
            vec![(10, Point::Float(5.0)), (20, Point::Float(2.0))]
        );
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

    /// QL-5: a histogram row becomes the classic series its selector
    /// asked for.
    #[test]
    fn histogram_rows_expand_into_classic_series() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIMESTAMP,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(VALUE, DataType::Float64, true),
            Field::new(OVERFLOW, DataType::Utf8, true),
            Field::new(NAME_LABEL, DataType::Utf8, true),
            Field::new(HISTOGRAM, DataType::Utf8, true),
            Field::new(EXPAND, DataType::Utf8, true),
        ]));
        let point = r#"{"count":3,"sum":30.0,"bounds":[10.0,20.0],"counts":[1,2,0]}"#;
        let batch = |form: &str| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(TimestampNanosecondArray::from(vec![10, 20])),
                    Arc::new(Float64Array::from(vec![None, Some(7.0)])),
                    Arc::new(StringArray::from(vec![Some(r#"{"attr.job":"api"}"#); 2])),
                    Arc::new(StringArray::from(vec![Some("latency_bucket"); 2])),
                    Arc::new(StringArray::from(vec![Some(point), None])),
                    Arc::new(StringArray::from(vec![Some(form); 2])),
                ],
            )
            .unwrap()
        };
        let series = series_of(&[batch("bucket")]).unwrap();
        let les: Vec<(String, f64)> = series
            .iter()
            .filter_map(|s| Some((s.labels.get("le")?.clone(), s.samples[0].1.float()?)))
            .collect();
        assert_eq!(
            les,
            vec![
                ("+Inf".to_owned(), 3.0),
                ("10".to_owned(), 1.0),
                ("20".to_owned(), 3.0)
            ],
            "cumulative counts per upper bound, in label order"
        );
        let near = |series: &[Series], value: f64| {
            series
                .iter()
                .any(|s| (s.samples[0].1.float().unwrap_or(f64::NAN) - value).abs() < f64::EPSILON)
        };
        assert!(
            series.iter().any(|s| !s.labels.contains_key("le")
                && (s.samples[0].1.float().unwrap_or(f64::NAN) - 7.0).abs() < f64::EPSILON),
            "a plain row beside them stays a plain sample"
        );
        let sums = series_of(&[batch("sum")]).unwrap();
        assert!(near(&sums, 30.0), "{sums:?}");
        let counts = series_of(&[batch("count")]).unwrap();
        assert!(near(&counts, 3.0), "{counts:?}");
    }
}
