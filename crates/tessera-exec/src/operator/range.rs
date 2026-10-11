// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `RangeVector`: group rows by their full label set and evaluate a
//! range function per series at every step. Blocking; memory is
//! proportional to the samples held, accounted against the pool.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use tessera_ir::{Expr, RangeFunction, SeriesGrouping, range_vector_schema};
use tessera_kernels::window::{Sample, evaluate as evaluate_window};
use tessera_kernels::{attributes_column, evaluate, row_labels};
use tessera_types::Timestamp;
use tessera_types::event::{BODY, EXTRACTED, SEVERITY_TEXT, TIMESTAMP};

use crate::ExecError;
use crate::batch::Batch;
use crate::memory::MemoryPool;
use crate::operator::sort::emit;
use crate::operator::{Flow, Operator, Sink};

/// Bytes accounted per sample held.
const BYTES_PER_SAMPLE: usize = 16;

/// The steps of a range vector: the window, the step and the first and
/// last step times.
#[derive(Clone, Copy, Debug)]
pub struct Steps {
    /// The window length.
    pub range: Duration,
    /// The distance between steps.
    pub step: Duration,
    /// The first step.
    pub start: Timestamp,
    /// The last step, inclusive.
    pub end: Timestamp,
}

/// Per-series sliding windows.
pub struct RangeVector {
    function: RangeFunction,
    value: Option<Expr>,
    range: i64,
    step: i64,
    start: i64,
    end: i64,
    memory: Arc<MemoryPool>,
    grouping: Option<SeriesGrouping>,
    series: BTreeMap<String, Vec<Sample>>,
    reserved: usize,
}

impl RangeVector {
    /// Evaluates `function` over `(t - range, t]` at every step of `steps`.
    #[must_use]
    pub fn new(
        function: RangeFunction,
        value: Option<Expr>,
        steps: Steps,
        grouping: Option<SeriesGrouping>,
        memory: Arc<MemoryPool>,
    ) -> Self {
        let nanos = |d: Duration| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX);
        RangeVector {
            function,
            value,
            range: nanos(steps.range),
            step: nanos(steps.step),
            start: steps.start.as_nanos(),
            end: steps.end.as_nanos(),
            memory,
            grouping,
            series: BTreeMap::new(),
            reserved: 0,
        }
    }

    /// The labels that identify a row's series.
    fn series_labels(&self, mut labels: BTreeMap<String, String>) -> BTreeMap<String, String> {
        match &self.grouping {
            Some(SeriesGrouping {
                by: true,
                labels: keep,
            }) => {
                labels.retain(|name, _| keep.contains(name));
            }
            Some(SeriesGrouping {
                by: false,
                labels: drop,
            }) => {
                labels.retain(|name, _| !drop.contains(name));
            }
            None => {}
        }
        labels
    }

    /// The value each row contributes: 1 for counting functions, the
    /// line's bytes for the byte functions, the unwrapped value otherwise.
    #[allow(clippy::cast_precision_loss)]
    fn values(&self, rows: &RecordBatch) -> Result<Vec<Option<f64>>, ExecError> {
        let n = rows.num_rows();
        Ok(match self.function {
            RangeFunction::CountOverTime | RangeFunction::Rate | RangeFunction::AbsentOverTime => {
                vec![Some(1.0); n]
            }
            RangeFunction::BytesOverTime | RangeFunction::BytesRate => {
                let body = text(rows, BODY);
                (0..n)
                    .map(|i| {
                        Some(
                            body.filter(|b| !b.is_null(i))
                                .map_or(0.0, |b| b.value(i).len() as f64),
                        )
                    })
                    .collect()
            }
            _ => {
                let Some(value) = &self.value else {
                    return Err(ExecError::Unsupported(format!(
                        "{} needs an unwrapped value",
                        self.function
                    )));
                };
                let array = cast(&evaluate(value, rows)?, &DataType::Float64)?;
                let floats = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        ExecError::Unsupported("unwrapped value is not numeric".into())
                    })?;
                floats.iter().collect()
            }
        })
    }
}

fn text<'a>(rows: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    rows.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
}

fn cell(column: Option<&StringArray>, i: usize) -> Option<&str> {
    column.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

impl Operator for RangeVector {
    fn push(&mut self, input: Batch, _: &mut dyn Sink) -> Result<Flow, ExecError> {
        let rows = input.materialize()?;
        let timestamps = rows
            .column_by_name(TIMESTAMP)
            .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
            .ok_or_else(|| ExecError::Unsupported("range vector input has no timestamp".into()))?;
        let values = self.values(&rows)?;
        let merged = attributes_column(&rows);
        let (overflow, extracted, severity) = (
            merged.as_ref(),
            text(&rows, EXTRACTED),
            text(&rows, SEVERITY_TEXT),
        );
        let mut added = 0;
        for (i, value) in values.into_iter().enumerate() {
            let Some(value) = value else {
                continue;
            };
            let labels = self.series_labels(row_labels(
                cell(overflow, i),
                cell(extracted, i),
                cell(severity, i),
            ));
            let key = serde_json::to_string(&labels).unwrap_or_default();
            self.series
                .entry(key)
                .or_default()
                .push((timestamps.value(i), value));
            added += 1;
        }
        let bytes = added * BYTES_PER_SAMPLE;
        self.memory.reserve(bytes)?;
        self.reserved += bytes;
        Ok(Flow::Continue)
    }

    fn finish(&mut self, out: &mut dyn Sink) -> Result<(), ExecError> {
        let mut times: Vec<i64> = Vec::new();
        let mut labels: Vec<Option<String>> = Vec::new();
        let mut values: Vec<f64> = Vec::new();
        if self.function == RangeFunction::AbsentOverTime {
            // Absence is a property of the whole selection, not of a series.
            let mut all: Vec<Sample> = self.series.values().flatten().copied().collect();
            all.sort_by_key(|s| s.0);
            for (t, v) in evaluate_window(
                self.function,
                &all,
                self.start,
                self.end,
                self.step,
                self.range,
            ) {
                times.push(t);
                labels.push(None);
                values.push(v);
            }
        } else {
            for (key, samples) in &mut self.series {
                samples.sort_by_key(|s| s.0);
                for (t, v) in evaluate_window(
                    self.function,
                    samples,
                    self.start,
                    self.end,
                    self.step,
                    self.range,
                ) {
                    times.push(t);
                    labels.push(if key == "{}" { None } else { Some(key.clone()) });
                    values.push(v);
                }
            }
        }
        self.memory.release(self.reserved);
        self.reserved = 0;
        self.series.clear();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from(times)),
            Arc::new(StringArray::from(labels)),
            Arc::new(Float64Array::from(values)),
        ];
        let batch = RecordBatch::try_new(range_vector_schema(), columns)?;
        emit(&batch, out)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{Field, Schema, TimeUnit};

    use super::*;
    use crate::operator::testing::run;
    use tessera_types::event::OVERFLOW;

    const S: i64 = 1_000_000_000;

    fn rows(samples: &[(i64, &str, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIMESTAMP,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(BODY, DataType::Utf8, true),
            Field::new(OVERFLOW, DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(
                    samples.iter().map(|s| s.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    samples.iter().map(|s| s.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    samples.iter().map(|s| s.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    /// Rows as (time, labels, value in thousandths), so that they compare
    /// exactly.
    #[allow(clippy::cast_possible_truncation)]
    fn series(out: &[RecordBatch]) -> Vec<(i64, Option<String>, i64)> {
        out.iter()
            .flat_map(|b| {
                let t = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                let l = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                let v = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
                (0..b.num_rows())
                    .map(|i| {
                        (
                            t.value(i),
                            (!l.is_null(i)).then(|| l.value(i).to_owned()),
                            (v.value(i) * 1000.0).round() as i64,
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn evaluates_per_series_at_each_step() {
        let a = r#"{"attr.app":"a"}"#;
        let b = r#"{"attr.app":"b"}"#;
        let input = rows(&[
            (S, "x", a),
            (2 * S, "yy", a),
            (2 * S, "z", b),
            (5 * S, "w", a),
        ]);
        let mut op = RangeVector::new(
            RangeFunction::CountOverTime,
            None,
            Steps {
                range: Duration::from_secs(2),
                step: Duration::from_secs(2),
                start: Timestamp::from_nanos(2 * S),
                end: Timestamp::from_nanos(6 * S),
            },
            None,
            MemoryPool::unbounded(),
        );
        let out = series(&run(&mut op, vec![input.clone()]));
        assert_eq!(
            out,
            vec![
                (2 * S, Some(r#"{"app":"a"}"#.into()), 2000),
                (6 * S, Some(r#"{"app":"a"}"#.into()), 1000),
                (2 * S, Some(r#"{"app":"b"}"#.into()), 1000),
            ]
        );
        let mut bytes = RangeVector::new(
            RangeFunction::BytesOverTime,
            None,
            Steps {
                range: Duration::from_secs(10),
                step: Duration::from_secs(10),
                start: Timestamp::from_nanos(10 * S),
                end: Timestamp::from_nanos(10 * S),
            },
            None,
            MemoryPool::unbounded(),
        );
        let out = series(&run(&mut bytes, vec![input.clone()]));
        assert_eq!(out[0].2, 4000, "x, yy and w");
        let mut absent = RangeVector::new(
            RangeFunction::AbsentOverTime,
            None,
            Steps {
                range: Duration::from_secs(1),
                step: Duration::from_secs(1),
                start: Timestamp::from_nanos(3 * S),
                end: Timestamp::from_nanos(4 * S),
            },
            None,
            MemoryPool::unbounded(),
        );
        let out = series(&run(&mut absent, vec![input]));
        assert_eq!(out, vec![(3 * S, None, 1000), (4 * S, None, 1000)]);
    }

    #[test]
    fn unwraps_a_value_expression() {
        let input = rows(&[
            (S, "a", r#"{"attr.n":"3"}"#),
            (2 * S, "b", r#"{"attr.n":"5"}"#),
            (3 * S, "c", r#"{"attr.n":"x"}"#),
        ]);
        let mut op = RangeVector::new(
            RangeFunction::SumOverTime,
            Some(Expr::Label {
                name: "n".into(),
                data_type: DataType::Float64,
            }),
            Steps {
                range: Duration::from_secs(5),
                step: Duration::from_secs(5),
                start: Timestamp::from_nanos(5 * S),
                end: Timestamp::from_nanos(5 * S),
            },
            None,
            MemoryPool::unbounded(),
        );
        let out = series(&run(&mut op, vec![input]));
        let total: i64 = out.iter().map(|s| s.2).sum();
        assert_eq!(total, 8000, "{out:?}");
        assert_eq!(
            out.len(),
            2,
            "one series per label set; the unparsable row is dropped"
        );
    }
}
