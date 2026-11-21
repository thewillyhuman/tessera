// SPDX-License-Identifier: Apache-2.0

//! `RangeVector`: group rows by their full label set and evaluate a
//! range function per series at every step. Blocking; memory is
//! proportional to the samples held, accounted against the pool.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
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
    /// Each series' key, its labels as JSON, and its samples.
    series: Vec<(String, Vec<Sample>)>,
    /// Where each series is, by its key.
    by_key: HashMap<String, usize>,
    /// The series of the rows met so far, by the text of the cells
    /// their labels come from: the rows of a stream carry the same
    /// text, and a dataset of logs has few streams for its rows, so a
    /// row's labels are worked out once for each text, not parsed from
    /// it on every row.
    known: HashMap<String, usize>,
    /// The cells of the row at hand, written here to look them up.
    cells: String,
    reserved: usize,
}

/// The most distinct label cells whose series are remembered: past
/// it, as where an attribute differs on every row, a row's series is
/// worked out from its cells each time.
const MOST_KNOWN: usize = 65_536;

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
            series: Vec::new(),
            by_key: HashMap::new(),
            known: HashMap::new(),
            cells: String::new(),
            reserved: 0,
        }
    }

    /// Where the series of a row with these cells is: its attributes,
    /// the labels a parser stage derived and its severity.
    fn series_of(
        &mut self,
        overflow: Option<&str>,
        extracted: Option<&str>,
        severity: Option<&str>,
    ) -> usize {
        // Each cell with its length before it, so that no two rows'
        // cells write the same text.
        self.cells.clear();
        for cell in [overflow, extracted, severity] {
            match cell {
                Some(text) => {
                    let _ = write!(self.cells, "{}:{text}", text.len());
                }
                None => self.cells.push('-'),
            }
        }
        if let Some(index) = self.known.get(self.cells.as_str()) {
            return *index;
        }
        let labels = self.series_labels(row_labels(overflow, extracted, severity));
        let key = serde_json::to_string(&labels).unwrap_or_default();
        let index = if let Some(index) = self.by_key.get(&key) {
            *index
        } else {
            self.series.push((key.clone(), Vec::new()));
            self.by_key.insert(key, self.series.len() - 1);
            self.series.len() - 1
        };
        if self.known.len() < MOST_KNOWN {
            self.known.insert(self.cells.clone(), index);
        }
        index
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
    /// line's bytes for the byte functions, the unwrapped value
    /// otherwise, and for a rate that was given one.
    #[allow(clippy::cast_precision_loss)]
    fn values(&self, rows: &RecordBatch) -> Result<Vec<Option<f64>>, ExecError> {
        let n = rows.num_rows();
        let counts = match self.function {
            RangeFunction::CountOverTime | RangeFunction::AbsentOverTime => true,
            RangeFunction::Rate => self.value.is_none(),
            _ => false,
        };
        Ok(match self.function {
            _ if counts => vec![Some(1.0); n],
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
            let series = self.series_of(cell(overflow, i), cell(extracted, i), cell(severity, i));
            self.series[series].1.push((timestamps.value(i), value));
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
            let mut all: Vec<Sample> = self
                .series
                .iter()
                .flat_map(|(_, samples)| samples)
                .copied()
                .collect();
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
            // In the order of the keys, whatever order the rows came in.
            self.series.sort_by(|a, b| a.0.cmp(&b.0));
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
        self.by_key.clear();
        self.known.clear();
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

    /// A rate given a value sums it per second, as `LogQL`'s rate over
    /// an unwrapped range does; without one it counts the rows.
    #[test]
    fn a_rate_with_a_value_sums_it_per_second() {
        let input = rows(&[
            (S, "a", r#"{"attr.n":"3"}"#),
            (2 * S, "b", r#"{"attr.n":"5"}"#),
        ]);
        let rate = |value: Option<Expr>| {
            let mut op = RangeVector::new(
                RangeFunction::Rate,
                value,
                Steps {
                    range: Duration::from_secs(4),
                    step: Duration::from_secs(4),
                    start: Timestamp::from_nanos(4 * S),
                    end: Timestamp::from_nanos(4 * S),
                },
                Some(SeriesGrouping {
                    by: true,
                    labels: Vec::new(),
                }),
                MemoryPool::unbounded(),
            );
            series(&run(&mut op, vec![input.clone()]))
        };
        assert_eq!(
            rate(None),
            vec![(4 * S, None, 500)],
            "two rows in four seconds"
        );
        let summed = rate(Some(Expr::Label {
            name: "n".into(),
            data_type: DataType::Float64,
        }));
        assert_eq!(summed, vec![(4 * S, None, 2000)], "eight in four seconds");
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
