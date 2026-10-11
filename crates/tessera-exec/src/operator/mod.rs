// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The push-based physical operator (section 18.2) and the operators of
//! the engine core.
//!
//! An operator receives batches through [`Operator::push`] and sends
//! whatever it produces to the [`Sink`] it is given; [`Operator::finish`]
//! flushes what a blocking operator held back. A sink answers
//! [`Flow::Done`] when it needs no more input, so a limit stops its
//! upstream early.

mod aggregate;
mod extract;
mod filter;
mod join;
mod limit;
mod project;
mod range;
mod sort;
mod topk;
mod trace;

use arrow::array::RecordBatch;

use crate::ExecError;
use crate::batch::Batch;

pub use aggregate::HashAggregate;
pub use extract::{Extract, Format};
pub use filter::Filter;
pub use join::{BuildSide, HashJoin, JoinInput, partitioned};
pub use limit::Limit;
pub use project::Project;
pub use range::{RangeVector, Steps};
pub use sort::Sort;
pub use topk::TopK;
pub use trace::TraceGroup;

/// Whether a consumer wants more input.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Flow {
    /// Keep pushing.
    Continue,
    /// Nothing more is needed; the producer may stop.
    Done,
}

/// Where an operator sends its output.
pub trait Sink {
    /// Accepts a batch.
    ///
    /// # Errors
    ///
    /// If the consumer failed.
    fn push(&mut self, batch: Batch) -> Result<Flow, ExecError>;
}

/// A push-based physical operator. A pipeline is a chain of these.
pub trait Operator: Send {
    /// Processes `input`, sending output to `out`.
    ///
    /// # Errors
    ///
    /// If the operator or its sink failed.
    fn push(&mut self, input: Batch, out: &mut dyn Sink) -> Result<Flow, ExecError>;

    /// Signals the end of input; a blocking operator emits here.
    ///
    /// # Errors
    ///
    /// If the operator or its sink failed.
    fn finish(&mut self, out: &mut dyn Sink) -> Result<(), ExecError>;
}

/// A sink that keeps every batch it is given, materialized.
#[derive(Debug, Default)]
pub struct CollectSink {
    batches: Vec<RecordBatch>,
}

impl CollectSink {
    /// An empty collector.
    #[must_use]
    pub fn new() -> Self {
        CollectSink::default()
    }

    /// The batches collected so far.
    #[must_use]
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Takes the collected batches.
    #[must_use]
    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.batches
    }

    /// The total number of rows collected.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }
}

impl Sink for CollectSink {
    fn push(&mut self, batch: Batch) -> Result<Flow, ExecError> {
        if batch.is_empty() {
            return Ok(Flow::Continue);
        }
        self.batches.push(batch.materialize()?);
        Ok(Flow::Continue)
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int32Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

    use super::*;

    /// A log-like schema: `timestamp`, `severity_number`, `body`,
    /// `sample_rate`.
    pub fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("severity_number", DataType::Int32, true),
            Field::new("body", DataType::Utf8, true),
            Field::new("sample_rate", DataType::Float64, false),
        ]))
    }

    /// Rows with the given timestamps, severities `10 * i`, bodies
    /// `row{i}` and a sample rate of one.
    pub fn rows(timestamps: &[i64]) -> RecordBatch {
        let n = timestamps.len();
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(TimestampNanosecondArray::from(timestamps.to_vec())),
                Arc::new(Int32Array::from_iter_values(
                    (0..n).map(|i| i32::try_from(i).unwrap() * 10),
                )),
                Arc::new(StringArray::from_iter_values(
                    (0..n).map(|i| format!("row{i}")),
                )),
                Arc::new(Float64Array::from(vec![1.0; n])),
            ],
        )
        .unwrap()
    }

    /// Runs `operator` over `inputs` and returns everything it emitted.
    pub fn run(operator: &mut dyn Operator, inputs: Vec<RecordBatch>) -> Vec<RecordBatch> {
        let mut sink = CollectSink::new();
        for input in inputs {
            if operator.push(Batch::new(input), &mut sink).unwrap() == Flow::Done {
                break;
            }
        }
        operator.finish(&mut sink).unwrap();
        sink.into_batches()
    }

    /// The `timestamp` values of `batches`, in order.
    pub fn timestamps(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("timestamp")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }
}
