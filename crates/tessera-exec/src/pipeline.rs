// SPDX-License-Identifier: Apache-2.0

//! A pipeline: a chain of operators driven by a source of batches.
//!
//! Batches are pushed through the chain one operator at a time; each
//! operator's sink is the rest of the chain. When any consumer answers
//! [`Flow::Done`], the source stops being read.

use crate::ExecError;
use crate::batch::Batch;
use crate::operator::{Flow, Operator, Sink};

/// The operators between a source and a sink.
pub struct Pipeline {
    operators: Vec<Box<dyn Operator>>,
}

/// The rest of a chain, as the sink of the operator before it.
struct Chain<'a> {
    operators: &'a mut [Box<dyn Operator>],
    sink: &'a mut dyn Sink,
}

impl Sink for Chain<'_> {
    fn push(&mut self, batch: Batch) -> Result<Flow, ExecError> {
        match self.operators.split_first_mut() {
            None => self.sink.push(batch),
            Some((first, rest)) => first.push(
                batch,
                &mut Chain {
                    operators: rest,
                    sink: self.sink,
                },
            ),
        }
    }
}

impl Chain<'_> {
    fn finish(&mut self) -> Result<(), ExecError> {
        match self.operators.split_first_mut() {
            None => Ok(()),
            Some((first, rest)) => {
                let mut downstream = Chain {
                    operators: rest,
                    sink: self.sink,
                };
                first.finish(&mut downstream)?;
                downstream.finish()
            }
        }
    }
}

impl Pipeline {
    /// A pipeline of `operators`, first to last.
    #[must_use]
    pub fn new(operators: Vec<Box<dyn Operator>>) -> Self {
        Pipeline { operators }
    }

    /// Whether the pipeline has no operators.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }

    /// Drives every batch of `source` through the pipeline into `sink`,
    /// then finishes the operators in order.
    ///
    /// # Errors
    ///
    /// The first failure of the source, an operator or the sink.
    pub fn run(
        &mut self,
        source: impl IntoIterator<Item = Result<Batch, ExecError>>,
        sink: &mut dyn Sink,
    ) -> Result<(), ExecError> {
        let mut chain = Chain {
            operators: &mut self.operators,
            sink,
        };
        for batch in source {
            if chain.push(batch?)? == Flow::Done {
                break;
            }
        }
        chain.finish()
    }
}

#[cfg(test)]
mod tests {
    use tessera_ir::{BinaryOp, Expr, SortKey};
    use tessera_types::Timestamp;

    use super::*;
    use crate::memory::MemoryPool;
    use crate::operator::testing::{rows, schema, timestamps};
    use crate::operator::{CollectSink, Filter, Limit, Sort};

    #[test]
    fn chains_operators_and_finishes_blocking_ones() {
        let mut pipeline = Pipeline::new(vec![
            Box::new(Filter::new(
                Expr::column("timestamp")
                    .binary(BinaryOp::Gt, Expr::literal(Timestamp::from_nanos(1))),
            )),
            Box::new(Sort::new(
                vec![SortKey::descending(Expr::column("timestamp"))],
                schema(),
                MemoryPool::unbounded(),
            )),
            Box::new(Limit::new(0, Some(2))),
        ]);
        let source = vec![rows(&[1, 5, 3]), rows(&[4, 2])]
            .into_iter()
            .map(|b| Ok(Batch::new(b)));
        let mut sink = CollectSink::new();
        pipeline.run(source, &mut sink).unwrap();
        assert_eq!(timestamps(sink.batches()), vec![5, 4]);
    }

    #[test]
    fn a_satisfied_limit_stops_reading_the_source() {
        let mut pipeline = Pipeline::new(vec![Box::new(Limit::new(0, Some(1)))]);
        let mut read = 0;
        let source = std::iter::from_fn(|| {
            read += 1;
            Some(Ok(Batch::new(rows(&[1]))))
        })
        .take(100);
        let mut sink = CollectSink::new();
        pipeline.run(source, &mut sink).unwrap();
        assert_eq!(sink.num_rows(), 1);
        assert_eq!(read, 1);
    }

    #[test]
    fn an_empty_pipeline_passes_batches_through() {
        let mut pipeline = Pipeline::new(vec![]);
        assert!(pipeline.is_empty());
        let mut sink = CollectSink::new();
        pipeline
            .run(vec![Ok(Batch::new(rows(&[7])))], &mut sink)
            .unwrap();
        assert_eq!(sink.num_rows(), 1);
    }
}
