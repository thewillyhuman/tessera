// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `TopK`: the first `k` rows under an ordering, with memory proportional
//! to `k` rather than to the input.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use tessera_ir::SortKey;

use crate::ExecError;
use crate::batch::Batch;
use crate::memory::MemoryPool;
use crate::operator::sort::{emit, sorted};
use crate::operator::{Flow, Operator, Sink};

/// Keeps the best `k` rows seen so far; whenever the buffer holds twice
/// that many, it is sorted and cut back to `k`.
pub struct TopK {
    keys: Vec<SortKey>,
    k: usize,
    schema: SchemaRef,
    memory: Arc<MemoryPool>,
    buffered: Vec<RecordBatch>,
    buffered_rows: usize,
    reserved: usize,
}

impl TopK {
    /// The first `k` rows by `keys` over rows of `schema`.
    #[must_use]
    pub fn new(keys: Vec<SortKey>, k: usize, schema: SchemaRef, memory: Arc<MemoryPool>) -> Self {
        TopK {
            keys,
            k,
            schema,
            memory,
            buffered: Vec::new(),
            buffered_rows: 0,
            reserved: 0,
        }
    }

    fn compact(&mut self) -> Result<RecordBatch, ExecError> {
        let all = concat_batches(&self.schema, &self.buffered)?;
        let best = if all.num_rows() == 0 {
            all
        } else {
            sorted(&all, &self.keys, Some(self.k))?
        };
        self.memory.release(self.reserved);
        self.reserved = best.get_array_memory_size();
        self.memory.reserve(self.reserved)?;
        self.buffered_rows = best.num_rows();
        self.buffered = vec![best.clone()];
        Ok(best)
    }
}

impl Operator for TopK {
    fn push(&mut self, input: Batch, _: &mut dyn Sink) -> Result<Flow, ExecError> {
        if self.k == 0 {
            return Ok(Flow::Done);
        }
        let rows = input.materialize()?;
        let bytes = rows.get_array_memory_size();
        self.memory.reserve(bytes)?;
        self.reserved += bytes;
        self.buffered_rows += rows.num_rows();
        self.buffered.push(rows);
        if self.buffered_rows >= 2 * self.k {
            self.compact()?;
        }
        Ok(Flow::Continue)
    }

    fn finish(&mut self, out: &mut dyn Sink) -> Result<(), ExecError> {
        let result = self.compact().and_then(|best| emit(&best, out));
        self.memory.release(self.reserved);
        self.reserved = 0;
        self.buffered.clear();
        self.buffered_rows = 0;
        result
    }
}

#[cfg(test)]
mod tests {
    use tessera_ir::Expr;

    use super::*;
    use crate::operator::testing::{rows, run, schema, timestamps};

    #[test]
    fn keeps_the_newest_k_across_many_batches() {
        let mut topk = TopK::new(
            vec![SortKey::descending(Expr::column("timestamp"))],
            3,
            schema(),
            MemoryPool::unbounded(),
        );
        let inputs = (0..10).map(|i| rows(&[i, 100 - i, 50])).collect();
        let out = run(&mut topk, inputs);
        assert_eq!(timestamps(&out), vec![100, 99, 98]);
    }

    #[test]
    fn memory_stays_proportional_to_k() {
        let pool = MemoryPool::unbounded();
        let mut topk = TopK::new(
            vec![SortKey::ascending(Expr::column("timestamp"))],
            2,
            schema(),
            Arc::clone(&pool),
        );
        let inputs = (0..50).map(|i| rows(&[i, i + 1, i + 2])).collect();
        let out = run(&mut topk, inputs);
        assert_eq!(timestamps(&out), vec![0, 1]);
        assert_eq!(pool.used(), 0);
    }
}
