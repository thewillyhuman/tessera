// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `TopK`: the first `k` rows under an ordering, with memory proportional
//! to `k` rather than to the input.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::{concat_batches, partition, take_record_batch};
use arrow::datatypes::SchemaRef;
use tessera_ir::{Expr, SortKey};
use tessera_kernels::evaluate;

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
    /// Each distinct value of these keeps `k` rows; none keeps `k` in all.
    partition: Vec<Expr>,
    schema: SchemaRef,
    memory: Arc<MemoryPool>,
    buffered: Vec<RecordBatch>,
    buffered_rows: usize,
    reserved: usize,
}

impl TopK {
    /// The first `k` rows by `keys` over rows of `schema`.
    #[must_use]
    pub fn new(
        keys: Vec<SortKey>,
        k: usize,
        partition: Vec<Expr>,
        schema: SchemaRef,
        memory: Arc<MemoryPool>,
    ) -> Self {
        TopK {
            keys,
            k,
            partition,
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
        } else if self.partition.is_empty() {
            sorted(&all, &self.keys, Some(self.k))?
        } else {
            self.best_per_partition(&all)?
        };
        self.memory.release(self.reserved);
        self.reserved = best.get_array_memory_size();
        self.memory.reserve(self.reserved)?;
        self.buffered_rows = best.num_rows();
        self.buffered = vec![best.clone()];
        Ok(best)
    }
}

impl TopK {
    /// The first `k` rows of every partition: the rows sorted by the
    /// partition values and then the keys, each run of equal partition
    /// values cut to `k`.
    fn best_per_partition(&self, all: &RecordBatch) -> Result<RecordBatch, ExecError> {
        let mut keys: Vec<SortKey> = self
            .partition
            .iter()
            .cloned()
            .map(SortKey::ascending)
            .collect();
        keys.extend(self.keys.iter().cloned());
        let ordered = sorted(all, &keys, None)?;
        let columns: Vec<ArrayRef> = self
            .partition
            .iter()
            .map(|expr| evaluate(expr, &ordered))
            .collect::<Result<_, _>>()?;
        let mut indices: Vec<u32> = Vec::new();
        for range in partition(&columns)?.ranges() {
            let end = range.end.min(range.start + self.k);
            indices.extend((range.start..end).map(|i| u32::try_from(i).unwrap_or(u32::MAX)));
        }
        Ok(take_record_batch(&ordered, &UInt32Array::from(indices))?)
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
            Vec::new(),
            schema(),
            MemoryPool::unbounded(),
        );
        let inputs = (0..10).map(|i| rows(&[i, 100 - i, 50])).collect();
        let out = run(&mut topk, inputs);
        assert_eq!(timestamps(&out), vec![100, 99, 98]);
    }

    /// With a partition, every distinct value keeps its own `k` rows.
    #[test]
    fn keeps_k_rows_per_partition() {
        let mut topk = TopK::new(
            vec![SortKey::descending(Expr::column("timestamp"))],
            1,
            vec![Expr::column("severity_number")],
            schema(),
            MemoryPool::unbounded(),
        );
        // Severities are 0, 10 and 20 within every batch of three rows.
        let inputs = (0..4).map(|i| rows(&[i, 100 + i, 200 + i])).collect();
        let out = run(&mut topk, inputs);
        assert_eq!(timestamps(&out), vec![3, 103, 203]);
    }

    #[test]
    fn memory_stays_proportional_to_k() {
        let pool = MemoryPool::unbounded();
        let mut topk = TopK::new(
            vec![SortKey::ascending(Expr::column("timestamp"))],
            2,
            Vec::new(),
            schema(),
            Arc::clone(&pool),
        );
        let inputs = (0..50).map(|i| rows(&[i, i + 1, i + 2])).collect();
        let out = run(&mut topk, inputs);
        assert_eq!(timestamps(&out), vec![0, 1]);
        assert_eq!(pool.used(), 0);
    }
}
