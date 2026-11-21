// SPDX-License-Identifier: Apache-2.0

//! `TopK`: the first `k` rows under an ordering, with memory proportional
//! to `k` rather than to the input. Past the grant, when `k` is large,
//! the rows go through sorted runs on local disk instead and the best
//! `k` are cut from the merged run (QE-11).

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::{concat_batches, partition, take_record_batch};
use arrow::datatypes::{Schema, SchemaRef};
use tessera_ir::{Expr, Registry, SortKey, expr_type};
use tessera_kernels::{Grouper, evaluate};

use crate::ExecError;
use crate::batch::Batch;
use crate::memory::MemoryPool;
use crate::operator::sort::{Run, SortedBuffer, emit, sorted};
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
    /// Every row, in partition and key order, once the grant ran out.
    spilled: Option<SortedBuffer>,
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
            spilled: None,
        }
    }

    /// The order the rows are cut in: by partition, then by the keys.
    fn ordering(&self) -> Vec<SortKey> {
        let mut keys: Vec<SortKey> = self
            .partition
            .iter()
            .cloned()
            .map(SortKey::ascending)
            .collect();
        keys.extend(self.keys.iter().cloned());
        keys
    }

    /// Moves what is buffered, and every row to come, into a sorted
    /// buffer that spills to disk (QE-11).
    fn start_spilling(&mut self, rows: RecordBatch) -> Result<(), ExecError> {
        let mut buffer = SortedBuffer::new(
            self.ordering(),
            Arc::clone(&self.schema),
            Arc::clone(&self.memory),
        );
        self.memory.release(self.reserved);
        self.reserved = 0;
        self.buffered_rows = 0;
        for held in std::mem::take(&mut self.buffered) {
            buffer.push(held)?;
        }
        buffer.push(rows)?;
        self.spilled = Some(buffer);
        Ok(())
    }

    /// Emits the first `k` rows of every partition of `run`, which is
    /// in partition and key order.
    fn cut(&self, run: Run, out: &mut dyn Sink) -> Result<(), ExecError> {
        let mut kept = Kept::new(&self.partition, &self.schema)?;
        for rows in run {
            let rows = rows?;
            let indices = kept.keep(&rows, &self.partition, self.k)?;
            if !indices.is_empty() {
                let best = take_record_batch(&rows, &UInt32Array::from(indices))?;
                emit(&best, out)?;
            }
            if kept.full(self.k) {
                break;
            }
        }
        Ok(())
    }
}

/// How many rows of every partition a cut has kept so far.
struct Kept {
    /// Numbers the partitions; none when all rows are one partition.
    grouper: Option<Grouper>,
    counts: Vec<usize>,
}

impl Kept {
    fn new(partition: &[Expr], schema: &Schema) -> Result<Self, ExecError> {
        let grouper = if partition.is_empty() {
            None
        } else {
            let registry = Registry::builtin();
            let types = partition
                .iter()
                .map(|expr| {
                    expr_type(expr, schema, &registry)
                        .map_err(|e| ExecError::Unsupported(e.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Some(Grouper::new(types)?)
        };
        Ok(Kept {
            grouper,
            counts: Vec::new(),
        })
    }

    /// The rows of `rows` to keep: those of a partition that has fewer
    /// than `k` so far, counted in.
    fn keep(
        &mut self,
        rows: &RecordBatch,
        partition: &[Expr],
        k: usize,
    ) -> Result<Vec<u32>, ExecError> {
        let groups: Vec<usize> = match &mut self.grouper {
            None => vec![0; rows.num_rows()],
            Some(grouper) => {
                let columns: Vec<ArrayRef> = partition
                    .iter()
                    .map(|expr| evaluate(expr, rows))
                    .collect::<Result<_, _>>()?;
                grouper
                    .group_ids(&columns)?
                    .into_iter()
                    .map(|id| usize::try_from(id).unwrap_or(usize::MAX))
                    .collect()
            }
        };
        let mut indices = Vec::new();
        for (row, group) in groups.into_iter().enumerate() {
            if self.counts.len() <= group {
                self.counts.resize(group + 1, 0);
            }
            if self.counts[group] < k {
                self.counts[group] += 1;
                indices.push(u32::try_from(row).unwrap_or(u32::MAX));
            }
        }
        Ok(indices)
    }

    /// Whether nothing more can be kept: known only without partitions,
    /// since another partition may still come.
    fn full(&self, k: usize) -> bool {
        self.grouper.is_none() && self.counts.first().is_some_and(|count| *count >= k)
    }
}

impl TopK {
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
        if let Some(buffer) = &mut self.spilled {
            buffer.push(rows)?;
            return Ok(Flow::Continue);
        }
        let bytes = rows.get_array_memory_size();
        if let Err(failure) = self.memory.reserve(bytes) {
            if self.memory.spill_dir().is_none() {
                return Err(failure.into());
            }
            self.start_spilling(rows)?;
            return Ok(Flow::Continue);
        }
        self.reserved += bytes;
        self.buffered_rows += rows.num_rows();
        self.buffered.push(rows);
        if self.buffered_rows >= 2 * self.k {
            self.compact()?;
        }
        Ok(Flow::Continue)
    }

    fn finish(&mut self, out: &mut dyn Sink) -> Result<(), ExecError> {
        if let Some(mut buffer) = self.spilled.take() {
            let result = buffer.drain().and_then(|run| self.cut(run, out));
            buffer.release();
            return result;
        }
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

    /// QE-11: past its grant a top-k goes through sorted runs on disk
    /// and still gives the best `k`, in all and per partition, leaving
    /// no files behind.
    #[test]
    fn spills_past_its_grant_and_still_keeps_the_best_k() {
        let dir = std::env::temp_dir().join(format!("tessera-topk-spill-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // The grant holds one batch of three rows but not two.
        let one = rows(&[1, 2, 3]).get_array_memory_size();
        let pool = MemoryPool::with_spill_dir(one * 3 / 2, Some(dir.clone()));
        let mut topk = TopK::new(
            vec![SortKey::descending(Expr::column("timestamp"))],
            3,
            Vec::new(),
            schema(),
            Arc::clone(&pool),
        );
        let inputs = (0..30).map(|i| rows(&[i, 100 - i, 50])).collect();
        let out = run(&mut topk, inputs);
        assert_eq!(timestamps(&out), vec![100, 99, 98]);
        assert!(pool.spilled_bytes() > 0, "rows went to disk");
        assert_eq!(pool.used(), 0);

        let pool = MemoryPool::with_spill_dir(one * 3 / 2, Some(dir.clone()));
        let mut per_partition = TopK::new(
            vec![SortKey::descending(Expr::column("timestamp"))],
            1,
            vec![Expr::column("severity_number")],
            schema(),
            Arc::clone(&pool),
        );
        let inputs = (0..30).map(|i| rows(&[i, 100 + i, 200 + i])).collect();
        let out = run(&mut per_partition, inputs);
        let mut got = timestamps(&out);
        got.sort_unstable();
        assert_eq!(got, vec![29, 129, 229]);
        assert!(pool.spilled_bytes() > 0, "rows went to disk");
        assert_eq!(pool.used(), 0);
        assert!(
            std::fs::read_dir(&dir).unwrap().next().is_none(),
            "spill files are removed"
        );
    }
}
