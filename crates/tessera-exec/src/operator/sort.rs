// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `Sort`: order every input row. Blocking; accounts its buffer and,
//! when the grant runs out, spills what it holds as a sorted run to
//! local disk and merges the runs back on finish (QE-11), holding one
//! batch of each run at a time. The [`SortedBuffer`] behind it serves
//! every blocking operator that reads its input in key order.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::{
    SortColumn, SortOptions, concat_batches, interleave_record_batch, lexsort_to_indices,
    take_record_batch,
};
use arrow::datatypes::SchemaRef;
use arrow::row::{OwnedRow, RowConverter, Rows, SortField};
use tessera_ir::SortKey;
use tessera_kernels::evaluate;

use crate::ExecError;
use crate::batch::Batch;
use crate::memory::{MemoryPool, OutOfMemory};
use crate::operator::{Flow, Operator, Sink};
use crate::spill::SpillFile;

/// Rows per output batch of a blocking operator.
pub(crate) const OUTPUT_ROWS: usize = 8_192;

/// Rows kept in key order across memory and disk: what the grant holds
/// stays in memory, the rest goes to local disk as sorted runs when a
/// reservation is refused (QE-11), and everything comes back merged in
/// key order, one batch of each run in memory at a time.
pub(crate) struct SortedBuffer {
    keys: Vec<SortKey>,
    schema: SchemaRef,
    memory: Arc<MemoryPool>,
    buffered: Vec<RecordBatch>,
    reserved: usize,
    /// Runs spilled to disk, each ordered on `keys`.
    runs: Vec<SpillFile>,
}

impl SortedBuffer {
    /// A buffer ordered on `keys` over rows of `schema`.
    pub(crate) fn new(keys: Vec<SortKey>, schema: SchemaRef, memory: Arc<MemoryPool>) -> Self {
        SortedBuffer {
            keys,
            schema,
            memory,
            buffered: Vec::new(),
            reserved: 0,
            runs: Vec::new(),
        }
    }

    /// Adds `rows`, spilling first when the grant refuses them.
    pub(crate) fn push(&mut self, rows: RecordBatch) -> Result<(), ExecError> {
        let bytes = rows.get_array_memory_size();
        if let Err(failure) = self.memory.reserve(bytes) {
            self.spill(failure)?;
            self.memory.reserve(bytes)?;
        }
        self.reserved += bytes;
        self.buffered.push(rows);
        Ok(())
    }

    /// Sorts what is buffered into one run on disk and frees its
    /// memory; without a spill directory, or with nothing buffered,
    /// `failure` stands.
    fn spill(&mut self, failure: OutOfMemory) -> Result<(), ExecError> {
        let Some(dir) = self.memory.spill_dir() else {
            return Err(failure.into());
        };
        if self.buffered.is_empty() {
            return Err(failure.into());
        }
        let all = concat_batches(&self.schema, &self.buffered)?;
        self.buffered.clear();
        let run = sorted(&all, &self.keys, None)?;
        drop(all);
        let file = SpillFile::write(dir, &self.schema, &slices(&run))?;
        self.memory.note_spilled(file.bytes());
        self.runs.push(file);
        self.memory.release(self.reserved);
        self.reserved = 0;
        Ok(())
    }

    /// Everything in key order, in batches of at most [`OUTPUT_ROWS`]:
    /// the buffer alone when nothing spilled, else the runs on disk
    /// merged with the sorted buffer. The memory stays reserved until
    /// [`SortedBuffer::release`].
    pub(crate) fn drain(&mut self) -> Result<Run, ExecError> {
        let all = concat_batches(&self.schema, &self.buffered)?;
        self.buffered.clear();
        let last = if all.num_rows() == 0 {
            all
        } else {
            sorted(&all, &self.keys, None)?
        };
        let in_memory: Run = Box::new(slices(&last).into_iter().map(Ok));
        if self.runs.is_empty() {
            return Ok(in_memory);
        }
        let mut runs: Vec<Run> = Vec::new();
        for file in &self.runs {
            runs.push(Box::new(file.read()?));
        }
        runs.push(in_memory);
        Ok(Box::new(Merged::new(runs, &self.keys, &self.schema)?))
    }

    /// Frees the memory and the runs on disk.
    pub(crate) fn release(&mut self) {
        self.memory.release(self.reserved);
        self.reserved = 0;
        self.buffered.clear();
        self.runs.clear();
    }
}

/// Buffers its input and emits it sorted on `finish`.
pub struct Sort {
    buffer: SortedBuffer,
}

impl Sort {
    /// A sort on `keys` over rows of `schema`.
    #[must_use]
    pub fn new(keys: Vec<SortKey>, schema: SchemaRef, memory: Arc<MemoryPool>) -> Self {
        Sort {
            buffer: SortedBuffer::new(keys, schema, memory),
        }
    }
}

/// `rows` in slices of [`OUTPUT_ROWS`].
fn slices(rows: &RecordBatch) -> Vec<RecordBatch> {
    (0..rows.num_rows())
        .step_by(OUTPUT_ROWS)
        .map(|offset| rows.slice(offset, OUTPUT_ROWS.min(rows.num_rows() - offset)))
        .collect()
}

/// The batches of one sorted run, in order.
pub(crate) type Run = Box<dyn Iterator<Item = Result<RecordBatch, ExecError>>>;

/// Where a merge is in one run: its current batch, the batch's keys as
/// comparable rows, and the next row to take.
struct Cursor {
    batches: Run,
    batch: Option<RecordBatch>,
    rows: Option<Rows>,
    index: usize,
}

impl Cursor {
    /// Moves to the next batch with rows; false once the run is done.
    fn advance(
        &mut self,
        keys: &[SortKey],
        converter: &mut Option<RowConverter>,
    ) -> Result<bool, ExecError> {
        loop {
            let Some(batch) = self.batches.next().transpose()? else {
                self.batch = None;
                self.rows = None;
                return Ok(false);
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let columns: Vec<ArrayRef> = keys
                .iter()
                .map(|key| evaluate(&key.expr, &batch))
                .collect::<Result<_, _>>()?;
            let converter = match converter {
                Some(converter) => converter,
                None => converter.insert(RowConverter::new(
                    columns
                        .iter()
                        .zip(keys)
                        .map(|(column, key)| {
                            SortField::new_with_options(
                                column.data_type().clone(),
                                SortOptions {
                                    descending: key.descending,
                                    nulls_first: !key.descending,
                                },
                            )
                        })
                        .collect(),
                )?),
            };
            self.rows = Some(converter.convert_columns(&columns)?);
            self.batch = Some(batch);
            self.index = 0;
            return Ok(true);
        }
    }

    /// The key of the next row to take.
    fn key(&self) -> OwnedRow {
        match &self.rows {
            Some(rows) => rows.row(self.index).owned(),
            None => unreachable!("a cursor in the heap has a batch"),
        }
    }
}

/// The merge of runs ordered on the same keys, one ordered batch at a
/// time.
struct Merged {
    keys: Vec<SortKey>,
    schema: SchemaRef,
    converter: Option<RowConverter>,
    cursors: Vec<Cursor>,
    heap: BinaryHeap<Reverse<(OwnedRow, usize)>>,
    /// Rows to emit, as (run, row of the run's current batch); taken
    /// before any run moves on, so the batches referenced stay.
    pending: Vec<(usize, usize)>,
}

impl Merged {
    fn new(runs: Vec<Run>, keys: &[SortKey], schema: &SchemaRef) -> Result<Self, ExecError> {
        let mut merged = Merged {
            keys: keys.to_vec(),
            schema: Arc::clone(schema),
            converter: None,
            cursors: runs
                .into_iter()
                .map(|batches| Cursor {
                    batches,
                    batch: None,
                    rows: None,
                    index: 0,
                })
                .collect(),
            heap: BinaryHeap::new(),
            pending: Vec::new(),
        };
        for run in 0..merged.cursors.len() {
            if merged.cursors[run].advance(&merged.keys, &mut merged.converter)? {
                merged.heap.push(Reverse((merged.cursors[run].key(), run)));
            }
        }
        Ok(merged)
    }

    /// The pending rows as one batch, taken from the cursors' current
    /// batches; none when nothing is pending.
    fn take_pending(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        let empty = RecordBatch::new_empty(Arc::clone(&self.schema));
        let batches: Vec<&RecordBatch> = self
            .cursors
            .iter()
            .map(|cursor| cursor.batch.as_ref().unwrap_or(&empty))
            .collect();
        let rows = interleave_record_batch(&batches, &self.pending)?;
        self.pending.clear();
        Ok(Some(rows))
    }

    /// The next ordered batch, or none at the end.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        while let Some(Reverse((_, run))) = self.heap.pop() {
            let cursor = &mut self.cursors[run];
            self.pending.push((run, cursor.index));
            cursor.index += 1;
            let batch_done = cursor
                .batch
                .as_ref()
                .is_none_or(|b| cursor.index >= b.num_rows());
            let taken = if batch_done || self.pending.len() >= OUTPUT_ROWS {
                self.take_pending()?
            } else {
                None
            };
            let cursor = &mut self.cursors[run];
            if !batch_done || cursor.advance(&self.keys, &mut self.converter)? {
                self.heap.push(Reverse((cursor.key(), run)));
            }
            if taken.is_some() {
                return Ok(taken);
            }
        }
        self.take_pending()
    }
}

impl Iterator for Merged {
    type Item = Result<RecordBatch, ExecError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}

/// `rows` ordered by `keys`, at most `limit` of them.
pub(crate) fn sorted(
    rows: &RecordBatch,
    keys: &[SortKey],
    limit: Option<usize>,
) -> Result<RecordBatch, ExecError> {
    let columns = keys
        .iter()
        .map(|key| {
            Ok(SortColumn {
                values: evaluate(&key.expr, rows)?,
                options: Some(SortOptions {
                    descending: key.descending,
                    nulls_first: !key.descending,
                }),
            })
        })
        .collect::<Result<Vec<_>, ExecError>>()?;
    let indices = lexsort_to_indices(&columns, limit)?;
    Ok(take_record_batch(rows, &indices)?)
}

/// Pushes `rows` to `out` in batches of [`OUTPUT_ROWS`].
pub(crate) fn emit(rows: &RecordBatch, out: &mut dyn Sink) -> Result<(), ExecError> {
    emit_flow(rows, out).map(|_| ())
}

/// [`emit`] telling whether the sink wants more, for a streaming
/// operator that may stop its input.
pub(crate) fn emit_flow(rows: &RecordBatch, out: &mut dyn Sink) -> Result<Flow, ExecError> {
    let mut offset = 0;
    while offset < rows.num_rows() {
        let length = OUTPUT_ROWS.min(rows.num_rows() - offset);
        if out.push(Batch::new(rows.slice(offset, length)))? == Flow::Done {
            return Ok(Flow::Done);
        }
        offset += length;
    }
    Ok(Flow::Continue)
}

impl Operator for Sort {
    fn push(&mut self, input: Batch, _: &mut dyn Sink) -> Result<Flow, ExecError> {
        self.buffer.push(input.materialize()?)?;
        Ok(Flow::Continue)
    }

    fn finish(&mut self, out: &mut dyn Sink) -> Result<(), ExecError> {
        let result = self.buffer.drain().and_then(|batches| {
            for batch in batches {
                emit(&batch?, out)?;
            }
            Ok(())
        });
        self.buffer.release();
        result
    }
}

#[cfg(test)]
mod tests {
    use tessera_ir::Expr;

    use super::*;
    use crate::operator::testing::{rows, run, schema, timestamps};

    #[test]
    fn orders_all_input_rows() {
        let mut sort = Sort::new(
            vec![SortKey::descending(Expr::column("timestamp"))],
            schema(),
            MemoryPool::unbounded(),
        );
        let out = run(&mut sort, vec![rows(&[3, 1]), rows(&[2, 9])]);
        assert_eq!(timestamps(&out), vec![9, 3, 2, 1]);
    }

    /// QE-11: past the grant, sorted runs go to disk and come back
    /// merged, in order, with the files gone afterwards.
    #[test]
    fn spills_sorted_runs_and_merges_them_back() {
        let dir = std::env::temp_dir().join(format!("tessera-sort-spill-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let one = rows(&[5, 3]).get_array_memory_size();
        let pool = MemoryPool::with_spill_dir(one * 2, Some(dir.clone()));
        let mut sort = Sort::new(
            vec![SortKey::ascending(Expr::column("timestamp"))],
            schema(),
            Arc::clone(&pool),
        );
        let out = run(
            &mut sort,
            vec![
                rows(&[5, 3]),
                rows(&[9, 1]),
                rows(&[4, 8]),
                rows(&[2, 7]),
                rows(&[6, 0]),
            ],
        );
        assert_eq!(timestamps(&out), vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert!(pool.spilled_bytes() > 0, "two runs went to disk");
        assert_eq!(pool.used(), 0);
        assert!(
            std::fs::read_dir(&dir).unwrap().next().is_none(),
            "spill files are removed"
        );

        let mut descending = Sort::new(
            vec![SortKey::descending(Expr::column("timestamp"))],
            schema(),
            Arc::clone(&pool),
        );
        let out = run(
            &mut descending,
            vec![rows(&[5, 3]), rows(&[9, 1]), rows(&[4, 8])],
        );
        assert_eq!(timestamps(&out), vec![9, 8, 5, 4, 3, 1]);
        std::fs::remove_dir(&dir).unwrap();
    }

    #[test]
    fn fails_by_itself_when_memory_runs_out() {
        let mut sort = Sort::new(
            vec![SortKey::ascending(Expr::column("timestamp"))],
            schema(),
            MemoryPool::new(16),
        );
        let mut sink = crate::operator::CollectSink::new();
        let error = sort
            .push(Batch::new(rows(&[1, 2, 3])), &mut sink)
            .unwrap_err();
        assert!(matches!(error, ExecError::Memory(_)), "{error}");
    }
}
