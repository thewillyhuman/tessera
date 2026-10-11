// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `Sort`: order every input row. Blocking; accounts its buffer and,
//! when the grant runs out, spills what it holds as a sorted run to
//! local disk and merges the runs back on finish (QE-11), holding one
//! batch of each run at a time.

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

/// Buffers its input and emits it sorted on `finish`.
pub struct Sort {
    keys: Vec<SortKey>,
    schema: SchemaRef,
    memory: Arc<MemoryPool>,
    buffered: Vec<RecordBatch>,
    reserved: usize,
    /// Runs spilled to disk, each ordered on `keys`.
    runs: Vec<SpillFile>,
}

impl Sort {
    /// A sort on `keys` over rows of `schema`.
    #[must_use]
    pub fn new(keys: Vec<SortKey>, schema: SchemaRef, memory: Arc<MemoryPool>) -> Self {
        Sort {
            keys,
            schema,
            memory,
            buffered: Vec::new(),
            reserved: 0,
            runs: Vec::new(),
        }
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

    /// Emits everything sorted: the buffer alone when nothing spilled,
    /// else the runs on disk merged with the sorted buffer.
    fn emit_sorted(&mut self, out: &mut dyn Sink) -> Result<(), ExecError> {
        let all = concat_batches(&self.schema, &self.buffered)?;
        self.buffered.clear();
        if self.runs.is_empty() {
            if all.num_rows() == 0 {
                return Ok(());
            }
            return sorted(&all, &self.keys, None).and_then(|rows| emit(&rows, out));
        }
        let last = sorted(&all, &self.keys, None)?;
        let mut runs: Vec<Run> = Vec::new();
        for file in &self.runs {
            runs.push(Box::new(file.read()?));
        }
        runs.push(Box::new(slices(&last).into_iter().map(Ok)));
        merge(runs, &self.keys, &self.schema, out)
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
type Run = Box<dyn Iterator<Item = Result<RecordBatch, ExecError>>>;

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

/// Merges `runs`, each ordered on `keys`, into one ordered output.
fn merge(
    runs: Vec<Run>,
    keys: &[SortKey],
    schema: &SchemaRef,
    out: &mut dyn Sink,
) -> Result<(), ExecError> {
    let mut converter = None;
    let mut cursors: Vec<Cursor> = runs
        .into_iter()
        .map(|batches| Cursor {
            batches,
            batch: None,
            rows: None,
            index: 0,
        })
        .collect();
    let mut heap: BinaryHeap<Reverse<(OwnedRow, usize)>> = BinaryHeap::new();
    for (run, cursor) in cursors.iter_mut().enumerate() {
        if cursor.advance(keys, &mut converter)? {
            heap.push(Reverse((cursor.key(), run)));
        }
    }
    // Rows to emit, as (run, row of the run's current batch); flushed
    // before any run moves on, so the batches referenced stay.
    let mut pending: Vec<(usize, usize)> = Vec::new();
    while let Some(Reverse((_, run))) = heap.pop() {
        let cursor = &mut cursors[run];
        pending.push((run, cursor.index));
        cursor.index += 1;
        let batch_done = cursor
            .batch
            .as_ref()
            .is_none_or(|b| cursor.index >= b.num_rows());
        if batch_done || pending.len() >= OUTPUT_ROWS {
            flush(&cursors, &mut pending, schema, out)?;
        }
        let cursor = &mut cursors[run];
        if !batch_done || cursor.advance(keys, &mut converter)? {
            heap.push(Reverse((cursor.key(), run)));
        }
    }
    flush(&cursors, &mut pending, schema, out)
}

/// Emits the pending rows, taken from the cursors' current batches.
fn flush(
    cursors: &[Cursor],
    pending: &mut Vec<(usize, usize)>,
    schema: &SchemaRef,
    out: &mut dyn Sink,
) -> Result<(), ExecError> {
    if pending.is_empty() {
        return Ok(());
    }
    let empty = RecordBatch::new_empty(Arc::clone(schema));
    let batches: Vec<&RecordBatch> = cursors
        .iter()
        .map(|cursor| cursor.batch.as_ref().unwrap_or(&empty))
        .collect();
    let rows = interleave_record_batch(&batches, pending)?;
    pending.clear();
    emit(&rows, out)
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
        let rows = input.materialize()?;
        let bytes = rows.get_array_memory_size();
        if let Err(failure) = self.memory.reserve(bytes) {
            self.spill(failure)?;
            self.memory.reserve(bytes)?;
        }
        self.reserved += bytes;
        self.buffered.push(rows);
        Ok(Flow::Continue)
    }

    fn finish(&mut self, out: &mut dyn Sink) -> Result<(), ExecError> {
        let result = self.emit_sorted(out);
        self.memory.release(self.reserved);
        self.reserved = 0;
        self.buffered.clear();
        self.runs.clear();
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
