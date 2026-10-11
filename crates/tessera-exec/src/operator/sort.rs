// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `Sort`: order every input row. Blocking; accounts its buffer.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::{
    SortColumn, SortOptions, concat_batches, lexsort_to_indices, take_record_batch,
};
use arrow::datatypes::SchemaRef;
use tessera_ir::SortKey;
use tessera_kernels::evaluate;

use crate::ExecError;
use crate::batch::Batch;
use crate::memory::MemoryPool;
use crate::operator::{Flow, Operator, Sink};

/// Rows per output batch of a blocking operator.
pub(crate) const OUTPUT_ROWS: usize = 8_192;

/// Buffers its input and emits it sorted on `finish`.
pub struct Sort {
    keys: Vec<SortKey>,
    schema: SchemaRef,
    memory: Arc<MemoryPool>,
    buffered: Vec<RecordBatch>,
    reserved: usize,
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
        }
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
        let rows = input.materialize()?;
        let bytes = rows.get_array_memory_size();
        self.memory.reserve(bytes)?;
        self.reserved += bytes;
        self.buffered.push(rows);
        Ok(Flow::Continue)
    }

    fn finish(&mut self, out: &mut dyn Sink) -> Result<(), ExecError> {
        let all = concat_batches(&self.schema, &self.buffered)?;
        self.buffered.clear();
        let result = if all.num_rows() == 0 {
            Ok(())
        } else {
            sorted(&all, &self.keys, None).and_then(|rows| emit(&rows, out))
        };
        self.memory.release(self.reserved);
        self.reserved = 0;
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
