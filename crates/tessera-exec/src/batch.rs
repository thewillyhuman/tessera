// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! A batch of rows with a selection vector.
//!
//! A filter marks the rows it keeps in a boolean mask instead of copying
//! them (QE-10). The mask travels with the batch until an operator needs
//! the rows contiguous, when [`Batch::materialize`] applies it once.

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::{and, filter_record_batch};
use arrow::datatypes::SchemaRef;

use crate::ExecError;

/// Rows and the mask of those still selected.
#[derive(Clone, Debug)]
pub struct Batch {
    rows: RecordBatch,
    selection: Option<BooleanArray>,
}

impl Batch {
    /// A batch with every row selected.
    #[must_use]
    pub fn new(rows: RecordBatch) -> Self {
        Batch {
            rows,
            selection: None,
        }
    }

    /// The rows, selected or not.
    #[must_use]
    pub fn rows(&self) -> &RecordBatch {
        &self.rows
    }

    /// The rows' schema.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        self.rows.schema()
    }

    /// The selection mask, if any row has been deselected.
    #[must_use]
    pub fn selection(&self) -> Option<&BooleanArray> {
        self.selection.as_ref()
    }

    /// How many rows are selected.
    #[must_use]
    pub fn num_selected(&self) -> usize {
        self.selection
            .as_ref()
            .map_or(self.rows.num_rows(), BooleanArray::true_count)
    }

    /// Whether no row is selected.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.num_selected() == 0
    }

    /// Narrows the selection to the rows where `keep` is true. `keep` has
    /// one entry per row of the batch and no nulls.
    ///
    /// # Errors
    ///
    /// If `keep` has the wrong length.
    pub fn select(self, keep: &BooleanArray) -> Result<Self, ExecError> {
        let selection = match &self.selection {
            None => keep.clone(),
            Some(current) => and(current, keep)?,
        };
        Ok(Batch {
            rows: self.rows,
            selection: Some(selection),
        })
    }

    /// The selected rows, contiguous.
    ///
    /// # Errors
    ///
    /// If the mask cannot be applied.
    pub fn materialize(self) -> Result<RecordBatch, ExecError> {
        match self.selection {
            None => Ok(self.rows),
            Some(mask) if mask.true_count() == self.rows.num_rows() => Ok(self.rows),
            Some(mask) => Ok(filter_record_batch(&self.rows, &mask)?),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn rows() -> RecordBatch {
        let schema = Schema::new(vec![Field::new("n", DataType::Int64, false)]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap()
    }

    #[test]
    fn selections_intersect_and_materialize_once() {
        let batch = Batch::new(rows());
        assert_eq!(batch.num_selected(), 4);
        let batch = batch
            .select(&BooleanArray::from(vec![true, true, false, true]))
            .unwrap();
        assert_eq!(batch.num_selected(), 3);
        let batch = batch
            .select(&BooleanArray::from(vec![false, true, true, true]))
            .unwrap();
        assert_eq!(batch.num_selected(), 2);
        let out = batch.materialize().unwrap();
        let n = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(n.values().to_vec(), vec![2, 4]);
    }

    #[test]
    fn a_full_selection_materializes_without_copying() {
        let batch = Batch::new(rows())
            .select(&BooleanArray::from(vec![true; 4]))
            .unwrap();
        assert!(!batch.is_empty());
        assert_eq!(batch.materialize().unwrap().num_rows(), 4);
    }
}
