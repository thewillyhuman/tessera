// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Grouping: mapping each row's key values to a dense group id.
//!
//! Keys are encoded with Arrow's row format, which gives one comparable,
//! hashable byte string per row across any column types, and looked up
//! in a hash map. Group ids are dense so that accumulators can be plain
//! vectors indexed by group.

use std::collections::HashMap;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::row::{OwnedRow, RowConverter, SortField};

use crate::KernelError;

/// Assigns dense group ids to rows by their key values.
#[derive(Debug)]
pub struct Grouper {
    converter: RowConverter,
    groups: HashMap<OwnedRow, u32>,
    keys: Vec<OwnedRow>,
}

impl Grouper {
    /// A grouper over keys of the given types.
    ///
    /// # Errors
    ///
    /// If a key type cannot be encoded.
    pub fn new(key_types: Vec<DataType>) -> Result<Self, KernelError> {
        let converter = RowConverter::new(key_types.into_iter().map(SortField::new).collect())?;
        Ok(Grouper {
            converter,
            groups: HashMap::new(),
            keys: Vec::new(),
        })
    }

    /// The group id of each row of `keys`, creating groups as needed.
    ///
    /// # Errors
    ///
    /// If the keys cannot be encoded.
    pub fn group_ids(&mut self, keys: &[ArrayRef]) -> Result<Vec<u32>, KernelError> {
        let rows = self.converter.convert_columns(keys)?;
        let mut ids = Vec::with_capacity(rows.num_rows());
        for row in &rows {
            let owned = row.owned();
            let id = if let Some(id) = self.groups.get(&owned) {
                *id
            } else {
                let next = u32::try_from(self.keys.len()).unwrap_or(u32::MAX);
                self.groups.insert(owned.clone(), next);
                self.keys.push(owned);
                next
            };
            ids.push(id);
        }
        Ok(ids)
    }

    /// How many groups have been seen.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether no group has been seen.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// The key columns of every group, in group id order.
    ///
    /// # Errors
    ///
    /// If the rows cannot be decoded.
    pub fn keys(&self) -> Result<Vec<ArrayRef>, KernelError> {
        let rows: Vec<_> = self.keys.iter().map(OwnedRow::row).collect();
        Ok(self.converter.convert_rows(rows)?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array, StringArray};

    use super::*;

    #[test]
    fn equal_keys_share_a_group_across_batches() {
        let mut grouper = Grouper::new(vec![DataType::Utf8, DataType::Int64]).unwrap();
        let first = grouper
            .group_ids(&[
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("a"),
                    None,
                ])),
                Arc::new(Int64Array::from(vec![1, 1, 1, 2])),
            ])
            .unwrap();
        assert_eq!(first, vec![0, 1, 0, 2]);
        let second = grouper
            .group_ids(&[
                Arc::new(StringArray::from(vec![Some("b"), None])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ])
            .unwrap();
        assert_eq!(second, vec![1, 2]);
        assert_eq!(grouper.len(), 3);
        let keys = grouper.keys().unwrap();
        let names = keys[0].as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "a");
        assert_eq!(names.value(1), "b");
        assert!(names.is_null(2));
    }
}
