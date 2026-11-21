// SPDX-License-Identifier: Apache-2.0

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

    /// The group id of each row of `keys` among the groups seen so far,
    /// `None` where the row's key is new or has a null in it: a probe,
    /// which creates nothing.
    ///
    /// # Errors
    ///
    /// If the keys cannot be encoded.
    pub fn lookup(&self, keys: &[ArrayRef]) -> Result<Vec<Option<u32>>, KernelError> {
        let rows = self.converter.convert_columns(keys)?;
        Ok(rows
            .iter()
            .enumerate()
            .map(|(i, row)| {
                if keys.iter().any(|k| k.is_null(i)) {
                    None
                } else {
                    self.groups.get(&row.owned()).copied()
                }
            })
            .collect())
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

    /// The bucket, below `buckets`, of every group in group id order:
    /// a hash of the group's key, the same for equal keys in any
    /// grouper of this process, so that an operator can spill its
    /// groups by bucket and fold each bucket back on its own (QE-11).
    #[must_use]
    pub fn buckets(&self, buckets: u32) -> Vec<u32> {
        self.keys.iter().map(|key| bucket(key, buckets)).collect()
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

/// The bucket, below `buckets`, of each row of `keys`: a hash of the
/// row's key values, the same for equal values in any call within a
/// process, so that the two inputs of a join are split alike (QE-11).
///
/// # Errors
///
/// If the keys cannot be encoded.
pub fn buckets_of(keys: &[ArrayRef], buckets: u32) -> Result<Vec<u32>, KernelError> {
    let converter = RowConverter::new(
        keys.iter()
            .map(|k| SortField::new(k.data_type().clone()))
            .collect(),
    )?;
    let rows = converter.convert_columns(keys)?;
    Ok(rows
        .iter()
        .map(|row| bucket(&row.owned(), buckets))
        .collect())
}

fn bucket(key: &OwnedRow, buckets: u32) -> u32 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    u32::try_from(hasher.finish() % u64::from(buckets.max(1))).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array, StringArray};

    use super::*;

    /// QE-11: equal keys land in the same bucket whether they come from a
    /// grouper or from plain columns, so that spilled groups and the
    /// two sides of a join are split alike.
    #[test]
    fn equal_keys_share_a_bucket_across_groupers_and_columns() {
        let keys: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["a", "b", "a", "c"])),
            Arc::new(Int64Array::from(vec![1, 2, 1, 3])),
        ];
        let mut grouper = Grouper::new(vec![DataType::Utf8, DataType::Int64]).unwrap();
        let ids = grouper.group_ids(&keys).unwrap();
        let by_group = grouper.buckets(8);
        let by_row = buckets_of(&keys, 8).unwrap();
        for (row, id) in ids.iter().enumerate() {
            assert_eq!(by_row[row], by_group[*id as usize]);
        }
        assert_eq!(by_row[0], by_row[2]);
        assert!(by_row.iter().all(|b| *b < 8));
    }

    #[test]
    fn a_lookup_finds_seen_keys_and_never_a_null_one() {
        let mut grouper = Grouper::new(vec![DataType::Utf8]).unwrap();
        grouper
            .group_ids(&[Arc::new(StringArray::from(vec![
                Some("a"),
                None,
                Some("b"),
            ]))])
            .unwrap();
        let found = grouper
            .lookup(&[Arc::new(StringArray::from(vec![
                Some("b"),
                Some("c"),
                None,
                Some("a"),
            ]))])
            .unwrap();
        assert_eq!(found, vec![Some(2), None, None, Some(0)]);
        assert_eq!(grouper.len(), 3, "a lookup creates no group");
    }

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
