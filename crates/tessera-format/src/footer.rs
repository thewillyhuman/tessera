// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! File footers kept in memory (section 10.8): a Parquet footer costs a
//! range request and a decode per scan, and never changes, since a file
//! is immutable (ST-3). The cache is local and disposable: an entry
//! dropped past the capacity costs one request to read again.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};

use parquet::file::metadata::ParquetMetaData;
use tessera_object::Key;

/// Footers kept by default.
pub const DEFAULT_CAPACITY: usize = 4096;

/// The decoded footers of the files a node has read, oldest forgotten
/// first past the capacity. A file is identified by its key and size,
/// so a key reused for another object, which never happens by design,
/// could not be served the wrong footer.
#[derive(Debug)]
pub struct FooterCache {
    capacity: usize,
    entries: Mutex<Entries>,
}

#[derive(Debug, Default)]
struct Entries {
    by_file: HashMap<(Key, u64), Arc<ParquetMetaData>>,
    order: VecDeque<(Key, u64)>,
}

impl FooterCache {
    /// A cache of at most `capacity` footers.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        FooterCache {
            capacity: capacity.max(1),
            entries: Mutex::new(Entries::default()),
        }
    }

    /// The footer of the `size`-byte file at `key`, if kept.
    #[must_use]
    pub fn get(&self, key: &Key, size: u64) -> Option<Arc<ParquetMetaData>> {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .by_file
            .get(&(key.clone(), size))
            .cloned()
    }

    /// Keeps `footer` for the `size`-byte file at `key`.
    pub fn insert(&self, key: &Key, size: u64, footer: Arc<ParquetMetaData>) {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let id = (key.clone(), size);
        if entries.by_file.insert(id.clone(), footer).is_none() {
            entries.order.push_back(id);
        }
        while entries.order.len() > self.capacity {
            if let Some(oldest) = entries.order.pop_front() {
                entries.by_file.remove(&oldest);
            }
        }
    }

    /// How many footers are kept.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .by_file
            .len()
    }

    /// Whether none is kept.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use parquet::file::metadata::{FileMetaData, RowGroupMetaData};
    use parquet::schema::types::{SchemaDescriptor, Type};

    use super::*;

    fn footer() -> Arc<ParquetMetaData> {
        let schema = Arc::new(Type::group_type_builder("schema").build().unwrap());
        let descriptor = Arc::new(SchemaDescriptor::new(schema));
        let file = FileMetaData::new(1, 0, None, None, descriptor, None);
        Arc::new(ParquetMetaData::new(file, Vec::<RowGroupMetaData>::new()))
    }

    fn key(name: &str) -> Key {
        Key::new(name).unwrap()
    }

    #[test]
    fn footers_are_kept_by_key_and_size_until_forgotten() {
        let cache = FooterCache::new(2);
        cache.insert(&key("a"), 10, footer());
        assert!(cache.get(&key("a"), 10).is_some());
        assert!(
            cache.get(&key("a"), 11).is_none(),
            "another size is another file"
        );
        cache.insert(&key("b"), 10, footer());
        cache.insert(&key("c"), 10, footer());
        assert_eq!(cache.len(), 2);
        assert!(
            cache.get(&key("a"), 10).is_none(),
            "the oldest is forgotten"
        );
        cache.insert(&key("c"), 10, footer());
        assert_eq!(cache.len(), 2, "re-inserting does not count twice");
    }
}
