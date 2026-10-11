// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Query results kept for closed time ranges (section 10.8).
//!
//! A result is keyed by the plan's fingerprint and the versions of the
//! partition manifests its scans depend on, so a change to any of them
//! is a new key and the old entry is simply never asked for again. Only
//! plans whose every scan ends before [`CLOSED_HORIZON`] ago are kept:
//! their rows change only when late data arrives, and that moves a
//! manifest version once intake adopts it. Rows still in the tail at
//! that point are the one thing the key does not see, so a kept result
//! can lag intake by one round. The cache is local and disposable:
//! bounded in bytes, oldest forgotten first.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use tessera_ir::{Plan, Scan};
use tessera_types::{PartitionId, Timestamp, Version, stable_hash};

/// Bytes of results kept by default.
pub const DEFAULT_BYTES: usize = 64 << 20;
/// How far behind now a scan must end for its result to be kept.
pub const CLOSED_HORIZON: Duration = Duration::from_secs(600);

/// What a kept result depends on.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ResultKey {
    /// The plan, hashed whole.
    pub fingerprint: u64,
    /// The manifests the plan's scans read, with their versions.
    pub versions: Vec<(PartitionId, Version)>,
}

/// A kept result.
#[derive(Debug)]
pub struct Cached {
    /// The schema of the rows.
    pub schema: SchemaRef,
    /// The rows.
    pub batches: Vec<RecordBatch>,
    bytes: usize,
}

#[derive(Debug, Default)]
struct Entries {
    by_key: HashMap<ResultKey, Arc<Cached>>,
    order: VecDeque<ResultKey>,
    bytes: usize,
}

/// Results of closed-range queries, at most `capacity` bytes of them.
#[derive(Debug)]
pub struct ResultCache {
    capacity: usize,
    entries: Mutex<Entries>,
}

impl ResultCache {
    /// A cache of at most `capacity` bytes of rows.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        ResultCache {
            capacity,
            entries: Mutex::new(Entries::default()),
        }
    }

    /// The result kept under `key`, if any.
    #[must_use]
    pub fn get(&self, key: &ResultKey) -> Option<Arc<Cached>> {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .by_key
            .get(key)
            .cloned()
    }

    /// Keeps a result under `key`, unless it alone exceeds the capacity.
    pub fn insert(&self, key: ResultKey, schema: SchemaRef, batches: Vec<RecordBatch>) {
        let bytes = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        if bytes > self.capacity {
            return;
        }
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(old) = entries.by_key.remove(&key) {
            entries.bytes -= old.bytes;
            entries.order.retain(|k| k != &key);
        }
        entries.bytes += bytes;
        entries.order.push_back(key.clone());
        entries.by_key.insert(
            key,
            Arc::new(Cached {
                schema,
                batches,
                bytes,
            }),
        );
        while entries.bytes > self.capacity {
            let Some(oldest) = entries.order.pop_front() else {
                break;
            };
            if let Some(old) = entries.by_key.remove(&oldest) {
                entries.bytes -= old.bytes;
            }
        }
    }

    /// How many results are kept.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .by_key
            .len()
    }

    /// Whether none is kept.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes of rows kept.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .bytes
    }
}

/// The plan, hashed whole.
#[must_use]
pub fn fingerprint(plan: &Plan) -> u64 {
    stable_hash(format!("{plan:?}").as_bytes())
}

/// Every scan in `plan`, in plan order.
#[must_use]
pub fn scans(plan: &Plan) -> Vec<&Scan> {
    let mut found = Vec::new();
    let mut pending = vec![plan];
    while let Some(node) = pending.pop() {
        if let Plan::Scan(scan) = node {
            found.push(scan);
        }
        let mut inputs = node.inputs();
        inputs.reverse();
        pending.extend(inputs);
    }
    found
}

/// Whether `plan` reads at least one scan and every scan ends at least
/// [`CLOSED_HORIZON`] before `now`.
#[must_use]
pub fn is_closed(plan: &Plan, now: Timestamp) -> bool {
    let horizon = i64::try_from(CLOSED_HORIZON.as_nanos()).unwrap_or(i64::MAX);
    let closed_before = Timestamp::from_nanos(now.as_nanos().saturating_sub(horizon));
    let scans = scans(plan);
    !scans.is_empty() && scans.iter().all(|s| s.time_range.end <= closed_before)
}

#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use tessera_types::{DatasetName, Kind, TenantId, TimeRange};

    use super::*;

    fn rows(n: usize) -> (SchemaRef, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values: Vec<i64> = (0..n).map(|i| i64::try_from(i).unwrap()).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        (schema, vec![batch])
    }

    fn key(n: u64) -> ResultKey {
        ResultKey {
            fingerprint: n,
            versions: Vec::new(),
        }
    }

    #[test]
    fn results_are_kept_within_the_byte_budget_oldest_forgotten_first() {
        let (schema, small) = rows(8);
        let size = small[0].get_array_memory_size();
        let cache = ResultCache::new(size * 2);
        cache.insert(key(1), Arc::clone(&schema), small.clone());
        cache.insert(key(2), Arc::clone(&schema), small.clone());
        assert_eq!(cache.len(), 2);
        cache.insert(key(3), Arc::clone(&schema), small.clone());
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&key(1)).is_none(), "the oldest went first");
        assert!(cache.get(&key(3)).is_some());
        assert!(cache.bytes() <= size * 2);
        let (_, big) = rows(10_000);
        cache.insert(key(4), schema, big);
        assert!(cache.get(&key(4)).is_none(), "too big to keep");
    }

    #[test]
    fn a_plan_is_closed_when_every_scan_ends_before_the_horizon() {
        let now = Timestamp::from_nanos(3_600 * 1_000_000_000);
        let mut scan = Scan::all(TenantId::DEFAULT, DatasetName::new("d").unwrap(), Kind::Log);
        assert!(
            !is_closed(&Plan::Scan(scan.clone()), now),
            "all time is open"
        );
        scan.time_range = TimeRange::new(Timestamp::from_nanos(0), Timestamp::from_nanos(1_000));
        let plan = Plan::Limit {
            input: Box::new(Plan::Scan(scan.clone())),
            offset: 0,
            count: Some(1),
        };
        assert!(is_closed(&plan, now));
        assert_eq!(scans(&plan).len(), 1);
        assert_ne!(fingerprint(&plan), fingerprint(&Plan::Scan(scan)));
        assert!(!is_closed(
            &Plan::Values {
                schema: Arc::new(Schema::empty()),
                rows: vec![]
            },
            now
        ));
    }
}
