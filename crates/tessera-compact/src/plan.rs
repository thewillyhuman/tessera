// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Choosing what to compact.
//!
//! The partition owner selects jobs (CP-4). The policy is deliberately
//! small: the L1 files of a partition are merged into one L2 file once
//! there are enough of them, or once a lone L1 file is old enough that
//! nothing more is likely to join it. Grouping by ingest-time window
//! (CP-2) arrives with partitions large enough to need it.

use std::time::Duration;

use tessera_manifest::ManifestState;
use tessera_types::{FileMeta, Level, PartitionId, Timestamp};

/// When a partition is compacted.
#[derive(Clone, Copy, Debug)]
pub struct Policy {
    /// L1 files that trigger a job on their own.
    pub min_inputs: usize,
    /// A lone L1 file is compacted once its newest row is this old.
    pub lone_file_age: Duration,
    /// Inputs are cut off once their total size reaches this.
    pub max_input_bytes: u64,
}

impl Default for Policy {
    fn default() -> Self {
        Policy {
            min_inputs: 4,
            lone_file_age: Duration::from_secs(3_600),
            max_input_bytes: 512 << 20,
        }
    }
}

/// One compaction: these inputs become one output at `level`.
#[derive(Clone, Debug, PartialEq)]
pub struct Job {
    /// The partition.
    pub partition: PartitionId,
    /// The files to merge, in id order.
    pub inputs: Vec<FileMeta>,
    /// The level of the output.
    pub level: Level,
}

/// The job for `state`, if the policy calls for one now.
#[must_use]
pub fn plan(
    partition: &PartitionId,
    state: &ManifestState,
    now: Timestamp,
    policy: &Policy,
) -> Option<Job> {
    if state.is_dropped() {
        return None;
    }
    let mut inputs: Vec<FileMeta> = state
        .files()
        .filter(|f| f.level == Level::L1)
        .cloned()
        .collect();
    inputs.sort_by_key(|f| f.max_timestamp);
    let old_enough = |f: &FileMeta| {
        now.duration_since(f.max_timestamp)
            .is_some_and(|age| age >= policy.lone_file_age)
    };
    let due = inputs.len() >= policy.min_inputs || inputs.iter().any(old_enough);
    if inputs.is_empty() || !due {
        return None;
    }
    let mut total = 0;
    let mut taken = Vec::new();
    for file in inputs {
        if !taken.is_empty() && total + file.size_bytes > policy.max_input_bytes {
            break;
        }
        total += file.size_bytes;
        taken.push(file);
    }
    Some(Job {
        partition: partition.clone(),
        inputs: taken,
        level: Level::L2,
    })
}
