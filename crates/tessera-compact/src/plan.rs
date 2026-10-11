// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Choosing what to compact.
//!
//! The partition owner selects jobs (CP-4). The policy is deliberately
//! small: the L1 files of a partition are merged into one L2 file once
//! there are enough of them, or once a lone L1 file is old enough that
//! nothing more is likely to join it. When nothing is due, compacted
//! files written under an older layout epoch than the dataset's are
//! rewritten into the current one (AD-10); the caller budgets those
//! rewrites and offers recent partitions first. Grouping by ingest-time
//! window (CP-2) arrives with partitions large enough to need it.

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
    /// The dataset's current layout epoch (AD-9); compacted files of an
    /// older epoch are re-clustered. Zero asks for no re-clustering.
    pub layout_epoch: u64,
}

impl Default for Policy {
    fn default() -> Self {
        Policy {
            min_inputs: 4,
            lone_file_age: Duration::from_secs(3_600),
            max_input_bytes: 512 << 20,
            layout_epoch: 0,
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
        return recluster(partition, state, policy);
    }
    Some(Job {
        partition: partition.clone(),
        inputs: take_within(inputs, policy.max_input_bytes),
        level: Level::L2,
    })
}

/// A job that rewrites compacted files of an older layout epoch into the
/// current one (AD-10), oldest epoch first; `None` when every compacted
/// file is current.
fn recluster(partition: &PartitionId, state: &ManifestState, policy: &Policy) -> Option<Job> {
    if policy.layout_epoch == 0 {
        return None;
    }
    let mut outdated: Vec<FileMeta> = state
        .files()
        .filter(|f| f.level != Level::L1 && f.layout_epoch < policy.layout_epoch)
        .cloned()
        .collect();
    if outdated.is_empty() {
        return None;
    }
    outdated.sort_by_key(|f| (f.layout_epoch, f.max_timestamp));
    let level = outdated
        .iter()
        .map(|f| f.level)
        .max_by_key(|l| match l {
            Level::L1 => 1,
            Level::L2 => 2,
            Level::L3 => 3,
        })
        .unwrap_or(Level::L2);
    Some(Job {
        partition: partition.clone(),
        inputs: take_within(outdated, policy.max_input_bytes),
        level,
    })
}

/// The leading files whose sizes fit in `limit`, at least one.
fn take_within(files: Vec<FileMeta>, limit: u64) -> Vec<FileMeta> {
    let mut total = 0;
    let mut taken = Vec::new();
    for file in files {
        if !taken.is_empty() && total + file.size_bytes > limit {
            break;
        }
        total += file.size_bytes;
        taken.push(file);
    }
    taken
}

/// The bytes a job reads.
#[must_use]
pub fn input_bytes(job: &Job) -> u64 {
    job.inputs.iter().map(|f| f.size_bytes).sum()
}

#[cfg(test)]
mod tests {
    use tessera_commit::{Action, State};
    use tessera_types::{
        DatasetName, FileId, FileSource, Kind, PoolId, SeededRandom, TenantId, Window,
    };

    use super::*;

    fn partition() -> PartitionId {
        PartitionId {
            tenant: TenantId::DEFAULT,
            dataset: DatasetName::new("prod").unwrap(),
            kind: Kind::Log,
            window: Window::containing(Timestamp::from_nanos(0), Window::DEFAULT_LENGTH).unwrap(),
        }
    }

    fn file(random: &mut SeededRandom, level: Level, epoch: u64, at: i64) -> FileMeta {
        FileMeta {
            file_id: FileId::random(random),
            partition: partition(),
            pool: PoolId::new("root").unwrap(),
            level,
            source: FileSource::Writer {
                writer: tessera_types::WriterId::random(random),
                after: tessera_types::Seq::ZERO,
                through: tessera_types::Seq::ZERO,
            },
            min_timestamp: Timestamp::from_nanos(at),
            max_timestamp: Timestamp::from_nanos(at),
            row_count: 10,
            size_bytes: 100,
            layout_epoch: epoch,
            sort_key: vec!["timestamp".into()],
            columns: Vec::new(),
            indexes: Vec::new(),
        }
    }

    fn state(files: &[FileMeta]) -> ManifestState {
        let mut state = ManifestState::default();
        for file in files {
            state
                .apply(&Action::AddFile(Box::new(file.clone())))
                .unwrap();
        }
        state
    }

    #[test]
    fn stale_compacted_files_are_reclustered_when_nothing_else_is_due() {
        let mut random = SeededRandom::new(1);
        let files = [
            file(&mut random, Level::L2, 0, 10),
            file(&mut random, Level::L2, 1, 20),
            file(&mut random, Level::L1, 0, 30),
        ];
        let now = Timestamp::from_nanos(40);
        let quiet = Policy {
            layout_epoch: 0,
            ..Policy::default()
        };
        assert!(
            plan(&partition(), &state(&files), now, &quiet).is_none(),
            "one young L1 file"
        );
        let current = Policy {
            layout_epoch: 1,
            ..Policy::default()
        };
        let job = plan(&partition(), &state(&files), now, &current).unwrap();
        assert_eq!(job.inputs.len(), 1, "only the epoch-zero file is stale");
        assert_eq!(job.inputs[0].layout_epoch, 0);
        assert_eq!(job.level, Level::L2);
        assert_eq!(input_bytes(&job), 100);
        let newer = Policy {
            layout_epoch: 2,
            ..Policy::default()
        };
        let job = plan(&partition(), &state(&files), now, &newer).unwrap();
        assert_eq!(job.inputs.len(), 2, "both compacted files are stale");
        assert_eq!(job.inputs[0].layout_epoch, 0, "oldest epoch first");
    }

    #[test]
    fn due_l1_files_come_before_reclustering() {
        let mut random = SeededRandom::new(2);
        let mut files: Vec<FileMeta> = (0..4).map(|i| file(&mut random, Level::L1, 0, i)).collect();
        files.push(file(&mut random, Level::L2, 0, 9));
        let policy = Policy {
            layout_epoch: 5,
            ..Policy::default()
        };
        let job = plan(
            &partition(),
            &state(&files),
            Timestamp::from_nanos(10),
            &policy,
        )
        .unwrap();
        assert_eq!(job.inputs.len(), 4);
        assert!(job.inputs.iter().all(|f| f.level == Level::L1));
    }
}
