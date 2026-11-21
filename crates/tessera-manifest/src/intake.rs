// SPDX-License-Identifier: Apache-2.0

//! Intake (spec section 6.7): moves L1 files from writer logs into
//! partition manifests. Metadata work only.
//!
//! For one dataset and writer, intake reads the writer's flush records in
//! order and asks each partition a record touches to adopt the record's
//! files for that partition together with the writer's new position, in
//! one commit (IT-1). Adoption is idempotent (IT-2), so intake can start
//! over from the beginning of the log, run twice, or run on two nodes at
//! once. When the log has ended and every record is adopted, the writer
//! leaves the dataset's registry (IT-3).

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::TryStreamExt;
use tessera_object::ObjectStore;
use tessera_types::{DatasetName, Env, FileMeta, PartitionId, Seq, TenantId, WriterId};
use tessera_wal::{EntryBody, reader};

use crate::{Manifest, ManifestError, registry};

/// What one intake run did.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct IntakeOutcome {
    /// Flush records of the dataset seen in the log.
    pub flushes: usize,
    /// Manifest commits made.
    pub adoptions: usize,
    /// Whether the log has ended.
    pub ended: bool,
    /// Whether the writer was removed from the registry.
    pub removed: bool,
    /// The position of the last entry read: every entry up to it has
    /// been dealt with for this dataset, so a later run may start
    /// after it ([`run_after`]).
    pub through: Seq,
}

/// Runs intake for `writer` and one dataset over the whole log.
///
/// # Errors
///
/// If the log cannot be read or a manifest cannot be committed.
pub async fn run(
    store: Arc<dyn ObjectStore>,
    env: &Env,
    tenant: &TenantId,
    dataset: &DatasetName,
    writer: WriterId,
) -> Result<IntakeOutcome, ManifestError> {
    run_after(store, env, tenant, dataset, writer, Seq::ZERO).await
}

/// Runs intake for `writer` and one dataset over the entries after
/// `after`: the `through` of an earlier run for the same writer and
/// dataset, or [`Seq::ZERO`] for the whole log.
///
/// Resuming is what keeps a round's work to what the writer appended
/// since the last one; a log is otherwise read whole every time, and
/// it holds an hour of segments (GC-2). Starting over is always
/// allowed (IT-2), so a position that was lost costs one full read and
/// nothing else.
///
/// # Errors
///
/// If the log cannot be read or a manifest cannot be committed; the
/// run can then be repeated from the same position.
pub async fn run_after(
    store: Arc<dyn ObjectStore>,
    env: &Env,
    tenant: &TenantId,
    dataset: &DatasetName,
    writer: WriterId,
    after: Seq,
) -> Result<IntakeOutcome, ManifestError> {
    let mut outcome = IntakeOutcome {
        through: after,
        ..IntakeOutcome::default()
    };
    let mut adopted = None;
    let mut entries = reader::entries(Arc::clone(&store), writer, after, Arc::clone(&env.keys));
    while let Some(entry) = entries.try_next().await? {
        outcome.through = entry.seq();
        match entry.body() {
            EntryBody::Flush {
                tenant: t,
                dataset: d,
                through,
                files,
                ..
            } if t == tenant && d == dataset => {
                outcome.flushes += 1;
                for (partition, files) in by_partition(files) {
                    let manifest = Manifest::open(Arc::clone(&store), partition, env.clone());
                    if manifest.adopt(writer, *through, &files).await? {
                        outcome.adoptions += 1;
                    }
                }
                // Every partition the record touches has it now, and
                // those of the records before it, which this run or an
                // earlier one went through first.
                adopted = Some(*through);
            }
            EntryBody::End => outcome.ended = true,
            // The entries come unlocked: none is a flush still sealed.
            EntryBody::Flush { .. }
            | EntryBody::SealedFlush { .. }
            | EntryBody::Segment { .. }
            | EntryBody::Seal { .. } => {}
        }
    }
    if outcome.ended {
        registry::remove(&*store, tenant, dataset, writer).await?;
        outcome.removed = true;
    } else if let Some(through) = adopted {
        // Readers start after what is adopted (the registration says
        // how far), in place of the log's first entry.
        registry::advance(&*store, tenant, dataset, writer, through).await?;
    }
    Ok(outcome)
}

fn by_partition(files: &[FileMeta]) -> BTreeMap<PartitionId, Vec<FileMeta>> {
    let mut groups: BTreeMap<PartitionId, Vec<FileMeta>> = BTreeMap::new();
    for file in files {
        groups
            .entry(file.partition.clone())
            .or_default()
            .push(file.clone());
    }
    groups
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tessera_format::ParquetFormat;
    use tessera_object::MemoryStore;
    use tessera_types::{EventBatch, Kind, LogRow, PoolId, Timestamp};
    use tessera_wal::{FlushTarget, WriterLog, write_l1_files};

    use super::*;
    use crate::list_partitions;

    const WINDOW: Duration = Duration::from_secs(86_400);

    fn env() -> Env {
        Env::simulated(1, Timestamp::UNIX_EPOCH)
    }

    fn dataset() -> DatasetName {
        DatasetName::new("prod").unwrap()
    }

    fn batch(dataset: &str, day: i64, count: i64) -> EventBatch {
        let rows: Vec<LogRow> = (0..count)
            .map(|i| LogRow {
                timestamp: Timestamp::from_nanos(day * 86_400_000_000_000 + i),
                body: Some("x".into()),
                ..LogRow::default()
            })
            .collect();
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new(dataset).unwrap(), &rows).unwrap()
    }

    /// A writer that wrote two segments and flushed them: day 0 and day 1
    /// of prod, and day 0 of staging.
    async fn writer_with_flushes(store: &MemoryStore) -> WriterLog {
        let target = FlushTarget {
            store: Arc::new(store.clone()),
            pool: PoolId::new("root").unwrap(),
            format: Arc::new(ParquetFormat::default()),
        };
        let mut log = WriterLog::start(Arc::new(store.clone()), env());
        registry::register(
            store,
            &env(),
            &TenantId::DEFAULT,
            &dataset(),
            log.writer_id(),
        )
        .await
        .unwrap();
        log.append_segment(&[batch("prod", 0, 2), batch("staging", 0, 1)])
            .await
            .unwrap();
        log.append_segment(&[batch("prod", 1, 3)]).await.unwrap();
        for (name, rows) in [
            ("prod", vec![batch("prod", 0, 2), batch("prod", 1, 3)]),
            ("staging", vec![batch("staging", 0, 1)]),
        ] {
            let files = write_l1_files(
                &target,
                &env(),
                log.writer_id(),
                Seq::ZERO,
                Seq::new(2),
                WINDOW,
                rows,
            )
            .await
            .unwrap();
            log.append(EntryBody::Flush {
                tenant: TenantId::DEFAULT,
                dataset: DatasetName::new(name).unwrap(),
                after: Seq::ZERO,
                through: Seq::new(2),
                files,
            })
            .await
            .unwrap();
        }
        log
    }

    #[tokio::test]
    async fn adopts_each_flush_record_into_the_partitions_it_touches() {
        let store = MemoryStore::new();
        let log = writer_with_flushes(&store).await;
        let outcome = run(
            Arc::new(store.clone()),
            &env(),
            &TenantId::DEFAULT,
            &dataset(),
            log.writer_id(),
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            IntakeOutcome {
                flushes: 1,
                adoptions: 2,
                ended: false,
                removed: false,
                through: Seq::new(4),
            }
        );
        let partitions = list_partitions(&store, &TenantId::DEFAULT, &dataset(), Kind::Log)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 2);
        for partition in &partitions {
            let state = Manifest::open(Arc::new(store.clone()), partition.clone(), env())
                .snapshot()
                .await
                .unwrap();
            assert_eq!(state.state().files().count(), 1);
            assert_eq!(state.state().position(log.writer_id()), Seq::new(2));
        }
        let registered = registry::list(&store, &TenantId::DEFAULT, &dataset())
            .await
            .unwrap();
        assert_eq!(
            registered[0].adopted,
            Seq::new(2),
            "the registration tells readers how far the flushes are adopted"
        );
        assert!(
            list_partitions(
                &store,
                &TenantId::DEFAULT,
                &DatasetName::new("staging").unwrap(),
                Kind::Log
            )
            .await
            .unwrap()
            .is_empty(),
            "other datasets are untouched"
        );
    }

    #[tokio::test]
    async fn running_again_changes_nothing() {
        let store = MemoryStore::new();
        let log = writer_with_flushes(&store).await;
        run(
            Arc::new(store.clone()),
            &env(),
            &TenantId::DEFAULT,
            &dataset(),
            log.writer_id(),
        )
        .await
        .unwrap();
        let again = run(
            Arc::new(store.clone()),
            &env(),
            &TenantId::DEFAULT,
            &dataset(),
            log.writer_id(),
        )
        .await
        .unwrap();
        assert_eq!(again.adoptions, 0);
        assert_eq!(again.flushes, 1);
    }

    /// A run that resumes where the one before stopped reads what the
    /// writer appended since and nothing else: the work of a round
    /// follows what is new, not what the log holds.
    #[tokio::test]
    async fn a_resumed_run_reads_only_what_came_after() {
        let store = MemoryStore::new();
        let mut log = writer_with_flushes(&store).await;
        let writer = log.writer_id();
        let resume = async |after: Seq| {
            run_after(
                Arc::new(store.clone()),
                &env(),
                &TenantId::DEFAULT,
                &dataset(),
                writer,
                after,
            )
            .await
            .unwrap()
        };
        let first = resume(Seq::ZERO).await;
        assert_eq!((first.flushes, first.through), (1, Seq::new(4)));
        let idle = resume(first.through).await;
        assert_eq!((idle.flushes, idle.through), (0, Seq::new(4)));

        let target = FlushTarget {
            store: Arc::new(store.clone()),
            pool: PoolId::new("root").unwrap(),
            format: Arc::new(ParquetFormat::default()),
        };
        log.append_segment(&[batch("prod", 2, 1)]).await.unwrap();
        let files = write_l1_files(
            &target,
            &env(),
            log.writer_id(),
            Seq::new(4),
            Seq::new(5),
            WINDOW,
            vec![batch("prod", 2, 1)],
        )
        .await
        .unwrap();
        log.append(EntryBody::Flush {
            tenant: TenantId::DEFAULT,
            dataset: dataset(),
            after: Seq::new(4),
            through: Seq::new(5),
            files,
        })
        .await
        .unwrap();
        let later = resume(first.through).await;
        assert_eq!(
            (later.flushes, later.adoptions, later.through),
            (1, 1, Seq::new(6))
        );
        let partitions = list_partitions(&store, &TenantId::DEFAULT, &dataset(), Kind::Log)
            .await
            .unwrap();
        assert_eq!(partitions.len(), 3);
    }

    #[tokio::test]
    async fn an_ended_log_leaves_the_registry_once_adopted() {
        let store = MemoryStore::new();
        let mut log = writer_with_flushes(&store).await;
        log.end().await.unwrap();
        let outcome = run(
            Arc::new(store.clone()),
            &env(),
            &TenantId::DEFAULT,
            &dataset(),
            log.writer_id(),
        )
        .await
        .unwrap();
        assert!(outcome.ended && outcome.removed, "{outcome:?}");
        assert!(
            registry::list(&store, &TenantId::DEFAULT, &dataset())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_writer_without_flushes_stays_registered() {
        let store = MemoryStore::new();
        let mut log = WriterLog::start(Arc::new(store.clone()), env());
        registry::register(
            &store,
            &env(),
            &TenantId::DEFAULT,
            &dataset(),
            log.writer_id(),
        )
        .await
        .unwrap();
        log.append_segment(&[batch("prod", 0, 1)]).await.unwrap();
        let outcome = run(
            Arc::new(store.clone()),
            &env(),
            &TenantId::DEFAULT,
            &dataset(),
            log.writer_id(),
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            IntakeOutcome {
                through: Seq::new(1),
                ..IntakeOutcome::default()
            }
        );
        assert_eq!(
            registry::list(&store, &TenantId::DEFAULT, &dataset())
                .await
                .unwrap()
                .len(),
            1
        );
    }
}
