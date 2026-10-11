// SPDX-License-Identifier: LicenseRef-Tessera-Pending

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
}

/// Runs intake for `writer` and one dataset.
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
    let mut outcome = IntakeOutcome::default();
    let mut entries = reader::entries(Arc::clone(&store), writer, Seq::ZERO, Arc::clone(&env.keys));
    while let Some(entry) = entries.try_next().await? {
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
            }
            EntryBody::End => outcome.ended = true,
            EntryBody::Flush { .. } | EntryBody::Segment { .. } | EntryBody::Seal { .. } => {}
        }
    }
    if outcome.ended {
        registry::remove(&*store, tenant, dataset, writer).await?;
        outcome.removed = true;
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
                removed: false
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
        assert_eq!(outcome, IntakeOutcome::default());
        assert_eq!(
            registry::list(&store, &TenantId::DEFAULT, &dataset())
                .await
                .unwrap()
                .len(),
            1
        );
    }
}
