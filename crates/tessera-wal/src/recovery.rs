// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Recovery of a writer believed dead (ST-9, ST-10).
//!
//! The job creates a seal entry at the next unused position. If the create
//! succeeds the writer is fenced: its next append collides. If it fails
//! because the writer appended meanwhile, the writer is alive. After the
//! seal, the segments not yet covered by a flush record are converted
//! into L1 files, a flush record is appended per dataset, and the end
//! entry closes the log.
//!
//! Concurrent jobs are serialized by the same create-if-absent rule: a
//! job whose append collides with another job's entry stops; the other
//! finishes. A job that finds a seal already in place carries on from it,
//! so a job that died after sealing is completed by the next one.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt;
use tessera_types::{DatasetName, Env, EventBatch, Keys, Seq, TenantId, WriterId};

use crate::flush::{FlushTarget, write_l1_files};
use crate::{Entry, EntryBody, WalError, WriterLog, reader};

/// How many times the seal is retried when the writer keeps appending.
const SEAL_ATTEMPTS: u32 = 3;

/// How a recovery attempt ended.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Recovery {
    /// The log already had its end entry.
    AlreadyEnded,
    /// The writer appended while the seal was attempted: it is alive.
    WriterAlive,
    /// Another job is ahead of this one and will finish the log.
    Superseded,
    /// This job (or a previous one that sealed) completed the log.
    Recovered {
        /// The position of the seal.
        sealed_at: Seq,
        /// How many flush records this job appended.
        flushes: usize,
    },
}

/// Seals, flushes and ends the log of `writer`.
///
/// # Errors
///
/// `Store`, `Corrupt`, `Format` or `Flush` if the log cannot be read, the
/// files cannot be written or the entries cannot be appended.
pub async fn recover(
    target: &FlushTarget,
    env: &Env,
    writer: WriterId,
    window: Duration,
) -> Result<Recovery, WalError> {
    let store = &target.store;
    let sealed_at = match seal(store, env, writer).await? {
        Sealed::At(seq) => seq,
        Sealed::Ended => return Ok(Recovery::AlreadyEnded),
        Sealed::Alive => return Ok(Recovery::WriterAlive),
    };

    let pending = unflushed(Arc::clone(store), writer, sealed_at, Arc::clone(&env.keys)).await?;
    let mut log = WriterLog::resume(Arc::clone(store), env.clone(), writer, sealed_at.next());
    let mut flushes = 0;
    for ((tenant, dataset), rows) in pending {
        let files = write_l1_files(
            target,
            env,
            writer,
            rows.after,
            rows.through,
            window,
            rows.batches,
        )
        .await?;
        let record = EntryBody::Flush {
            tenant,
            dataset,
            after: rows.after,
            through: rows.through,
            files,
        };
        match log.append(record).await {
            Ok(_) => flushes += 1,
            Err(WalError::Fenced { .. }) => return Ok(Recovery::Superseded),
            Err(error) => return Err(error),
        }
    }
    match log.end().await {
        Ok(_) => Ok(Recovery::Recovered { sealed_at, flushes }),
        Err(WalError::Fenced { .. }) => Ok(Recovery::Superseded),
        Err(error) => Err(error),
    }
}

enum Sealed {
    At(Seq),
    Ended,
    Alive,
}

/// Creates the seal, or finds the one already there.
async fn seal(
    store: &Arc<dyn tessera_object::ObjectStore>,
    env: &Env,
    writer: WriterId,
) -> Result<Sealed, WalError> {
    for _ in 0..SEAL_ATTEMPTS {
        let last = reader::last(&**store, writer).await?;
        if let Some(seq) = last {
            match reader::read(&**store, writer, seq).await?.body() {
                EntryBody::End => return Ok(Sealed::Ended),
                EntryBody::Seal { .. } => return Ok(Sealed::At(seq)),
                EntryBody::Segment { .. } | EntryBody::Flush { .. } => {}
            }
        }
        let next = last.map_or(Seq::new(1), Seq::next);
        let mut log = WriterLog::resume(Arc::clone(store), env.clone(), writer, next);
        match log.append(EntryBody::Seal { by: env.node }).await {
            Ok(seq) => return Ok(Sealed::At(seq)),
            // Something landed at `next` first: a new segment (the writer
            // lives), or another job's seal, which the next round finds.
            Err(WalError::Fenced { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(Sealed::Alive)
}

/// The rows of one dataset not yet covered by a flush record.
struct Pending {
    after: Seq,
    through: Seq,
    batches: Vec<EventBatch>,
}

/// For each dataset, the segments beyond its last flush record.
async fn unflushed(
    store: Arc<dyn tessera_object::ObjectStore>,
    writer: WriterId,
    sealed_at: Seq,
    keys: Arc<Keys>,
) -> Result<BTreeMap<(TenantId, DatasetName), Pending>, WalError> {
    let mut covered: BTreeMap<(TenantId, DatasetName), Seq> = BTreeMap::new();
    let mut segments: Vec<Entry> = Vec::new();
    let mut entries = reader::entries(store, writer, Seq::ZERO, keys);
    while let Some(entry) = entries.try_next().await? {
        if entry.seq() >= sealed_at {
            break;
        }
        match entry.body() {
            EntryBody::Segment { .. } => segments.push(entry),
            EntryBody::Flush {
                tenant,
                dataset,
                through,
                ..
            } => {
                let key = (tenant.clone(), dataset.clone());
                let seen = covered.entry(key).or_insert(Seq::ZERO);
                *seen = (*seen).max(*through);
            }
            EntryBody::Seal { .. } | EntryBody::End => {}
        }
    }
    let last_segment = segments.last().map_or(Seq::ZERO, Entry::seq);
    let mut pending: BTreeMap<(TenantId, DatasetName), Pending> = BTreeMap::new();
    for segment in &segments {
        for batch in segment.all_blocks()? {
            let key = (batch.tenant().clone(), batch.dataset().clone());
            let after = covered.get(&key).copied().unwrap_or(Seq::ZERO);
            if segment.seq() <= after {
                continue;
            }
            pending
                .entry(key)
                .or_insert_with(|| Pending {
                    after,
                    through: last_segment,
                    batches: Vec::new(),
                })
                .batches
                .push(batch);
        }
    }
    Ok(pending)
}

#[cfg(test)]
mod tests {
    use tessera_format::ParquetFormat;
    use tessera_object::{MemoryStore, ObjectStore};
    use tessera_types::{DatasetName, LogRow, NodeId, PoolId, Timestamp};

    use super::*;
    use crate::EntryId;

    const WINDOW: Duration = Duration::from_secs(86_400);

    fn target(store: &MemoryStore) -> FlushTarget {
        FlushTarget {
            store: Arc::new(store.clone()),
            pool: PoolId::new("root").unwrap(),
            format: Arc::new(ParquetFormat::default()),
        }
    }

    fn env(seed: u64) -> Env {
        Env::simulated(seed, Timestamp::from_nanos(1_000_000_000))
    }

    fn batch(dataset: &str, count: i64) -> EventBatch {
        let rows: Vec<LogRow> = (0..count)
            .map(|i| LogRow {
                timestamp: Timestamp::from_nanos(i),
                body: Some("x".into()),
                ..LogRow::default()
            })
            .collect();
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new(dataset).unwrap(), &rows).unwrap()
    }

    async fn bodies(store: &MemoryStore, writer: WriterId) -> Vec<String> {
        let entries: Vec<Entry> = reader::entries(
            Arc::new(store.clone()),
            writer,
            Seq::ZERO,
            Arc::new(Keys::none()),
        )
        .try_collect()
        .await
        .unwrap();
        entries
            .iter()
            .map(|e| match e.body() {
                EntryBody::Segment { .. } => "segment".to_owned(),
                EntryBody::Flush {
                    dataset,
                    after,
                    through,
                    files,
                    ..
                } => {
                    format!(
                        "flush {dataset} ({},{}] {} files",
                        after.get(),
                        through.get(),
                        files.len()
                    )
                }
                EntryBody::Seal { .. } => "seal".to_owned(),
                EntryBody::End => "end".to_owned(),
            })
            .collect()
    }

    #[tokio::test]
    async fn recovers_unflushed_segments_and_ends_the_log() {
        let store = MemoryStore::new();
        let target = target(&store);
        let env = env(1);
        let mut writer = WriterLog::start(Arc::new(store.clone()), env.clone());
        writer
            .append_segment(&[batch("prod", 2), batch("staging", 1)])
            .await
            .unwrap();
        writer.append_segment(&[batch("prod", 3)]).await.unwrap();
        // The writer flushed prod through segment 1 before dying.
        let files = write_l1_files(
            &target,
            &env,
            writer.writer_id(),
            Seq::ZERO,
            Seq::new(1),
            WINDOW,
            vec![batch("prod", 2)],
        )
        .await
        .unwrap();
        writer
            .append(EntryBody::Flush {
                tenant: TenantId::DEFAULT,
                dataset: DatasetName::new("prod").unwrap(),
                after: Seq::ZERO,
                through: Seq::new(1),
                files,
            })
            .await
            .unwrap();
        writer.append_segment(&[batch("prod", 4)]).await.unwrap();

        let outcome = recover(
            &target,
            &env.as_node(NodeId::from_u128(9)),
            writer.writer_id(),
            WINDOW,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            Recovery::Recovered {
                sealed_at: Seq::new(5),
                flushes: 2
            }
        );
        assert_eq!(
            bodies(&store, writer.writer_id()).await,
            [
                "segment",
                "segment",
                "flush prod (0,1] 1 files",
                "segment",
                "seal",
                "flush prod (1,4] 1 files",
                "flush staging (0,4] 1 files",
                "end"
            ]
        );
        let entries: Vec<Entry> = reader::entries(
            Arc::new(store.clone()),
            writer.writer_id(),
            Seq::new(5),
            Arc::new(Keys::none()),
        )
        .try_collect()
        .await
        .unwrap();
        let EntryBody::Flush { files, .. } = entries[0].body() else {
            panic!("flush expected")
        };
        assert_eq!(files[0].row_count, 7, "prod rows of segments 2 and 4");
        let EntryBody::Flush { files, .. } = entries[1].body() else {
            panic!("flush expected")
        };
        assert_eq!(files[0].row_count, 1, "staging rows of segment 1");
        assert!(
            matches!(
                writer.append_segment(&[batch("prod", 1)]).await,
                Err(WalError::Fenced { .. })
            ),
            "the writer is fenced"
        );
    }

    #[tokio::test]
    async fn an_ended_log_needs_no_recovery() {
        let store = MemoryStore::new();
        let mut writer = WriterLog::start(Arc::new(store.clone()), env(1));
        writer.append_segment(&[batch("prod", 1)]).await.unwrap();
        writer.end().await.unwrap();
        let before = bodies(&store, writer.writer_id()).await;
        let outcome = recover(&target(&store), &env(2), writer.writer_id(), WINDOW)
            .await
            .unwrap();
        assert_eq!(outcome, Recovery::AlreadyEnded);
        assert_eq!(bodies(&store, writer.writer_id()).await, before);
    }

    #[tokio::test]
    async fn a_writer_with_no_entries_is_sealed_and_ended() {
        let store = MemoryStore::new();
        let writer = WriterId::from_u128(42);
        let outcome = recover(&target(&store), &env(2), writer, WINDOW)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            Recovery::Recovered {
                sealed_at: Seq::new(1),
                flushes: 0
            }
        );
        assert_eq!(bodies(&store, writer).await, ["seal", "end"]);
    }

    #[tokio::test]
    async fn a_job_that_finds_a_seal_completes_the_log() {
        let store = MemoryStore::new();
        let mut writer = WriterLog::start(Arc::new(store.clone()), env(1));
        writer.append_segment(&[batch("prod", 2)]).await.unwrap();
        // A previous job sealed the log and died.
        let seal = Entry::new(
            writer.writer_id(),
            Seq::new(2),
            EntryId::random(&mut tessera_types::SeededRandom::new(5)),
            Timestamp::UNIX_EPOCH,
            EntryBody::Seal {
                by: NodeId::from_u128(3),
            },
        );
        store
            .put(
                &tessera_object::layout::writer_log_entry(writer.writer_id(), Seq::new(2)),
                seal.encode(),
            )
            .await
            .unwrap();
        let outcome = recover(&target(&store), &env(2), writer.writer_id(), WINDOW)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            Recovery::Recovered {
                sealed_at: Seq::new(2),
                flushes: 1
            }
        );
        assert_eq!(
            bodies(&store, writer.writer_id()).await,
            ["segment", "seal", "flush prod (0,1] 1 files", "end"]
        );
    }

    #[tokio::test]
    async fn recovering_twice_is_harmless() {
        let store = MemoryStore::new();
        let mut writer = WriterLog::start(Arc::new(store.clone()), env(1));
        writer.append_segment(&[batch("prod", 2)]).await.unwrap();
        recover(&target(&store), &env(2), writer.writer_id(), WINDOW)
            .await
            .unwrap();
        let again = recover(&target(&store), &env(3), writer.writer_id(), WINDOW)
            .await
            .unwrap();
        assert_eq!(again, Recovery::AlreadyEnded);
    }
}
