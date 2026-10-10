// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The writer side of a log: one ingester process appending entries at
//! strictly increasing sequence numbers (ST-5).
//!
//! Appending is the only write on the acknowledgment path. Each append is
//! a create-if-absent of `_wal/{writer}/{seq}`: if the object exists and
//! is not ours, recovery has sealed the log (ST-9) and the writer must
//! stop; if the response is lost, the object is read back and recognised
//! by its entry id.

use std::sync::Arc;
use std::time::Duration;

use tessera_object::{ObjectError, ObjectStore, layout};
use tessera_types::{Env, EventBatch, Seq, Timestamp, WriterId};

use crate::{Entry, EntryBody, EntryId, WalError};

/// How many times a lost write is retried or read back before giving up.
const ATTEMPTS: u32 = 8;
/// Backoff between attempts, doubled each time.
const BACKOFF: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    Open,
    Fenced,
    Ended,
}

/// This process's writer log.
pub struct WriterLog {
    store: Arc<dyn ObjectStore>,
    env: Env,
    writer: WriterId,
    next: Seq,
    state: State,
}

impl WriterLog {
    /// Starts a new log with a fresh random writer id (ST-5).
    #[must_use]
    pub fn start(store: Arc<dyn ObjectStore>, env: Env) -> Self {
        let writer = WriterId::random(&mut env.random.clone());
        WriterLog {
            store,
            env,
            writer,
            next: Seq::new(1),
            state: State::Open,
        }
    }

    /// A handle that appends to an existing log from `next` on. Only
    /// recovery uses this, after sealing the log (ST-10); a live writer
    /// never resumes a log, it starts a new one (ST-8).
    #[must_use]
    pub fn resume(store: Arc<dyn ObjectStore>, env: Env, writer: WriterId, next: Seq) -> Self {
        WriterLog {
            store,
            env,
            writer,
            next,
            state: State::Open,
        }
    }

    /// The log's identity.
    #[must_use]
    pub fn writer_id(&self) -> WriterId {
        self.writer
    }

    /// The sequence number the next entry will get.
    #[must_use]
    pub fn next_seq(&self) -> Seq {
        self.next
    }

    /// The last appended sequence number, if any.
    #[must_use]
    pub fn last_seq(&self) -> Option<Seq> {
        (self.next > Seq::new(1)).then(|| Seq::new(self.next.get() - 1))
    }

    /// Whether recovery has fenced this writer.
    #[must_use]
    pub fn is_fenced(&self) -> bool {
        self.state == State::Fenced
    }

    /// Appends a segment holding `batches` and returns its position.
    ///
    /// # Errors
    ///
    /// `Fenced` if recovery sealed the log; `Ended` after `end`; `Store` if
    /// the object store failed and the write did not land.
    pub async fn append_segment(&mut self, batches: &[EventBatch]) -> Result<Seq, WalError> {
        self.append_with(|writer, seq, id, now| Entry::segment(writer, seq, id, now, batches))
            .await
    }

    /// Appends a flush record (section 6.3).
    ///
    /// # Errors
    ///
    /// As for [`append_segment`](Self::append_segment).
    pub async fn append(&mut self, body: EntryBody) -> Result<Seq, WalError> {
        self.append_with(|writer, seq, id, now| Ok(Entry::new(writer, seq, id, now, body)))
            .await
    }

    /// Appends the end entry (ST-8): the log is complete and accepts no
    /// more entries.
    ///
    /// # Errors
    ///
    /// As for [`append_segment`](Self::append_segment).
    pub async fn end(&mut self) -> Result<Seq, WalError> {
        let seq = self.append(EntryBody::End).await?;
        self.state = State::Ended;
        Ok(seq)
    }

    async fn append_with(
        &mut self,
        make: impl FnOnce(WriterId, Seq, EntryId, Timestamp) -> Result<Entry, WalError>,
    ) -> Result<Seq, WalError> {
        match self.state {
            State::Open => {}
            State::Fenced => {
                return Err(WalError::Fenced {
                    writer: self.writer,
                    seq: self.next,
                });
            }
            State::Ended => {
                return Err(WalError::Ended {
                    writer: self.writer,
                });
            }
        }
        let seq = self.next;
        let id = EntryId::random(&mut self.env.random.clone());
        let entry = make(self.writer, seq, id, self.env.clock.now())?;
        let key = layout::writer_log_entry(self.writer, seq);
        let bytes = entry.encode();
        let mut backoff = BACKOFF;
        let mut last = None;
        for _ in 0..ATTEMPTS {
            match self.store.put_if_absent(&key, bytes.clone()).await {
                Ok(()) => return Ok(self.advance()),
                Err(ObjectError::AlreadyExists { .. }) => {
                    return self.resolve_existing(seq, id).await;
                }
                Err(error) if error.is_retryable() => {
                    // The write may have landed: read back before resending.
                    match self.store.get(&key).await {
                        Ok(found) => {
                            let found = Entry::decode(&found).map_err(|e| corrupt(&key, e))?;
                            return self.resolve_found(seq, id, &found);
                        }
                        Err(ObjectError::NotFound { .. }) => last = Some(error),
                        Err(other) if other.is_retryable() => last = Some(other),
                        Err(other) => return Err(WalError::Store(other)),
                    }
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
                Err(error) => return Err(WalError::Store(error)),
            }
        }
        Err(WalError::Store(last.unwrap_or_else(|| {
            ObjectError::Unavailable {
                key,
                source: "gave up appending".into(),
            }
        })))
    }

    async fn resolve_existing(&mut self, seq: Seq, id: EntryId) -> Result<Seq, WalError> {
        let key = layout::writer_log_entry(self.writer, seq);
        let found = self.store.get(&key).await?;
        let found = Entry::decode(&found).map_err(|e| corrupt(&key, e))?;
        self.resolve_found(seq, id, &found)
    }

    /// Decides what an entry found at our next position means: our own
    /// earlier write, or a fence.
    fn resolve_found(&mut self, seq: Seq, id: EntryId, found: &Entry) -> Result<Seq, WalError> {
        if found.header().id == id {
            return Ok(self.advance());
        }
        self.state = State::Fenced;
        Err(WalError::Fenced {
            writer: self.writer,
            seq,
        })
    }

    fn advance(&mut self) -> Seq {
        let seq = self.next;
        self.next = seq.next();
        seq
    }
}

impl std::fmt::Debug for WriterLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterLog")
            .field("writer", &self.writer)
            .field("next", &self.next)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

fn corrupt(key: &tessera_object::Key, source: WalError) -> WalError {
    WalError::Corrupt {
        key: key.clone(),
        source: Box::new(source),
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use tessera_object::{FaultPolicy, FaultyStore, Key, MemoryStore, ObjectMeta};
    use tessera_types::{DatasetName, LogRow, NodeId, TenantId};

    use super::*;
    use crate::reader;

    fn env(seed: u64) -> Env {
        Env::simulated(seed, Timestamp::UNIX_EPOCH)
    }

    fn batch() -> EventBatch {
        let row = LogRow {
            body: Some("x".into()),
            ..LogRow::default()
        };
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), &[row]).unwrap()
    }

    #[tokio::test]
    async fn appends_entries_at_increasing_positions() {
        let store = MemoryStore::new();
        let mut log = WriterLog::start(Arc::new(store.clone()), env(1));
        assert_eq!(log.append_segment(&[batch()]).await.unwrap(), Seq::new(1));
        assert_eq!(log.append_segment(&[batch()]).await.unwrap(), Seq::new(2));
        assert_eq!(log.end().await.unwrap(), Seq::new(3));
        assert_eq!(log.last_seq(), Some(Seq::new(3)));
        let listed = reader::list(
            &*Arc::new(store.clone()) as &dyn ObjectStore,
            log.writer_id(),
        )
        .await
        .unwrap();
        assert_eq!(listed, [Seq::new(1), Seq::new(2), Seq::new(3)]);
        let third = reader::read(&store, log.writer_id(), Seq::new(3))
            .await
            .unwrap();
        assert_eq!(third.body(), &EntryBody::End);
        assert!(matches!(
            log.append(EntryBody::End).await,
            Err(WalError::Ended { .. })
        ));
    }

    #[tokio::test]
    async fn a_seal_fences_the_writer() {
        let store = MemoryStore::new();
        let mut log = WriterLog::start(Arc::new(store.clone()), env(1));
        log.append_segment(&[batch()]).await.unwrap();
        let seal = Entry::new(
            log.writer_id(),
            Seq::new(2),
            EntryId::random(&mut tessera_types::SeededRandom::new(99)),
            Timestamp::UNIX_EPOCH,
            EntryBody::Seal {
                by: NodeId::from_u128(7),
            },
        );
        store
            .put(
                &layout::writer_log_entry(log.writer_id(), Seq::new(2)),
                seal.encode(),
            )
            .await
            .unwrap();
        let error = log.append_segment(&[batch()]).await.unwrap_err();
        assert!(
            matches!(error, WalError::Fenced { seq, .. } if seq == Seq::new(2)),
            "{error}"
        );
        assert!(log.is_fenced());
        assert!(matches!(
            log.append_segment(&[batch()]).await,
            Err(WalError::Fenced { .. })
        ));
        assert_eq!(
            reader::list(&store, log.writer_id()).await.unwrap().len(),
            2,
            "nothing was appended"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_response_is_recognised_as_our_own_entry() {
        let inner = MemoryStore::new();
        let faulty = FaultyStore::new(
            Arc::new(inner.clone()),
            env(3).random,
            FaultPolicy {
                fail_before: 0.0,
                fail_after: 1.0,
            },
        );
        let mut log = WriterLog::start(Arc::new(faulty), env(1));
        assert_eq!(log.append_segment(&[batch()]).await.unwrap(), Seq::new(1));
        assert_eq!(log.append_segment(&[batch()]).await.unwrap(), Seq::new(2));
        assert_eq!(
            reader::list(&inner, log.writer_id()).await.unwrap().len(),
            2
        );
    }

    /// A store whose writes fail without effect while reads work.
    #[derive(Debug)]
    struct ReadOnly(MemoryStore);

    #[async_trait]
    impl ObjectStore for ReadOnly {
        async fn get(&self, key: &Key) -> Result<Bytes, ObjectError> {
            self.0.get(key).await
        }
        async fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Bytes, ObjectError> {
            self.0.get_range(key, range).await
        }
        async fn head(&self, key: &Key) -> Result<ObjectMeta, ObjectError> {
            self.0.head(key).await
        }
        async fn put(&self, key: &Key, _: Bytes) -> Result<(), ObjectError> {
            Err(ObjectError::Unavailable {
                key: key.clone(),
                source: "read only".into(),
            })
        }
        async fn put_if_absent(&self, key: &Key, _: Bytes) -> Result<(), ObjectError> {
            Err(ObjectError::Unavailable {
                key: key.clone(),
                source: "read only".into(),
            })
        }
        fn list(&self, prefix: &Key) -> BoxStream<'_, Result<ObjectMeta, ObjectError>> {
            self.0.list(prefix)
        }
        async fn delete(&self, key: &Key) -> Result<(), ObjectError> {
            self.0.delete(key).await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_write_that_never_lands_is_reported_and_the_position_kept() {
        let mut log = WriterLog::start(Arc::new(ReadOnly(MemoryStore::new())), env(1));
        let error = log.append_segment(&[batch()]).await.unwrap_err();
        assert!(
            matches!(error, WalError::Store(ref e) if e.is_retryable()),
            "{error}"
        );
        assert_eq!(log.next_seq(), Seq::new(1));
        assert!(!log.is_fenced());
    }
}
