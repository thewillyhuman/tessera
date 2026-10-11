// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The ingester: buffers batches, writes segments to its own writer log,
//! acknowledges, keeps rows in a memtable until they are flushed to L1
//! files (section 6.9), and ends its log on shutdown.
//!
//! The ingester is driven explicitly: `write` buffers, `tick` does the
//! time-based work. That keeps it deterministic under simulation; the
//! node runtime ticks it from a timer.
//!
//! A writer log is replaced by a fresh one when it reaches its age or
//! entry limit (ST-8), and when recovery fences it (ST-9): a fenced log
//! is left to recovery, whose flush covers every row the log holds, and
//! the ingester carries on with a new writer id. Rows that were pending
//! are retried on the new log; nothing acknowledged is lost either way.
//!
//! An append whose outcome is unknown is settled before the next one: a
//! segment that turns out to have landed joins the memtable so that it is
//! flushed (its rows are durable, which is at-least-once delivery for a
//! caller that was told otherwise); a flush record that landed clears the
//! rows it covers, so that no two records cover the same rows.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use tessera_format::FileFormat;
use tessera_manifest::registry;
use tessera_object::ObjectStore;
use tessera_types::{DatasetName, Env, EventBatch, PoolId, Seq, TenantId, Timestamp, Window};
use tessera_wal::{EntryBody, FlushTarget, WalError, WriterLog, write_l1_files};
use tokio::sync::oneshot;

/// Tunables of section 6.3 and 6.9.
#[derive(Clone, Copy, Debug)]
pub struct IngestConfig {
    /// How long to wait for more rows before writing a segment (ST-6).
    pub linger: Duration,
    /// Buffered bytes that trigger a segment write at once (ST-6).
    pub segment_bytes: usize,
    /// A dataset's memtable size that triggers an L1 flush.
    pub flush_bytes: usize,
    /// The age of a dataset's oldest unflushed row that triggers a flush.
    pub flush_age: Duration,
    /// The partition window length (MF-1).
    pub window: Duration,
    /// How long a writer log is used before a new one is started (ST-8).
    pub max_log_age: Duration,
    /// How many entries a writer log holds before a new one is started
    /// (ST-8).
    pub max_log_entries: u64,
    /// How many writer logs the node appends to, each from a task of
    /// its own (ST-5); writes go to them in turn, so ingest takes as
    /// many cores. One by default.
    pub writers: usize,
}

impl Default for IngestConfig {
    fn default() -> Self {
        IngestConfig {
            linger: Duration::from_millis(50),
            segment_bytes: 8 << 20,
            flush_bytes: 64 << 20,
            flush_age: Duration::from_secs(120),
            window: Window::DEFAULT_LENGTH,
            max_log_age: Duration::from_secs(3600),
            max_log_entries: 1_000_000,
            writers: 1,
        }
    }
}

/// Why a write or a flush failed.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// Recovery sealed this writer's log; the ingester must stop (ST-9).
    #[error("this ingester is fenced and accepts no more writes")]
    Fenced,
    /// The ingester was shut down.
    #[error("this ingester is shut down")]
    Stopped,
    /// The segment could not be stored; the client may retry (IN-11).
    #[error("segment not stored: {0}")]
    NotStored(String),
    /// The acknowledgement was lost because the ingester went away.
    #[error("acknowledgement lost")]
    Lost,
    /// The writer log failed.
    #[error(transparent)]
    Wal(#[from] WalError),
    /// The registry failed.
    #[error(transparent)]
    Registry(#[from] tessera_manifest::ManifestError),
}

/// A pending acknowledgement: resolves to the segment position once the
/// rows are durable (IN-10).
pub struct Ack(oneshot::Receiver<Result<Seq, IngestError>>);

impl Ack {
    /// Waits for the rows to be durable.
    ///
    /// # Errors
    ///
    /// `Lost` if the ingester went away first, or what the segment write
    /// reported.
    pub async fn wait(self) -> Result<Seq, IngestError> {
        self.0.await.unwrap_or(Err(IngestError::Lost))
    }

    /// The result, if it is already known.
    #[must_use]
    pub fn try_now(&mut self) -> Option<Result<Seq, IngestError>> {
        match self.0.try_recv() {
            Ok(result) => Some(result),
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => Some(Err(IngestError::Lost)),
        }
    }
}

/// A dataset's rows that are durable in segments but not yet in files.
#[derive(Default)]
struct Memtable {
    rows: Vec<(Seq, EventBatch)>,
    bytes: usize,
    oldest: Option<Timestamp>,
    flushed_through: Seq,
}

/// What the last append attempted, when its outcome is unknown.
enum Doubt {
    Segment(Vec<EventBatch>),
    Flush {
        key: (TenantId, DatasetName),
        through: Seq,
    },
}

/// One ingester process's state.
pub struct Ingester {
    store: Arc<dyn ObjectStore>,
    env: Env,
    config: IngestConfig,
    log: WriterLog,
    target: FlushTarget,
    registered: BTreeSet<(TenantId, DatasetName)>,
    pending: Vec<(EventBatch, oneshot::Sender<Result<Seq, IngestError>>)>,
    pending_bytes: usize,
    pending_since: Option<Timestamp>,
    memtables: BTreeMap<(TenantId, DatasetName), Memtable>,
    log_started: Timestamp,
    in_doubt: Option<Doubt>,
    stopped: bool,
}

impl Ingester {
    /// Starts an ingester with a fresh writer log, storing files in `pool`.
    #[must_use]
    pub fn start(
        store: Arc<dyn ObjectStore>,
        env: Env,
        config: IngestConfig,
        pool: PoolId,
        format: Arc<dyn FileFormat>,
    ) -> Self {
        let log = WriterLog::start(Arc::clone(&store), env.clone());
        let target = FlushTarget {
            store: Arc::clone(&store),
            pool,
            format,
        };
        let log_started = env.clock.now();
        Ingester {
            store,
            env,
            config,
            log,
            target,
            registered: BTreeSet::new(),
            pending: Vec::new(),
            pending_bytes: 0,
            pending_since: None,
            memtables: BTreeMap::new(),
            log_started,
            in_doubt: None,
            stopped: false,
        }
    }

    /// This ingester's writer id.
    #[must_use]
    pub fn writer_id(&self) -> tessera_types::WriterId {
        self.log.writer_id()
    }

    /// Whether the ingester can still accept writes.
    #[must_use]
    pub fn is_open(&self) -> bool {
        !self.stopped
    }

    /// Buffers a batch. The returned [`Ack`] resolves once the segment
    /// holding the batch is stored, which happens at once if the buffer is
    /// full and otherwise on a later [`tick`](Self::tick).
    pub async fn write(&mut self, batch: EventBatch) -> Ack {
        let (sender, receiver) = oneshot::channel();
        if self.stopped {
            let _ = sender.send(Err(IngestError::Stopped));
            return Ack(receiver);
        }
        self.pending_bytes += batch.rows().get_array_memory_size();
        self.pending_since
            .get_or_insert_with(|| self.env.clock.now());
        self.pending.push((batch, sender));
        if self.pending_bytes >= self.config.segment_bytes {
            self.write_segment().await;
        }
        Ack(receiver)
    }

    /// Does the time-based work: writes the pending segment once the
    /// linger has elapsed and flushes datasets whose memtable is large or
    /// old enough.
    pub async fn tick(&mut self) {
        if let Err(error) = self.settle().await {
            tracing::debug!(error = %error, "previous append still in doubt");
        }
        let now = self.env.clock.now();
        let linger_elapsed = self.pending_since.is_some_and(|since| {
            now.duration_since(since)
                .is_some_and(|waited| waited >= self.config.linger)
        });
        if linger_elapsed {
            self.write_segment().await;
        }
        let aged = now
            .duration_since(self.log_started)
            .is_some_and(|age| age >= self.config.max_log_age);
        let full = self.log.next_seq().get() > self.config.max_log_entries;
        if aged || full {
            if let Err(error) = self.rotate().await {
                tracing::warn!(error = %error, "writer log rotation failed; will retry");
            }
        }
        let due: Vec<(TenantId, DatasetName)> = self
            .memtables
            .iter()
            .filter(|(_, m)| !m.rows.is_empty())
            .filter(|(_, m)| {
                m.bytes >= self.config.flush_bytes
                    || m.oldest.is_some_and(|oldest| {
                        now.duration_since(oldest)
                            .is_some_and(|age| age >= self.config.flush_age)
                    })
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in due {
            if let Err(error) = self.flush(&key).await {
                tracing::warn!(tenant = %key.0, dataset = %key.1, error = %error, "l1 flush failed; will retry");
            }
        }
    }

    /// Writes what is pending, flushes every memtable and ends the log
    /// (ST-8). Afterwards every write is refused.
    ///
    /// # Errors
    ///
    /// The first flush or log failure; the ingester is stopped either way.
    pub async fn shutdown(&mut self) -> Result<(), IngestError> {
        self.stopped = true;
        self.write_segment().await;
        let keys: Vec<_> = self.memtables.keys().cloned().collect();
        let mut first_error = None;
        for key in keys {
            if let Err(error) = self.flush(&key).await {
                first_error.get_or_insert(error);
            }
        }
        if !self.log.is_fenced() {
            self.log.end().await?;
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Writes the pending batches as one segment and resolves their acks.
    async fn write_segment(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let pending = std::mem::take(&mut self.pending);
        self.pending_bytes = 0;
        self.pending_since = None;
        let batches: Vec<EventBatch> = pending.iter().map(|(b, _)| b.clone()).collect();
        let mut result = match self.settle().await {
            Ok(()) => self.append(&batches).await,
            Err(error) => Err(error),
        };
        if matches!(result, Err(IngestError::Fenced)) {
            tracing::warn!(writer = %self.log.writer_id(), "writer log fenced by recovery; starting a new one");
            self.replace_log();
            result = self.append(&batches).await;
        }
        if matches!(result, Err(IngestError::NotStored(_))) && self.log.has_doubt() {
            self.in_doubt = Some(Doubt::Segment(batches));
        }
        match result {
            Ok(seq) => {
                for (batch, sender) in pending {
                    self.remember(seq, batch);
                    let _ = sender.send(Ok(seq));
                }
            }
            Err(error) => {
                let message = error.to_string();
                tracing::warn!(error = %message, "segment write failed; rejecting its writes");
                for (_, sender) in pending {
                    let _ = sender.send(Err(match error {
                        IngestError::Fenced => IngestError::Fenced,
                        _ => IngestError::NotStored(message.clone()),
                    }));
                }
            }
        }
    }

    /// Registers and appends one segment.
    async fn append(&mut self, batches: &[EventBatch]) -> Result<Seq, IngestError> {
        self.register_all(batches)
            .await
            .map_err(|e| IngestError::NotStored(e.to_string()))?;
        self.log.append_segment(batches).await.map_err(|e| match e {
            WalError::Fenced { .. } => IngestError::Fenced,
            other => IngestError::NotStored(other.to_string()),
        })
    }

    /// Starts a fresh writer log. The old log's unflushed rows are left to
    /// recovery, which is what fenced the old log in the first place.
    fn replace_log(&mut self) {
        self.log = WriterLog::start(Arc::clone(&self.store), self.env.clone());
        self.log_started = self.env.clock.now();
        self.registered.clear();
        self.memtables.clear();
        self.in_doubt = None;
    }

    /// Settles the last append if its outcome was unknown, and applies what
    /// it turns out to have done.
    async fn settle(&mut self) -> Result<(), IngestError> {
        let Some(doubt) = self.in_doubt.take() else {
            return Ok(());
        };
        match self.log.settle().await {
            Ok(Some(seq)) => match doubt {
                Doubt::Segment(batches) => {
                    tracing::info!(%seq, "a segment thought lost had landed; its rows will be flushed");
                    for batch in batches {
                        self.remember(seq, batch);
                    }
                }
                Doubt::Flush { key, through } => self.mark_flushed(&key, through),
            },
            // Nothing landed, or a seal sits at the position and the next
            // append rotates the log.
            Ok(None) | Err(WalError::Fenced { .. }) => {}
            Err(error) => {
                self.in_doubt = Some(doubt);
                return Err(IngestError::NotStored(error.to_string()));
            }
        }
        Ok(())
    }

    /// Drops a dataset's rows up to `through`: a flush record covering them
    /// is in the log.
    fn mark_flushed(&mut self, key: &(TenantId, DatasetName), through: Seq) {
        let Some(memtable) = self.memtables.get_mut(key) else {
            return;
        };
        memtable.rows.retain(|(seq, _)| *seq > through);
        memtable.bytes = memtable
            .rows
            .iter()
            .map(|(_, b)| b.rows().get_array_memory_size())
            .sum();
        if memtable.rows.is_empty() {
            memtable.oldest = None;
        }
        memtable.flushed_through = memtable.flushed_through.max(through);
    }

    /// Ends the current log cleanly, with everything flushed, and starts
    /// a new one (ST-8).
    async fn rotate(&mut self) -> Result<(), IngestError> {
        let keys: Vec<_> = self.memtables.keys().cloned().collect();
        for key in keys {
            self.flush(&key).await?;
        }
        if !self.log.is_fenced() {
            self.log.end().await?;
        }
        tracing::info!(writer = %self.log.writer_id(), "writer log rotated");
        self.replace_log();
        Ok(())
    }

    /// Registers this writer with every dataset in `batches` it has not
    /// registered with yet, before any of their rows is acknowledged (IN-6).
    async fn register_all(&mut self, batches: &[EventBatch]) -> Result<(), IngestError> {
        for batch in batches {
            let key = (batch.tenant().clone(), batch.dataset().clone());
            if self.registered.contains(&key) {
                continue;
            }
            registry::register(
                &*self.store,
                &self.env,
                &key.0,
                &key.1,
                self.log.writer_id(),
            )
            .await?;
            self.registered.insert(key);
        }
        Ok(())
    }

    fn remember(&mut self, seq: Seq, batch: EventBatch) {
        let now = self.env.clock.now();
        let memtable = self
            .memtables
            .entry((batch.tenant().clone(), batch.dataset().clone()))
            .or_default();
        memtable.bytes += batch.rows().get_array_memory_size();
        memtable.oldest.get_or_insert(now);
        memtable.rows.push((seq, batch));
    }

    /// Flushes one dataset's memtable to L1 files and appends the flush
    /// record. On failure the rows stay for the next attempt.
    async fn flush(&mut self, key: &(TenantId, DatasetName)) -> Result<(), IngestError> {
        self.settle().await?;
        let Some(memtable) = self.memtables.get(key) else {
            return Ok(());
        };
        if memtable.rows.is_empty() {
            return Ok(());
        }
        let after = memtable.flushed_through;
        let through = memtable
            .rows
            .iter()
            .map(|(seq, _)| *seq)
            .max()
            .unwrap_or(after);
        let batches: Vec<EventBatch> = memtable.rows.iter().map(|(_, b)| b.clone()).collect();
        let files = write_l1_files(
            &self.target,
            &self.env,
            self.log.writer_id(),
            after,
            through,
            self.config.window,
            batches,
        )
        .await?;
        let record = EntryBody::Flush {
            tenant: key.0.clone(),
            dataset: key.1.clone(),
            after,
            through,
            files,
        };
        if let Err(error) = self.log.append(record).await {
            if self.log.has_doubt() {
                self.in_doubt = Some(Doubt::Flush {
                    key: key.clone(),
                    through,
                });
            }
            return Err(error.into());
        }
        self.mark_flushed(key, through);
        Ok(())
    }
}

impl std::fmt::Debug for Ingester {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ingester")
            .field("writer", &self.log.writer_id())
            .field("pending", &self.pending.len())
            .field("memtables", &self.memtables.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, FixedSizeBinaryArray};
    use futures::TryStreamExt;
    use tessera_format::ParquetFormat;
    use tessera_manifest::{intake, view};
    use tessera_object::{FaultPolicy, FaultyStore, LostResponseStore, MemoryStore, Pools, layout};
    use tessera_types::event::EVENT_ID;
    use tessera_types::{
        Clock, Keys, Kind, LogRow, ManualClock, NodeId, SeededRandom, SharedRandom,
    };
    use tessera_wal::{Entry, EntryId, reader};

    use super::*;

    struct Harness {
        store: MemoryStore,
        clock: Arc<ManualClock>,
        env: Env,
    }

    impl Harness {
        fn new() -> Self {
            let clock = Arc::new(ManualClock::new(Timestamp::from_nanos(1_000_000_000_000)));
            let env = Env::new(
                NodeId::from_u128(1),
                Arc::clone(&clock) as Arc<dyn Clock>,
                SharedRandom::new(SeededRandom::new(1)),
            );
            Harness {
                store: MemoryStore::new(),
                clock,
                env,
            }
        }

        fn ingester(&self, config: IngestConfig) -> Ingester {
            self.ingester_on(Arc::new(self.store.clone()), config)
        }

        fn ingester_on(&self, store: Arc<dyn ObjectStore>, config: IngestConfig) -> Ingester {
            Ingester::start(
                store,
                self.env.clone(),
                config,
                PoolId::new("root").unwrap(),
                Arc::new(ParquetFormat::default()),
            )
        }

        async fn view_ids(&self, dataset: &str) -> Vec<u8> {
            let pools = Pools::new(PoolId::new("root").unwrap(), Arc::new(self.store.clone()));
            let format: Arc<dyn FileFormat> = Arc::new(ParquetFormat::default());
            let view = view::read(
                &pools,
                &format,
                &self.env,
                &TenantId::DEFAULT,
                &DatasetName::new(dataset).unwrap(),
                Kind::Log,
                Window::DEFAULT_LENGTH,
            )
            .await
            .unwrap();
            let mut ids: Vec<u8> = view
                .rows
                .iter()
                .flat_map(|r| {
                    let column = r.column_by_name(EVENT_ID).unwrap();
                    let column = column
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .unwrap();
                    (0..column.len())
                        .map(|i| column.value(i)[0])
                        .collect::<Vec<_>>()
                })
                .collect();
            ids.sort_unstable();
            ids
        }

        async fn entry_kinds(&self, ingester: &Ingester) -> Vec<&'static str> {
            let entries: Vec<Entry> = reader::entries(
                Arc::new(self.store.clone()),
                ingester.writer_id(),
                Seq::ZERO,
                Arc::new(Keys::none()),
            )
            .try_collect()
            .await
            .unwrap();
            entries
                .iter()
                .map(|e| match e.body() {
                    EntryBody::Segment { .. } => "segment",
                    EntryBody::Flush { .. } => "flush",
                    EntryBody::Seal { .. } => "seal",
                    EntryBody::End => "end",
                })
                .collect()
        }
    }

    fn config() -> IngestConfig {
        IngestConfig {
            linger: Duration::from_millis(250),
            segment_bytes: 1 << 30,
            flush_bytes: 1 << 30,
            flush_age: Duration::from_secs(120),
            window: Window::DEFAULT_LENGTH,
            max_log_age: Duration::from_secs(3600),
            max_log_entries: 1_000_000,
            writers: 1,
        }
    }

    fn batch(dataset: &str, ids: std::ops::Range<u8>) -> EventBatch {
        let rows: Vec<LogRow> = ids
            .map(|i| LogRow {
                timestamp: Timestamp::from_nanos(1_000_000_000_000 + i64::from(i)),
                event_id: [i; 16],
                body: Some("x".into()),
                ..LogRow::default()
            })
            .collect();
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new(dataset).unwrap(), &rows).unwrap()
    }

    #[tokio::test]
    async fn acknowledges_once_the_segment_is_stored() {
        let h = Harness::new();
        let mut ingester = h.ingester(config());
        let mut ack = ingester.write(batch("prod", 0..2)).await;
        assert!(ack.try_now().is_none(), "nothing is durable yet");
        ingester.tick().await;
        assert!(ack.try_now().is_none(), "the linger has not elapsed");
        h.clock.advance(Duration::from_millis(250));
        ingester.tick().await;
        assert_eq!(ack.wait().await.unwrap(), Seq::new(1));
        assert_eq!(h.entry_kinds(&ingester).await, ["segment"]);
        let writers = registry::list(
            &h.store,
            &TenantId::DEFAULT,
            &DatasetName::new("prod").unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            writers.len(),
            1,
            "registered before the first acknowledgement"
        );
        assert_eq!(h.view_ids("prod").await, [0, 1]);
    }

    /// SE-2: with a key named for the tenant, the segments an ingester
    /// writes hold sealed blocks: readable with the keyring, not
    /// without, and the view over them reads the rows.
    #[tokio::test]
    async fn segments_of_a_keyed_tenant_are_sealed_at_rest() {
        use tessera_types::{KeyPolicy, Keyring, TenantKey};

        struct Everyone;

        impl KeyPolicy for Everyone {
            fn key_name(&self, _: &TenantId) -> Option<String> {
                Some("k".into())
            }
        }

        let keys = || {
            Keys::new(
                Keyring::default().with("k", TenantKey::from_bytes([3; 32])),
                Arc::new(Everyone),
            )
        };
        let mut h = Harness::new();
        h.env = h.env.clone().with_keys(keys());
        let mut ingester = h.ingester(IngestConfig {
            segment_bytes: 1,
            ..config()
        });
        let mut ack = ingester.write(batch("prod", 0..2)).await;
        assert_eq!(ack.try_now().unwrap().unwrap(), Seq::new(1));
        let stored = reader::read(&h.store, ingester.writer_id(), Seq::new(1))
            .await
            .unwrap();
        assert_eq!(stored.block_metas()[0].key.as_deref(), Some("k"));
        assert!(
            matches!(stored.block(0), Err(WalError::Sealed { .. })),
            "sealed at rest"
        );
        assert!(stored.clone().unlock(&Keys::none()).is_err());
        let opened: Vec<Entry> = reader::entries(
            Arc::new(h.store.clone()),
            ingester.writer_id(),
            Seq::ZERO,
            Arc::new(keys()),
        )
        .try_collect()
        .await
        .unwrap();
        assert_eq!(opened[0].block(0).unwrap().len(), 2);
        assert_eq!(h.view_ids("prod").await, [0, 1], "the view reads the tail");
    }

    #[tokio::test]
    async fn a_full_buffer_is_written_at_once() {
        let h = Harness::new();
        let mut ingester = h.ingester(IngestConfig {
            segment_bytes: 1,
            ..config()
        });
        let mut ack = ingester.write(batch("prod", 0..1)).await;
        assert_eq!(ack.try_now().unwrap().unwrap(), Seq::new(1));
    }

    #[tokio::test]
    async fn flushes_a_dataset_once_its_rows_are_old_enough() {
        let h = Harness::new();
        let mut ingester = h.ingester(IngestConfig {
            segment_bytes: 1,
            ..config()
        });
        ingester
            .write(batch("prod", 0..3))
            .await
            .wait()
            .await
            .unwrap();
        ingester
            .write(batch("staging", 10..11))
            .await
            .wait()
            .await
            .unwrap();
        h.clock.advance(Duration::from_secs(60));
        ingester
            .write(batch("prod", 3..5))
            .await
            .wait()
            .await
            .unwrap();
        h.clock.advance(Duration::from_secs(60));
        ingester.tick().await;
        assert_eq!(
            h.entry_kinds(&ingester).await,
            ["segment", "segment", "segment", "flush", "flush"]
        );
        let entries: Vec<Entry> = reader::entries(
            Arc::new(h.store.clone()),
            ingester.writer_id(),
            Seq::new(3),
            Arc::new(Keys::none()),
        )
        .try_collect()
        .await
        .unwrap();
        let EntryBody::Flush {
            dataset,
            after,
            through,
            files,
            ..
        } = entries[0].body()
        else {
            panic!()
        };
        assert_eq!(
            (dataset.as_str(), after.get(), through.get(), files.len()),
            ("prod", 0, 3, 1)
        );
        assert_eq!(files[0].row_count, 5);
        assert_eq!(h.view_ids("prod").await, [0, 1, 2, 3, 4]);
        intake::run(
            Arc::new(h.store.clone()),
            &h.env,
            &TenantId::DEFAULT,
            &DatasetName::new("prod").unwrap(),
            ingester.writer_id(),
        )
        .await
        .unwrap();
        assert_eq!(
            h.view_ids("prod").await,
            [0, 1, 2, 3, 4],
            "no duplicates once adopted"
        );
        ingester
            .write(batch("prod", 5..6))
            .await
            .wait()
            .await
            .unwrap();
        h.clock.advance(Duration::from_secs(120));
        ingester.tick().await;
        let entries: Vec<Entry> = reader::entries(
            Arc::new(h.store.clone()),
            ingester.writer_id(),
            Seq::new(6),
            Arc::new(Keys::none()),
        )
        .try_collect()
        .await
        .unwrap();
        let EntryBody::Flush { after, through, .. } = entries[0].body() else {
            panic!()
        };
        assert_eq!(
            (after.get(), through.get()),
            (3, 6),
            "the next flush starts where the last ended"
        );
    }

    #[tokio::test]
    async fn shutdown_flushes_everything_and_ends_the_log() {
        let h = Harness::new();
        let mut ingester = h.ingester(config());
        let mut ack = ingester.write(batch("prod", 0..2)).await;
        ingester.shutdown().await.unwrap();
        assert_eq!(ack.try_now().unwrap().unwrap(), Seq::new(1));
        assert_eq!(h.entry_kinds(&ingester).await, ["segment", "flush", "end"]);
        assert!(!ingester.is_open());
        let mut refused = ingester.write(batch("prod", 2..3)).await;
        assert!(matches!(refused.try_now(), Some(Err(IngestError::Stopped))));
        assert_eq!(h.view_ids("prod").await, [0, 1]);
    }

    #[tokio::test]
    async fn a_fenced_ingester_starts_a_new_log_and_loses_nothing() {
        let h = Harness::new();
        let mut ingester = h.ingester(IngestConfig {
            segment_bytes: 1,
            ..config()
        });
        ingester
            .write(batch("prod", 0..1))
            .await
            .wait()
            .await
            .unwrap();
        let first = ingester.writer_id();
        let seal = Entry::new(
            first,
            Seq::new(2),
            EntryId::random(&mut SeededRandom::new(9)),
            Timestamp::UNIX_EPOCH,
            EntryBody::Seal {
                by: NodeId::from_u128(7),
            },
        );
        h.store
            .put(&layout::writer_log_entry(first, Seq::new(2)), seal.encode())
            .await
            .unwrap();
        let mut ack = ingester.write(batch("prod", 1..2)).await;
        assert!(
            matches!(ack.try_now(), Some(Ok(seq)) if seq == Seq::new(1)),
            "first segment of the new log"
        );
        assert_ne!(ingester.writer_id(), first);
        assert!(ingester.is_open());
        assert_eq!(
            h.view_ids("prod").await,
            [0, 1],
            "the old log is still registered and readable"
        );
    }

    #[tokio::test]
    async fn an_aged_log_is_ended_and_replaced() {
        let h = Harness::new();
        let mut ingester = h.ingester(IngestConfig {
            segment_bytes: 1,
            max_log_age: Duration::from_secs(60),
            ..config()
        });
        ingester
            .write(batch("prod", 0..1))
            .await
            .wait()
            .await
            .unwrap();
        let first = ingester.writer_id();
        h.clock.advance(Duration::from_secs(60));
        ingester.tick().await;
        assert_ne!(ingester.writer_id(), first);
        let entries: Vec<Entry> = reader::entries(
            Arc::new(h.store.clone()),
            first,
            Seq::ZERO,
            Arc::new(Keys::none()),
        )
        .try_collect()
        .await
        .unwrap();
        let kinds: Vec<&str> = entries
            .iter()
            .map(|e| match e.body() {
                EntryBody::Segment { .. } => "segment",
                EntryBody::Flush { .. } => "flush",
                EntryBody::Seal { .. } => "seal",
                EntryBody::End => "end",
            })
            .collect();
        assert_eq!(kinds, ["segment", "flush", "end"]);
        ingester
            .write(batch("prod", 1..2))
            .await
            .wait()
            .await
            .unwrap();
        assert_eq!(h.view_ids("prod").await, [0, 1]);
    }

    #[tokio::test(start_paused = true)]
    async fn appends_in_doubt_are_settled_and_never_repeated() {
        let h = Harness::new();
        let lying = Arc::new(LostResponseStore::new(
            Arc::new(h.store.clone()),
            tessera_object::Key::new("_wal").unwrap(),
        ));
        let mut ingester = h.ingester_on(
            Arc::clone(&lying) as Arc<dyn ObjectStore>,
            IngestConfig {
                segment_bytes: 1,
                ..config()
            },
        );
        // A segment that lands while the store is blind: rejected, but durable.
        lying.set_blind(true);
        let mut ack = ingester.write(batch("prod", 0..2)).await;
        assert!(matches!(
            ack.try_now(),
            Some(Err(IngestError::NotStored(_)))
        ));
        lying.set_blind(false);
        let mut ack = ingester.write(batch("prod", 2..3)).await;
        assert!(
            matches!(ack.try_now(), Some(Ok(seq)) if seq == Seq::new(2)),
            "the lost segment is counted"
        );
        assert_eq!(h.view_ids("prod").await, [0, 1, 2]);
        // A flush record that lands while the store is blind: the rows must
        // not be flushed a second time.
        h.clock.advance(Duration::from_secs(120));
        lying.set_blind(true);
        ingester.tick().await;
        lying.set_blind(false);
        h.clock.advance(Duration::from_secs(120));
        ingester.tick().await;
        assert_eq!(
            h.entry_kinds(&ingester).await,
            ["segment", "segment", "flush"]
        );
        intake::run(
            Arc::new(h.store.clone()),
            &h.env,
            &TenantId::DEFAULT,
            &DatasetName::new("prod").unwrap(),
            ingester.writer_id(),
        )
        .await
        .unwrap();
        assert_eq!(
            h.view_ids("prod").await,
            [0, 1, 2],
            "every row exactly once"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_store_failure_rejects_the_writes_and_the_ingester_recovers() {
        let h = Harness::new();
        let faulty = Arc::new(FaultyStore::new(
            Arc::new(h.store.clone()),
            h.env.random.clone(),
            FaultPolicy {
                fail_before: 1.0,
                fail_after: 0.0,
            },
        ));
        let mut ingester = h.ingester_on(
            Arc::clone(&faulty) as Arc<dyn ObjectStore>,
            IngestConfig {
                segment_bytes: 1,
                ..config()
            },
        );
        let mut ack = ingester.write(batch("prod", 0..1)).await;
        assert!(matches!(
            ack.try_now(),
            Some(Err(IngestError::NotStored(_)))
        ));
        faulty.set_policy(FaultPolicy::NONE);
        let mut ack = ingester.write(batch("prod", 0..1)).await;
        assert!(matches!(ack.try_now(), Some(Ok(_))));
        assert_eq!(h.view_ids("prod").await, [0]);
    }
}
