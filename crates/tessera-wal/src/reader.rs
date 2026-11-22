// SPDX-License-Identifier: Apache-2.0

//! Reading a writer log: by intake, tail reads, continuous queries and
//! recovery.

use std::future::Future;
use std::sync::Arc;

use futures::TryStreamExt;
use futures::stream::{self, BoxStream};
use tessera_object::{Key, ObjectError, ObjectStore, layout};
use tessera_types::{Keys, Seq, WriterId};

use crate::{Entry, EntryBody, EntryHeader, WalError};

/// How many of an entry's last bytes are asked for to read its header:
/// more than the header of a segment of a hundred blocks takes, and
/// than most flush records take whole. A header that is longer says
/// so in the entry's last bytes, and is asked for again.
const HEADER_READ: u64 = 64 << 10;

/// The positions of every entry of a log, ascending.
///
/// # Errors
///
/// `Store` if the log cannot be listed.
pub async fn list(store: &dyn ObjectStore, writer: WriterId) -> Result<Vec<Seq>, WalError> {
    let mut positions = Vec::new();
    let mut listing = store.list(&layout::writer_log(writer));
    while let Some(meta) = listing.try_next().await? {
        if let Ok(seq) = meta.key.name().parse::<Seq>() {
            positions.push(seq);
        }
    }
    positions.sort_unstable();
    Ok(positions)
}

/// Every writer with at least one entry in the store, in id order:
/// registered or not, so that ended logs can be collected after their
/// writer left the registry.
///
/// # Errors
///
/// `Store` if the store cannot be listed.
pub async fn writers(store: &dyn ObjectStore) -> Result<Vec<WriterId>, WalError> {
    let prefix = tessera_object::Key::new("_wal")
        .unwrap_or_else(|e| unreachable!("a literal valid key: {e}"));
    // The names under the prefix are the writers: asked for as names,
    // they cost the same however many entries the logs hold.
    let names = store.list_names(&prefix).await?;
    let mut writers: Vec<WriterId> = names
        .iter()
        .filter_map(|name| name.parse::<WriterId>().ok())
        .collect();
    writers.sort();
    Ok(writers)
}

/// The last position of a log, if it has any entry.
///
/// # Errors
///
/// `Store` if the log cannot be listed.
pub async fn last(store: &dyn ObjectStore, writer: WriterId) -> Result<Option<Seq>, WalError> {
    Ok(list(store, writer).await?.last().copied())
}

/// One entry.
///
/// # Errors
///
/// `Store` with `NotFound` if there is no such entry; `Corrupt` if it
/// cannot be decoded.
pub async fn read(store: &dyn ObjectStore, writer: WriterId, seq: Seq) -> Result<Entry, WalError> {
    let key = layout::writer_log_entry(writer, seq);
    let bytes = store.get(&key).await?;
    Entry::decode(&bytes).map_err(|e| corrupt(&key, e))
}

/// The header of one entry: what the entry is, read without the rows
/// a segment carries, which are most of what a log holds. The files
/// of a flush record that was stored sealed are opened with `keys`
/// (SE-2), so that the header says what the writer wrote.
///
/// # Errors
///
/// `Store` with `NotFound` if there is no such entry; `Corrupt` if it
/// cannot be decoded; `Keys` if a sealed flush record does not open.
pub async fn header(
    store: &dyn ObjectStore,
    writer: WriterId,
    seq: Seq,
    keys: &Keys,
) -> Result<EntryHeader, WalError> {
    let key = layout::writer_log_entry(writer, seq);
    let mut tail = store.get_tail(&key, HEADER_READ).await?;
    // An entry shorter than what was asked for came whole.
    let whole = (tail.len() as u64) < HEADER_READ;
    let stored = EntryHeader::stored_len(&tail).map_err(|e| corrupt(&key, e))?;
    if stored > tail.len() && !whole {
        tail = store.get_tail(&key, stored as u64).await?;
    }
    let header = EntryHeader::decode(&tail).map_err(|e| corrupt(&key, e))?;
    if !matches!(header.body, EntryBody::SealedFlush { .. }) {
        return Ok(header);
    }
    // What a sealed flush record sealed, its files, lies before its
    // header. Such a record is small, and mostly came whole.
    let mut entry = if whole {
        Entry::decode(&tail).map_err(|e| corrupt(&key, e))?
    } else {
        read(store, writer, seq).await?
    };
    entry.unlock(keys)?;
    Ok(entry.header().clone())
}

fn corrupt(key: &Key, source: WalError) -> WalError {
    WalError::Corrupt {
        key: key.clone(),
        source: Box::new(source),
    }
}

/// One entry with its sealed blocks unlocked.
async fn unlocked(
    store: Arc<dyn ObjectStore>,
    writer: WriterId,
    seq: Seq,
    keys: Arc<Keys>,
) -> Result<Entry, WalError> {
    let mut entry = read(&*store, writer, seq).await?;
    entry.unlock(&keys)?;
    Ok(entry)
}

/// [`header`], for a stream that owns what it reads with.
async fn header_of(
    store: Arc<dyn ObjectStore>,
    writer: WriterId,
    seq: Seq,
    keys: Arc<Keys>,
) -> Result<EntryHeader, WalError> {
    header(&*store, writer, seq, &keys).await
}

/// The first position after `from` that the log still has. Asked of
/// the store as what lies past `from`'s key, which costs the same
/// however many entries the log keeps before it.
async fn first_after(
    store: &dyn ObjectStore,
    writer: WriterId,
    from: Seq,
) -> Result<Option<Seq>, WalError> {
    let log = layout::writer_log(writer);
    let mut later = store.list_after(&log, &layout::writer_log_entry(writer, from));
    // In key order, which is the order of the positions: the first
    // entry met is the one, and the rest is not asked for.
    while let Some(meta) = later.try_next().await? {
        if let Ok(seq) = meta.key.name().parse::<Seq>() {
            return Ok(Some(seq));
        }
    }
    Ok(None)
}

/// The entries after `from`, in order, until the first missing position,
/// their sealed blocks unlocked with `keys` (SE-2).
///
/// Garbage collection deletes a log's oldest entries (GC-2), so reading
/// starts at the first entry that still exists after `from`, found by
/// asking the store once what lies past it; from there every position
/// must be present.
pub fn entries(
    store: Arc<dyn ObjectStore>,
    writer: WriterId,
    from: Seq,
    keys: Arc<Keys>,
) -> BoxStream<'static, Result<Entry, WalError>> {
    walk(store, writer, (from, Start::Found), keys, unlocked)
}

/// The headers of the entries after `from` ([`header`]), in order,
/// found and ended as [`entries`] are: what a reader asks who wants to
/// know what a log says and not the rows it holds.
pub fn headers(
    store: Arc<dyn ObjectStore>,
    writer: WriterId,
    from: Seq,
    keys: Arc<Keys>,
) -> BoxStream<'static, Result<EntryHeader, WalError>> {
    walk(store, writer, (from, Start::Found), keys, header_of)
}

/// The entries that follow `after`, read by their positions one after
/// the other until the first that is missing, with no listing: what a
/// reader that comes back to a log every few seconds asks, a request
/// that finds nothing when nothing is new. Where the entries after
/// `after` were collected the reader finds nothing either, though the
/// log goes on further; [`entries`] finds where it goes on, and a
/// reader that only ever follows must ask it from time to time.
pub fn entries_following(
    store: Arc<dyn ObjectStore>,
    writer: WriterId,
    after: Seq,
    keys: Arc<Keys>,
) -> BoxStream<'static, Result<Entry, WalError>> {
    walk(store, writer, (after, Start::Next), keys, unlocked)
}

/// The headers of the entries that follow `after` ([`header`]), read
/// by their positions with no listing as [`entries_following`] reads
/// the entries.
pub fn headers_following(
    store: Arc<dyn ObjectStore>,
    writer: WriterId,
    after: Seq,
    keys: Arc<Keys>,
) -> BoxStream<'static, Result<EntryHeader, WalError>> {
    walk(store, writer, (after, Start::Next), keys, header_of)
}

/// Where a walk of a log begins after a position.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Start {
    /// At the first entry the log still has after it, asked of the
    /// store.
    Found,
    /// At the position that follows it.
    Next,
}

/// What `read` gives of each entry of `writer`'s log after `from`, in
/// order, from where `start` says until the first missing position.
fn walk<T, R>(
    store: Arc<dyn ObjectStore>,
    writer: WriterId,
    (from, start): (Seq, Start),
    keys: Arc<Keys>,
    read: fn(Arc<dyn ObjectStore>, WriterId, Seq, Arc<Keys>) -> R,
) -> BoxStream<'static, Result<T, WalError>>
where
    T: Send + 'static,
    R: Future<Output = Result<T, WalError>> + Send + 'static,
{
    Box::pin(stream::try_unfold((from, start), move |(seq, start)| {
        let store = Arc::clone(&store);
        let keys = Arc::clone(&keys);
        async move {
            let next = match start {
                Start::Next => seq.next(),
                Start::Found => match first_after(&*store, writer, seq).await? {
                    Some(first) => first,
                    None => return Ok(None),
                },
            };
            match read(store, writer, next, keys).await {
                Ok(found) => Ok(Some((found, (next, Start::Next)))),
                Err(WalError::Store(ObjectError::NotFound { .. })) => Ok(None),
                Err(error) => Err(error),
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tessera_object::MemoryStore;
    use tessera_types::{Env, Timestamp};

    use super::*;
    use crate::{EntryBody, WriterLog};

    #[tokio::test]
    async fn streams_entries_after_a_position_until_the_end() {
        let store = MemoryStore::new();
        let mut log = WriterLog::start(
            Arc::new(store.clone()),
            Env::simulated(1, Timestamp::UNIX_EPOCH),
        );
        for _ in 0..3 {
            log.append(EntryBody::End).await.unwrap();
        }
        let read: Vec<Seq> = entries(
            Arc::new(store.clone()),
            log.writer_id(),
            Seq::new(1),
            Arc::new(Keys::none()),
        )
        .map_ok(|e| e.seq())
        .try_collect()
        .await
        .unwrap();
        assert_eq!(read, [Seq::new(2), Seq::new(3)]);
        assert_eq!(
            last(&store, log.writer_id()).await.unwrap(),
            Some(Seq::new(3))
        );
    }

    /// A reader that resumes asks what came after its position, not
    /// for the whole log: a log keeps an hour of entries, and walking
    /// them all to find nothing new is what every poll used to cost.
    /// The entries a collection removed from the front are skipped the
    /// same way.
    #[tokio::test]
    async fn entries_after_a_position_are_found_without_walking_the_log() {
        let store = MemoryStore::new();
        let mut log = WriterLog::start(
            Arc::new(store.clone()),
            Env::simulated(1, Timestamp::UNIX_EPOCH),
        );
        for _ in 0..4 {
            log.append(EntryBody::End).await.unwrap();
        }
        let writer = log.writer_id();
        for collected in [1, 2] {
            store
                .delete(&layout::writer_log_entry(writer, Seq::new(collected)))
                .await
                .unwrap();
        }
        let shallow: Arc<dyn ObjectStore> = Arc::new(tessera_object::ShallowStore(store));
        let after = async |from: u64| {
            entries(
                Arc::clone(&shallow),
                writer,
                Seq::new(from),
                Arc::new(Keys::none()),
            )
            .map_ok(|e| e.seq())
            .try_collect::<Vec<Seq>>()
            .await
            .unwrap()
        };
        assert_eq!(after(0).await, [Seq::new(3), Seq::new(4)]);
        assert_eq!(after(3).await, [Seq::new(4)]);
        assert!(after(4).await.is_empty());
    }

    /// Following a log costs a read of the next position and no
    /// listing, and finds nothing past entries that were collected.
    #[tokio::test]
    async fn following_a_log_reads_the_next_positions_and_lists_nothing() {
        let metered = Arc::new(tessera_object::MeteredStore::new(Arc::new(
            MemoryStore::new(),
        )));
        let store: Arc<dyn ObjectStore> = metered.clone();
        let mut log =
            WriterLog::start(Arc::clone(&store), Env::simulated(1, Timestamp::UNIX_EPOCH));
        for _ in 0..4 {
            log.append(EntryBody::End).await.unwrap();
        }
        let writer = log.writer_id();
        let before = metered.requests().by_operation();
        let following = async |after: u64| {
            entries_following(
                Arc::clone(&store),
                writer,
                Seq::new(after),
                Arc::new(Keys::none()),
            )
            .map_ok(|e| e.seq())
            .try_collect::<Vec<Seq>>()
            .await
            .unwrap()
        };
        assert_eq!(following(2).await, [Seq::new(3), Seq::new(4)]);
        assert!(following(4).await.is_empty());
        let made: Vec<(&str, u64)> = metered
            .requests()
            .by_operation()
            .into_iter()
            .zip(before)
            .map(|((operation, now), (_, then))| (operation, now - then))
            .filter(|(_, made)| *made > 0)
            .collect();
        assert_eq!(made, [("get", 4)], "two entries, and the end twice");

        store
            .delete(&layout::writer_log_entry(writer, Seq::new(1)))
            .await
            .unwrap();
        assert!(
            following(0).await.is_empty(),
            "nothing past a collected entry"
        );
    }

    /// The requests `metered` counted since `before`, those made only.
    fn made_since(
        metered: &tessera_object::MeteredStore,
        before: Vec<(&'static str, u64)>,
    ) -> Vec<(&'static str, u64)> {
        metered
            .requests()
            .by_operation()
            .into_iter()
            .zip(before)
            .map(|((operation, now), (_, then))| (operation, now - then))
            .filter(|(_, made)| *made > 0)
            .collect()
    }

    /// A batch of `lines` log lines of the default tenant that no
    /// compression makes short.
    fn logs(lines: u64) -> tessera_types::EventBatch {
        let mut random = tessera_types::SeededRandom::new(7);
        let rows: Vec<tessera_types::LogRow> = (0..lines)
            .map(|_| tessera_types::LogRow {
                body: Some(format!(
                    "{:032x}{:032x}",
                    tessera_types::Random::next_u128(&mut random),
                    tessera_types::Random::next_u128(&mut random)
                )),
                ..tessera_types::LogRow::default()
            })
            .collect();
        tessera_types::EventBatch::from_logs(
            tessera_types::TenantId::DEFAULT,
            tessera_types::DatasetName::new("prod").unwrap(),
            &rows,
        )
        .unwrap()
    }

    /// Intake and collection ask what every entry of a log is, and
    /// the rows of its segments are nearly all a log holds: a header
    /// is read from the end of its entry, in one request that leaves
    /// the rows in the store.
    #[tokio::test]
    async fn a_header_is_read_without_the_rows_of_its_segment() {
        let inner = MemoryStore::new();
        let metered = Arc::new(tessera_object::MeteredStore::new(Arc::new(inner.clone())));
        let store: Arc<dyn ObjectStore> = metered.clone();
        let mut log =
            WriterLog::start(Arc::clone(&store), Env::simulated(1, Timestamp::UNIX_EPOCH));
        let seq = log.append_segment(&[logs(4_000)]).await.unwrap();
        let writer = log.writer_id();
        let stored = inner
            .get(&layout::writer_log_entry(writer, seq))
            .await
            .unwrap();
        assert!(
            stored.len() as u64 > 2 * HEADER_READ,
            "the segment is longer than what a header read asks for: {}",
            stored.len()
        );

        let before = metered.requests().by_operation();
        let found = header(&*store, writer, seq, &Keys::none()).await.unwrap();
        assert_eq!(made_since(&metered, before), [("get_tail", 1)]);
        let whole = read(&*store, writer, seq).await.unwrap();
        assert_eq!(&found, whole.header());
        assert!(
            matches!(&found.body, EntryBody::Segment { blocks } if blocks[0].rows == 4_000),
            "{:?}",
            found.body
        );
    }

    /// A flush record of `files` files of the default tenant.
    fn flush(files: u128) -> EntryBody {
        let tenant = tessera_types::TenantId::DEFAULT;
        let dataset = tessera_types::DatasetName::new("prod").unwrap();
        let partition = tessera_types::PartitionId {
            tenant: tenant.clone(),
            dataset: dataset.clone(),
            kind: tessera_types::Kind::Log,
            window: tessera_types::Window::containing(
                Timestamp::UNIX_EPOCH,
                tessera_types::Window::DEFAULT_LENGTH,
            )
            .unwrap(),
        };
        let file = |id: u128| tessera_types::FileMeta {
            file_id: tessera_types::FileId::from_u128(id),
            partition: partition.clone(),
            pool: tessera_types::PoolId::new("root").unwrap(),
            level: tessera_types::Level::L1,
            source: tessera_types::FileSource::Writer {
                writer: WriterId::from_u128(1),
                after: Seq::ZERO,
                through: Seq::new(2),
            },
            min_timestamp: Timestamp::from_nanos(1),
            max_timestamp: Timestamp::from_nanos(9),
            row_count: 9,
            size_bytes: 100,
            layout_epoch: 0,
            sort_key: vec!["timestamp".into()],
            columns: Vec::new(),
            indexes: Vec::new(),
            ingested_at: None,
        };
        EntryBody::Flush {
            tenant,
            dataset,
            after: Seq::ZERO,
            through: Seq::new(2),
            files: (0..files).map(file).collect(),
        }
    }

    /// A header may be longer than what is first asked for: a flush
    /// record names every file it wrote. The entry's last bytes say
    /// how long, and that much is asked for.
    #[tokio::test]
    async fn a_header_longer_than_the_first_read_is_asked_for_again() {
        let metered = Arc::new(tessera_object::MeteredStore::new(Arc::new(
            MemoryStore::new(),
        )));
        let store: Arc<dyn ObjectStore> = metered.clone();
        let mut log =
            WriterLog::start(Arc::clone(&store), Env::simulated(1, Timestamp::UNIX_EPOCH));
        let record = flush(400);
        let seq = log.append(record.clone()).await.unwrap();
        let before = metered.requests().by_operation();
        let found = header(&*store, log.writer_id(), seq, &Keys::none())
            .await
            .unwrap();
        assert_eq!(found.body, record);
        assert_eq!(made_since(&metered, before), [("get_tail", 2)]);
    }

    /// Names key `k` for every tenant.
    struct Keyed;

    impl tessera_types::KeyPolicy for Keyed {
        fn key_name(&self, _: &tessera_types::TenantId) -> Option<String> {
            Some("k".to_owned())
        }
    }

    /// SE-2: a keyed tenant's flush record is stored with its files
    /// sealed before its header. The header read of it holds them
    /// opened, as the record was written, whether the record came
    /// whole with its last bytes or not.
    #[tokio::test]
    async fn the_header_of_a_sealed_flush_record_holds_its_files() {
        let env = Env::simulated(1, Timestamp::UNIX_EPOCH).with_keys(Keys::new(
            tessera_types::Keyring::default().with(
                "k",
                tessera_types::TenantKey::from_bytes([1; tessera_types::keys::KEY_LEN]),
            ),
            Arc::new(Keyed),
        ));
        let keys = Arc::clone(&env.keys);
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let mut log = WriterLog::start(Arc::clone(&store), env);
        for files in [1, 400] {
            let record = flush(files);
            let seq = log.append(record.clone()).await.unwrap();
            let stored = read(&*store, log.writer_id(), seq).await.unwrap();
            assert!(
                matches!(stored.body(), EntryBody::SealedFlush { .. }),
                "stored sealed: {:?}",
                stored.body()
            );
            let found = header(&*store, log.writer_id(), seq, &keys).await.unwrap();
            assert_eq!(found.body, record, "{files} files");
            assert!(matches!(
                header(&*store, log.writer_id(), seq, &Keys::none()).await,
                Err(WalError::Keys(_))
            ));
        }
    }

    /// The headers of a log are walked as its entries are: from the
    /// first entry the log still has, or by the positions that follow
    /// one, until the first that is missing.
    #[tokio::test]
    async fn the_headers_of_a_log_are_walked_like_its_entries() {
        let store = MemoryStore::new();
        let mut log = WriterLog::start(
            Arc::new(store.clone()),
            Env::simulated(1, Timestamp::UNIX_EPOCH),
        );
        log.append_segment(&[logs(1)]).await.unwrap();
        for _ in 0..3 {
            log.append(EntryBody::End).await.unwrap();
        }
        let writer = log.writer_id();
        store
            .delete(&layout::writer_log_entry(writer, Seq::new(1)))
            .await
            .unwrap();
        let shared: Arc<dyn ObjectStore> = Arc::new(store);
        let positions = async |found: BoxStream<'static, Result<EntryHeader, WalError>>| {
            found
                .map_ok(|header| header.seq)
                .try_collect::<Vec<Seq>>()
                .await
                .unwrap()
        };
        let keys = Arc::new(Keys::none());
        let all = headers(Arc::clone(&shared), writer, Seq::ZERO, Arc::clone(&keys));
        assert_eq!(
            positions(all).await,
            [Seq::new(2), Seq::new(3), Seq::new(4)],
            "from the first entry the log still has"
        );
        let after = |seq| headers_following(Arc::clone(&shared), writer, seq, Arc::clone(&keys));
        assert_eq!(
            positions(after(Seq::new(2))).await,
            [Seq::new(3), Seq::new(4)]
        );
        assert!(
            positions(after(Seq::ZERO)).await.is_empty(),
            "nothing past a collected entry"
        );
    }

    #[tokio::test]
    async fn an_unknown_log_is_empty() {
        let store = MemoryStore::new();
        assert!(
            list(&store, WriterId::from_u128(1))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(last(&store, WriterId::from_u128(1)).await.unwrap(), None);
        let read: Vec<Entry> = entries(
            Arc::new(store),
            WriterId::from_u128(1),
            Seq::ZERO,
            Arc::new(Keys::none()),
        )
        .try_collect()
        .await
        .unwrap();
        assert!(read.is_empty());
    }

    #[tokio::test]
    async fn a_corrupt_entry_is_reported() {
        let store = MemoryStore::new();
        let writer = WriterId::from_u128(1);
        store
            .put(
                &layout::writer_log_entry(writer, Seq::new(1)),
                Bytes::from_static(b"junk"),
            )
            .await
            .unwrap();
        let error = read(&store, writer, Seq::new(1)).await.unwrap_err();
        assert!(matches!(error, WalError::Corrupt { .. }), "{error}");
        let streamed: Result<Vec<Entry>, _> =
            entries(Arc::new(store), writer, Seq::ZERO, Arc::new(Keys::none()))
                .try_collect()
                .await;
        assert!(matches!(streamed, Err(WalError::Corrupt { .. })));
    }

    #[tokio::test]
    async fn reading_starts_at_the_first_surviving_entry() {
        let store = MemoryStore::new();
        let mut log = WriterLog::start(
            Arc::new(store.clone()),
            Env::simulated(1, Timestamp::UNIX_EPOCH),
        );
        for _ in 0..4 {
            log.append(EntryBody::End).await.unwrap();
        }
        // Entries 1 and 2 were collected.
        for seq in [1, 2] {
            store
                .delete(&layout::writer_log_entry(log.writer_id(), Seq::new(seq)))
                .await
                .unwrap();
        }
        let read: Vec<Seq> = entries(
            Arc::new(store.clone()),
            log.writer_id(),
            Seq::ZERO,
            Arc::new(Keys::none()),
        )
        .map_ok(|e| e.seq())
        .try_collect()
        .await
        .unwrap();
        assert_eq!(read, vec![Seq::new(3), Seq::new(4)]);
        let after: Vec<Seq> = entries(
            Arc::new(store),
            log.writer_id(),
            Seq::new(3),
            Arc::new(Keys::none()),
        )
        .map_ok(|e| e.seq())
        .try_collect()
        .await
        .unwrap();
        assert_eq!(after, vec![Seq::new(4)]);
    }

    #[tokio::test]
    async fn lists_every_writer_with_entries() {
        let store = MemoryStore::new();
        assert!(writers(&store).await.unwrap().is_empty());
        let env = Env::simulated(1, Timestamp::UNIX_EPOCH);
        let mut a = WriterLog::start(Arc::new(store.clone()), env.clone());
        let mut b = WriterLog::start(Arc::new(store.clone()), env);
        a.append(EntryBody::End).await.unwrap();
        b.append(EntryBody::End).await.unwrap();
        b.append(EntryBody::End).await.unwrap();
        let mut expected = vec![a.writer_id(), b.writer_id()];
        expected.sort();
        assert_eq!(writers(&store).await.unwrap(), expected);
        // The background work asks every round: by name, not by
        // walking every entry of every log.
        let shallow = tessera_object::ShallowStore(store);
        assert_eq!(writers(&shallow).await.unwrap(), expected);
    }
}
