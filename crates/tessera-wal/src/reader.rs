// SPDX-License-Identifier: Apache-2.0

//! Reading a writer log: by intake, tail reads, continuous queries and
//! recovery.

use std::future::Future;
use std::sync::Arc;

use futures::TryStreamExt;
use futures::stream::{self, BoxStream};
use tessera_object::{Key, ObjectError, ObjectStore, layout};
use tessera_types::{EventBatch, Keys, Seq, WriterId};

use crate::{BlockMeta, Entry, EntryBody, EntryHeader, WalError};

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
    let end = End::read(store, &key).await?;
    let header = EntryHeader::decode(&end.bytes).map_err(|e| corrupt(&key, e))?;
    if !matches!(header.body, EntryBody::SealedFlush { .. }) {
        return Ok(header);
    }
    // What a sealed flush record sealed, its files, lies before its
    // header. Such a record is small, and mostly came whole.
    let mut entry = if end.whole {
        Entry::decode(&end.bytes).map_err(|e| corrupt(&key, e))?
    } else {
        read(store, writer, seq).await?
    };
    entry.unlock(keys)?;
    Ok(entry.header().clone())
}

/// The last bytes of an entry, as many as hold its header.
struct End {
    bytes: bytes::Bytes,
    /// Whether they are the whole entry.
    whole: bool,
    /// How many of them are the header and the trailer after it.
    header_len: usize,
}

impl End {
    async fn read(store: &dyn ObjectStore, key: &Key) -> Result<Self, WalError> {
        let mut bytes = store.get_tail(key, HEADER_READ).await?;
        // An entry shorter than what was asked for came whole.
        let whole = (bytes.len() as u64) < HEADER_READ;
        let header_len = EntryHeader::stored_len(&bytes).map_err(|e| corrupt(key, e))?;
        if header_len > bytes.len() && !whole {
            bytes = store.get_tail(key, header_len as u64).await?;
        }
        Ok(End {
            bytes,
            whole,
            header_len,
        })
    }
}

/// What [`blocks`] read of an entry.
#[derive(Clone, Debug)]
pub struct Blocks {
    /// The entry's header; a flush record stored sealed is left so
    /// ([`header`] opens it).
    pub header: EntryHeader,
    /// The rows of the blocks that were picked, in the order of the
    /// entry; none of an entry that is not a segment.
    pub rows: Vec<EventBatch>,
}

/// One entry as a reader of some of its rows wants it: its header
/// and, of a segment, the rows of the blocks `picked` chooses, sealed
/// blocks opened with `keys` (SE-2). A segment holds a block for each
/// tenant, dataset and kind its writer took rows of, and a reader
/// mostly wants one of them: the header is read from the entry's last
/// bytes, a block that came with them is taken from them, and the
/// others are asked for, those that lie one after the other in one
/// request. The blocks that were not picked stay in the store.
///
/// # Errors
///
/// `Store` with `NotFound` if there is no such entry; `Corrupt` if it
/// cannot be decoded; `Keys` if a picked block does not open.
pub async fn blocks(
    store: &dyn ObjectStore,
    writer: WriterId,
    seq: Seq,
    keys: &Keys,
    picked: &(dyn Fn(&BlockMeta) -> bool + Sync),
) -> Result<Blocks, WalError> {
    let key = layout::writer_log_entry(writer, seq);
    let end = End::read(store, &key).await?;
    let header = EntryHeader::decode(&end.bytes).map_err(|e| corrupt(&key, e))?;
    let EntryBody::Segment { blocks: metas } = &header.body else {
        return Ok(Blocks {
            header,
            rows: Vec::new(),
        });
    };
    // The blocks lie one after the other from the entry's start, and
    // the header after them: where they end is where what was read
    // with the header begins, less what of them came with it.
    let blocks_end = metas.iter().map(|m| m.offset + m.length).max().unwrap_or(0);
    let read_from = (blocks_end + end.header_len as u64).saturating_sub(end.bytes.len() as u64);
    let mut rows = Vec::new();
    let mut asked: Option<(std::ops::Range<u64>, bytes::Bytes)> = None;
    for (index, meta) in metas.iter().enumerate().filter(|(_, m)| picked(m)) {
        let block = meta.offset..meta.offset + meta.length;
        let stored = if block.start >= read_from {
            within(&end.bytes, read_from, &block)
        } else {
            if asked.as_ref().is_none_or(|(run, _)| run.end <= block.start) {
                let run = run_from(metas, index, picked, read_from);
                let bytes = store.get_range(&key, run.clone()).await?;
                asked = Some((run, bytes));
            }
            asked
                .as_ref()
                .and_then(|(run, bytes)| within(bytes, run.start, &block))
        };
        let stored = stored.ok_or_else(|| corrupt(&key, WalError::NoSuchBlock(index)))?;
        rows.push(header.rows_of(index, stored, keys).map_err(|e| match e {
            WalError::Keys(_) => e,
            other => corrupt(&key, other),
        })?);
    }
    Ok(Blocks { header, rows })
}

/// The bytes of `block` in `bytes`, which begin at `start` of the
/// entry; none if they do not hold it all.
fn within<'a>(bytes: &'a [u8], start: u64, block: &std::ops::Range<u64>) -> Option<&'a [u8]> {
    let from = usize::try_from(block.start.checked_sub(start)?).ok()?;
    let to = usize::try_from(block.end.checked_sub(start)?).ok()?;
    bytes.get(from..to)
}

/// The bytes to ask for to have block `first` and the picked blocks
/// that follow it with nothing between them, up to `before`: blocks
/// that lie one after the other cost one request.
fn run_from(
    metas: &[BlockMeta],
    first: usize,
    picked: &(dyn Fn(&BlockMeta) -> bool + Sync),
    before: u64,
) -> std::ops::Range<u64> {
    let mut run = metas[first].offset..metas[first].offset + metas[first].length;
    for meta in &metas[first + 1..] {
        if meta.offset != run.end || meta.offset >= before || !picked(meta) {
            break;
        }
        run.end = meta.offset + meta.length;
    }
    run
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

    /// A batch of `lines` log lines of the default tenant's `prod`
    /// that no compression makes short.
    fn logs(lines: u64) -> EventBatch {
        logs_of("prod", lines)
    }

    /// A batch of `lines` log lines of `dataset` of the default tenant
    /// that no compression makes short.
    fn logs_of(dataset: &str, lines: u64) -> EventBatch {
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
        EventBatch::from_logs(
            tessera_types::TenantId::DEFAULT,
            tessera_types::DatasetName::new(dataset).unwrap(),
            &rows,
        )
        .unwrap()
    }

    /// Whether a block is of one of `datasets`.
    fn of(datasets: &'static [&'static str]) -> impl Fn(&BlockMeta) -> bool + Sync {
        move |meta| datasets.contains(&meta.dataset.as_str())
    }

    /// The datasets of `rows` and how many rows each batch holds.
    fn held(rows: &[EventBatch]) -> Vec<(&str, usize)> {
        rows.iter()
            .map(|batch| (batch.dataset().as_str(), batch.len()))
            .collect()
    }

    /// A segment holds the rows of every dataset its writer took, and
    /// a reader of one dataset wants its block: the header is read
    /// from the end, the block by its place, and the blocks between
    /// stay in the store. Blocks that lie one after the other are one
    /// request, and one that came with the header is none.
    #[tokio::test]
    async fn the_blocks_picked_are_read_and_the_others_left() {
        let metered = Arc::new(tessera_object::MeteredStore::new(Arc::new(
            MemoryStore::new(),
        )));
        let store: Arc<dyn ObjectStore> = metered.clone();
        let mut log =
            WriterLog::start(Arc::clone(&store), Env::simulated(1, Timestamp::UNIX_EPOCH));
        let segment = [
            logs_of("a", 4_000),
            logs_of("b", 3_000),
            logs_of("c", 2_000),
            logs_of("last", 5),
        ];
        let seq = log.append_segment(&segment).await.unwrap();
        let writer = log.writer_id();
        let none = Keys::none();
        let read = async |datasets: &'static [&'static str]| {
            let before = metered.requests().by_operation();
            let found = blocks(&*store, writer, seq, &none, &of(datasets))
                .await
                .unwrap();
            assert_eq!(found.header.seq, seq);
            (
                held(&found.rows)
                    .into_iter()
                    .map(|(dataset, rows)| (dataset.to_owned(), rows))
                    .collect::<Vec<_>>(),
                made_since(&metered, before),
            )
        };
        let rows = |held: &[(&str, usize)]| -> Vec<(String, usize)> {
            held.iter().map(|(d, n)| ((*d).to_owned(), *n)).collect()
        };
        assert_eq!(
            read(&["b"]).await,
            (
                rows(&[("b", 3_000)]),
                vec![("get_range", 1), ("get_tail", 1)]
            )
        );
        assert_eq!(
            read(&["a", "b"]).await,
            (
                rows(&[("a", 4_000), ("b", 3_000)]),
                vec![("get_range", 1), ("get_tail", 1)]
            ),
            "blocks that lie one after the other are one request"
        );
        assert_eq!(
            read(&["a", "c"]).await,
            (
                rows(&[("a", 4_000), ("c", 2_000)]),
                vec![("get_range", 2), ("get_tail", 1)]
            ),
            "the block between them is not read"
        );
        assert_eq!(
            read(&["c", "last"]).await,
            (
                rows(&[("c", 2_000), ("last", 5)]),
                vec![("get_range", 1), ("get_tail", 1)]
            ),
            "a block that came with the header is not asked for"
        );
        assert_eq!(
            read(&["last"]).await,
            (rows(&[("last", 5)]), vec![("get_tail", 1)])
        );
        assert_eq!(read(&["none"]).await, (rows(&[]), vec![("get_tail", 1)]));
    }

    /// An entry shorter than a header read comes whole with it, and
    /// an entry that is not a segment has no rows.
    #[tokio::test]
    async fn a_short_entry_gives_its_blocks_in_one_read() {
        let metered = Arc::new(tessera_object::MeteredStore::new(Arc::new(
            MemoryStore::new(),
        )));
        let store: Arc<dyn ObjectStore> = metered.clone();
        let mut log =
            WriterLog::start(Arc::clone(&store), Env::simulated(1, Timestamp::UNIX_EPOCH));
        let segment = log
            .append_segment(&[logs_of("a", 3), logs_of("b", 2)])
            .await
            .unwrap();
        let end = log.append(EntryBody::End).await.unwrap();
        let before = metered.requests().by_operation();
        let none = Keys::none();
        let found = blocks(&*store, log.writer_id(), segment, &none, &of(&["b"]))
            .await
            .unwrap();
        assert_eq!(held(&found.rows), [("b", 2)]);
        let found = blocks(&*store, log.writer_id(), end, &none, &of(&["b"]))
            .await
            .unwrap();
        assert_eq!(found.header.body, EntryBody::End);
        assert!(found.rows.is_empty());
        assert_eq!(made_since(&metered, before), [("get_tail", 2)]);
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

    /// SE-2: a keyed tenant's blocks are stored sealed. A block that
    /// is picked is opened with the keys, wherever it was read from,
    /// and does not open without them.
    #[tokio::test]
    async fn a_sealed_block_that_is_picked_is_opened() {
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
        let seq = log
            .append_segment(&[logs_of("a", 4_000), logs_of("b", 3_000), logs_of("c", 2)])
            .await
            .unwrap();
        let writer = log.writer_id();
        let whole = read(&*store, writer, seq).await.unwrap();
        assert!(
            whole.block_metas().iter().all(|meta| meta.key.is_some()),
            "stored sealed"
        );
        let found = blocks(&*store, writer, seq, &keys, &of(&["b", "c"]))
            .await
            .unwrap();
        assert_eq!(held(&found.rows), [("b", 3_000), ("c", 2)]);
        for dataset in [&["b"], &["c"]] {
            assert!(matches!(
                blocks(&*store, writer, seq, &Keys::none(), &of(dataset)).await,
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
