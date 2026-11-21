// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Reading a writer log: by intake, tail reads, continuous queries and
//! recovery.

use std::sync::Arc;

use futures::TryStreamExt;
use futures::stream::{self, BoxStream};
use tessera_object::{ObjectError, ObjectStore, layout};
use tessera_types::{Keys, Seq, WriterId};

use crate::{Entry, WalError};

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
    Entry::decode(&bytes).map_err(|e| WalError::Corrupt {
        key,
        source: Box::new(e),
    })
}

/// The entries after `from`, in order, until the first missing position,
/// their sealed blocks unlocked with `keys` (SE-2).
///
/// Garbage collection deletes a log's oldest entries (GC-2), so reading
/// starts at the first entry that still exists after `from`, found by
/// listing the log once; from there every position must be present.
pub fn entries(
    store: Arc<dyn ObjectStore>,
    writer: WriterId,
    from: Seq,
    keys: Arc<Keys>,
) -> BoxStream<'static, Result<Entry, WalError>> {
    Box::pin(stream::try_unfold((from, false), move |(seq, started)| {
        let store = Arc::clone(&store);
        let keys = Arc::clone(&keys);
        async move {
            let next = if started {
                seq.next()
            } else {
                match list(&*store, writer).await?.into_iter().find(|s| *s > seq) {
                    Some(first) => first,
                    None => return Ok(None),
                }
            };
            match read(&*store, writer, next).await {
                Ok(mut entry) => {
                    entry.unlock(&keys)?;
                    Ok(Some((entry, (next, true))))
                }
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
