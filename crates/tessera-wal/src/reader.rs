// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Reading a writer log: by intake, tail reads, continuous queries and
//! recovery.

use std::sync::Arc;

use futures::TryStreamExt;
use futures::stream::{self, BoxStream};
use tessera_object::{ObjectError, ObjectStore, layout};
use tessera_types::{Seq, WriterId};

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

/// The entries after `from`, in order, until the first missing position.
pub fn entries(
    store: Arc<dyn ObjectStore>,
    writer: WriterId,
    from: Seq,
) -> BoxStream<'static, Result<Entry, WalError>> {
    Box::pin(stream::try_unfold(from, move |seq| {
        let store = Arc::clone(&store);
        async move {
            let next = seq.next();
            match read(&*store, writer, next).await {
                Ok(entry) => Ok(Some((entry, next))),
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
        let read: Vec<Seq> = entries(Arc::new(store.clone()), log.writer_id(), Seq::new(1))
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
        let read: Vec<Entry> = entries(Arc::new(store), WriterId::from_u128(1), Seq::ZERO)
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
        let streamed: Result<Vec<Entry>, _> = entries(Arc::new(store), writer, Seq::ZERO)
            .try_collect()
            .await;
        assert!(matches!(streamed, Err(WalError::Corrupt { .. })));
    }
}
