// SPDX-License-Identifier: Apache-2.0

//! Writer-log entries and their encoding.
//!
//! An entry object is laid out as
//!
//! ```text
//! [block 0][block 1]...[header JSON][u32 header length][magic "TWL1"]
//! ```
//!
//! The header comes last so that a reader can fetch it with one range
//! request from the end, then fetch only the blocks it needs (ST-7). Each
//! block is one Arrow IPC stream holding the rows of one tenant, dataset
//! and kind, compressed with LZ4, and sealed under the tenant's key
//! where the tenant has one (SE-2): the block's metadata names the key,
//! and a reader unlocks the entry with its keyring before reading the
//! block. A flush record names files with the bounds of their columns,
//! which are the tenant's data as much as the rows: for a tenant with
//! a key the files are the entry's one block, sealed, and the header
//! says only whose flush it is and what it covers.

use std::collections::BTreeMap;
use std::io::Cursor;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::ipc::CompressionType;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tessera_types::{
    DatasetName, EventBatch, FileMeta, Id128, Keys, Kind, NodeId, Random, Seq, TenantId, Timestamp,
    WriterId,
};

use crate::WalError;

/// The newest format version of entries this release writes and reads
/// (OP-7): [`PLAIN_FORMAT`] for an entry without a sealed block,
/// [`SEALED_FORMAT`] for one with.
pub const FORMAT: u32 = SEALED_FORMAT;
/// The format of entries whose blocks are all plain.
const PLAIN_FORMAT: u32 = 1;
/// The format of entries with a block sealed under a tenant key (SE-2),
/// which a build without the keys module cannot read.
const SEALED_FORMAT: u32 = 2;

const MAGIC: &[u8; 4] = b"TWL1";
const TRAILER_LEN: usize = 8;

/// The identity of one entry write, so that a writer recognises its own
/// entry after a lost response.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EntryId(Id128);

impl EntryId {
    /// A fresh identity.
    pub fn random(random: &mut impl Random) -> Self {
        EntryId(Id128::random(random))
    }
}

/// Where one block lies in a segment and what it holds.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct BlockMeta {
    /// The block's tenant.
    pub tenant: TenantId,
    /// The block's dataset.
    pub dataset: DatasetName,
    /// The block's kind.
    pub kind: Kind,
    /// Byte offset of the block in the entry.
    pub offset: u64,
    /// Byte length of the block.
    pub length: u64,
    /// How many rows it holds.
    pub rows: u64,
    /// The earliest observed time of its rows.
    pub min_observed: Timestamp,
    /// The latest observed time of its rows.
    pub max_observed: Timestamp,
    /// The earliest event time of its rows; the open bound for entries
    /// written before this field, so that garbage collection stays
    /// conservative (GC-2).
    #[serde(default = "Timestamp::min_value")]
    pub min_timestamp: Timestamp,
    /// The latest event time of its rows; the open bound for older
    /// entries.
    #[serde(default = "Timestamp::max_value")]
    pub max_timestamp: Timestamp,
    /// The name of the key the block is sealed under (SE-2); none for a
    /// plain block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// What an entry means (section 6.3).
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum EntryBody {
    /// A batch of acknowledged rows, one block per tenant, dataset and kind.
    Segment {
        /// The blocks, in entry order.
        blocks: Vec<BlockMeta>,
    },
    /// For one dataset, the rows in segments `(after, through]` are now
    /// also in these L1 files.
    Flush {
        /// The dataset's tenant.
        tenant: TenantId,
        /// The dataset.
        dataset: DatasetName,
        /// The last segment before the covered range.
        after: Seq,
        /// The last covered segment.
        through: Seq,
        /// The files, each tagged by its partition.
        files: Vec<FileMeta>,
    },
    /// A flush record as it is stored for a tenant with a key (SE-2):
    /// its files are the entry's block, sealed under `key`.
    /// [`Entry::unlock`] makes it the [`EntryBody::Flush`] it is.
    #[serde(rename = "sealed_flush")]
    SealedFlush {
        /// The dataset's tenant.
        tenant: TenantId,
        /// The dataset.
        dataset: DatasetName,
        /// The last segment before the covered range.
        after: Seq,
        /// The last covered segment.
        through: Seq,
        /// The name of the key the files are sealed under.
        key: String,
    },
    /// The writer is declared dead; only recovery appends after this.
    Seal {
        /// The node that sealed the log.
        by: NodeId,
    },
    /// The log is complete.
    End,
}

/// The metadata of an entry: everything but the block bytes.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct EntryHeader {
    /// The format this entry was written in.
    pub format: u32,
    /// The log the entry belongs to.
    pub writer: WriterId,
    /// The entry's position in the log.
    pub seq: Seq,
    /// The identity of the write.
    pub id: EntryId,
    /// When it was written, by the writer's clock.
    pub written_at: Timestamp,
    /// What it means.
    pub body: EntryBody,
}

impl EntryHeader {
    /// How many of an entry's last bytes hold its header and the
    /// trailer after it, as the entry's last bytes say.
    ///
    /// # Errors
    ///
    /// `Unreadable` if `tail` does not end as an entry does.
    pub(crate) fn stored_len(tail: &[u8]) -> Result<usize, WalError> {
        let total = tail.len();
        if total < TRAILER_LEN || &tail[total - 4..] != MAGIC {
            return Err(WalError::Unreadable("missing trailer".into()));
        }
        let header_len = u32::from_le_bytes(
            tail[total - TRAILER_LEN..total - 4]
                .try_into()
                .unwrap_or_else(|_| unreachable!("four bytes")),
        ) as usize;
        Ok(header_len.saturating_add(TRAILER_LEN))
    }

    /// The header that `tail`, the last bytes of an entry, ends with.
    ///
    /// # Errors
    ///
    /// `Unreadable` if `tail` does not end with a header or is too
    /// short to hold it ([`stored_len`](Self::stored_len) says how
    /// long it must be); `UnsupportedFormat` if the entry was written
    /// by a newer release.
    pub(crate) fn decode(tail: &[u8]) -> Result<Self, WalError> {
        let start = tail
            .len()
            .checked_sub(Self::stored_len(tail)?)
            .ok_or_else(|| WalError::Unreadable("header length exceeds object".into()))?;
        let header: EntryHeader = serde_json::from_slice(&tail[start..tail.len() - TRAILER_LEN])
            .map_err(|e| WalError::Unreadable(format!("header: {e}")))?;
        if header.format > FORMAT {
            return Err(WalError::UnsupportedFormat {
                format: header.format,
                supported: FORMAT,
            });
        }
        Ok(header)
    }
}

/// One entry of a writer log.
#[derive(Clone, PartialEq, Debug)]
pub struct Entry {
    header: EntryHeader,
    /// The blocks as stored: sealed where their metadata names a key.
    blocks: Bytes,
    /// Sealed blocks that were opened, by index: by the writer that
    /// sealed them, or by [`Entry::unlock`].
    opened: BTreeMap<usize, Bytes>,
    /// The key the files of a flush record are sealed under in
    /// `blocks`, when the header holds them opened: the entry is
    /// stored as a [`EntryBody::SealedFlush`].
    flush_key: Option<String>,
}

impl Entry {
    /// An entry without blocks: a flush record, seal or end.
    #[must_use]
    pub fn new(
        writer: WriterId,
        seq: Seq,
        id: EntryId,
        written_at: Timestamp,
        body: EntryBody,
    ) -> Self {
        Entry {
            header: EntryHeader {
                format: PLAIN_FORMAT,
                writer,
                seq,
                id,
                written_at,
                body,
            },
            blocks: Bytes::new(),
            opened: BTreeMap::new(),
            flush_key: None,
        }
    }

    /// An entry without rows, as a writer appends it: a flush record
    /// has its files sealed under its tenant's key from `keys` where
    /// the tenant has one (SE-2); anything else is as [`Entry::new`]
    /// makes it.
    ///
    /// # Errors
    ///
    /// `Keys` if the tenant names a key the keyring lacks.
    pub fn record(
        writer: WriterId,
        seq: Seq,
        id: EntryId,
        written_at: Timestamp,
        body: EntryBody,
        keys: &Keys,
    ) -> Result<Self, WalError> {
        let mut entry = Entry::new(writer, seq, id, written_at, body);
        let EntryBody::Flush { tenant, files, .. } = &entry.header.body else {
            return Ok(entry);
        };
        let Some(named) = keys.for_tenant(tenant)? else {
            return Ok(entry);
        };
        let files = serde_json::to_vec(files)
            .unwrap_or_else(|e| unreachable!("file metadata always serializes: {e}"));
        entry.blocks = Bytes::from(named.key.seal(&block_aad(writer, seq, 0), &files));
        entry.flush_key = Some(named.name);
        entry.header.format = SEALED_FORMAT;
        Ok(entry)
    }

    /// A segment holding `batches`, one block per tenant, dataset and kind,
    /// each sealed under its tenant's key from `keys` where the tenant
    /// has one (SE-2). Batches of the same tenant, dataset and kind are
    /// merged into one block.
    ///
    /// # Errors
    ///
    /// `EmptySegment` if there are no batches; `Arrow` if a block cannot
    /// be encoded; `Keys` if a tenant names a key the keyring lacks.
    pub fn segment(
        writer: WriterId,
        seq: Seq,
        id: EntryId,
        written_at: Timestamp,
        batches: &[EventBatch],
        keys: &Keys,
    ) -> Result<Self, WalError> {
        if batches.is_empty() {
            return Err(WalError::EmptySegment);
        }
        let mut groups: Vec<(&EventBatch, Vec<&RecordBatch>)> = Vec::new();
        for batch in batches {
            match groups
                .iter_mut()
                .find(|(first, _)| same_block(first, batch))
            {
                Some((_, rows)) => rows.push(batch.rows()),
                None => groups.push((batch, vec![batch.rows()])),
            }
        }
        let mut bytes = BytesMut::new();
        let mut blocks = Vec::with_capacity(groups.len());
        let mut opened = BTreeMap::new();
        for (index, (first, rows)) in groups.into_iter().enumerate() {
            let offset = bytes.len() as u64;
            let merged = concat_batches(&first.rows().schema(), rows)?;
            let batch = EventBatch::new(
                first.tenant().clone(),
                first.dataset().clone(),
                first.kind(),
                merged,
            )?;
            let encoded = encode_block(batch.rows())?;
            let named = keys.for_tenant(batch.tenant())?;
            match &named {
                Some(named) => {
                    bytes.extend_from_slice(
                        &named.key.seal(&block_aad(writer, seq, index), &encoded),
                    );
                    opened.insert(index, Bytes::from(encoded));
                }
                None => bytes.extend_from_slice(&encoded),
            }
            let (min_observed, max_observed) = batch.observed_bounds();
            let (min_timestamp, max_timestamp) = batch.time_bounds();
            blocks.push(BlockMeta {
                tenant: batch.tenant().clone(),
                dataset: batch.dataset().clone(),
                kind: batch.kind(),
                offset,
                length: bytes.len() as u64 - offset,
                rows: batch.len() as u64,
                min_observed,
                max_observed,
                min_timestamp,
                max_timestamp,
                key: named.map(|n| n.name),
            });
        }
        let format = if opened.is_empty() {
            PLAIN_FORMAT
        } else {
            SEALED_FORMAT
        };
        let body = EntryBody::Segment { blocks };
        Ok(Entry {
            header: EntryHeader {
                format,
                writer,
                seq,
                id,
                written_at,
                body,
            },
            blocks: bytes.freeze(),
            opened,
            flush_key: None,
        })
    }

    /// Opens every sealed block with `keys`, so that the blocks can be
    /// read, and the files of a sealed flush record, which becomes the
    /// flush record it is; nothing to do for an entry with nothing
    /// sealed.
    ///
    /// # Errors
    ///
    /// `Keys` when a block's key is not in the keyring or does not open
    /// the block; `Unreadable` when what a flush record sealed is not
    /// its files.
    pub fn unlock(&mut self, keys: &Keys) -> Result<(), WalError> {
        if let EntryBody::SealedFlush {
            tenant,
            dataset,
            after,
            through,
            key,
        } = &self.header.body
        {
            let aad = block_aad(self.header.writer, self.header.seq, 0);
            let plain = keys.open(key, &aad, &self.blocks)?;
            let files = serde_json::from_slice(&plain)
                .map_err(|e| WalError::Unreadable(format!("sealed flush record: {e}")))?;
            self.flush_key = Some(key.clone());
            self.header.body = EntryBody::Flush {
                tenant: tenant.clone(),
                dataset: dataset.clone(),
                after: *after,
                through: *through,
                files,
            };
            return Ok(());
        }
        let sealed: Vec<(usize, String)> = self
            .block_metas()
            .iter()
            .enumerate()
            .filter(|(index, _)| !self.opened.contains_key(index))
            .filter_map(|(index, meta)| meta.key.clone().map(|key| (index, key)))
            .collect();
        for (index, key) in sealed {
            let stored = self.stored(index)?;
            let aad = block_aad(self.header.writer, self.header.seq, index);
            let plain = keys.open(&key, &aad, stored)?;
            self.opened.insert(index, Bytes::from(plain));
        }
        Ok(())
    }

    /// The bytes of block `index` as stored.
    fn stored(&self, index: usize) -> Result<&[u8], WalError> {
        let meta = self
            .block_metas()
            .get(index)
            .ok_or(WalError::NoSuchBlock(index))?;
        let start = usize::try_from(meta.offset).map_err(|_| WalError::NoSuchBlock(index))?;
        let end =
            usize::try_from(meta.offset + meta.length).map_err(|_| WalError::NoSuchBlock(index))?;
        self.blocks
            .get(start..end)
            .ok_or(WalError::NoSuchBlock(index))
    }

    /// The entry's metadata.
    #[must_use]
    pub fn header(&self) -> &EntryHeader {
        &self.header
    }

    /// What the entry means.
    #[must_use]
    pub fn body(&self) -> &EntryBody {
        &self.header.body
    }

    /// The entry's position.
    #[must_use]
    pub fn seq(&self) -> Seq {
        self.header.seq
    }

    /// The blocks of a segment, in order; empty for other entries.
    #[must_use]
    pub fn block_metas(&self) -> &[BlockMeta] {
        match &self.header.body {
            EntryBody::Segment { blocks } => blocks,
            _ => &[],
        }
    }

    /// Decodes block `index`.
    ///
    /// # Errors
    ///
    /// `NoSuchBlock`; `Sealed` for a sealed block of an entry that was
    /// not unlocked; `Arrow` or `Batch` if the block is corrupt.
    pub fn block(&self, index: usize) -> Result<EventBatch, WalError> {
        let meta = self
            .block_metas()
            .get(index)
            .ok_or(WalError::NoSuchBlock(index))?;
        let slice = match (self.opened.get(&index), &meta.key) {
            (Some(opened), _) => opened.as_ref(),
            (None, Some(key)) => return Err(WalError::Sealed { key: key.clone() }),
            (None, None) => self.stored(index)?,
        };
        let rows = decode_block(slice)?;
        Ok(EventBatch::new(
            meta.tenant.clone(),
            meta.dataset.clone(),
            meta.kind,
            rows,
        )?)
    }

    /// Decodes the blocks of one tenant, dataset and kind (CQ-2).
    ///
    /// # Errors
    ///
    /// If a matching block is corrupt.
    pub fn blocks_for(
        &self,
        tenant: &TenantId,
        dataset: &DatasetName,
        kind: Kind,
    ) -> Result<Vec<EventBatch>, WalError> {
        self.block_metas()
            .iter()
            .enumerate()
            .filter(|(_, m)| m.tenant == *tenant && m.dataset == *dataset && m.kind == kind)
            .map(|(i, _)| self.block(i))
            .collect()
    }

    /// Every block, decoded.
    ///
    /// # Errors
    ///
    /// If a block is corrupt.
    pub fn all_blocks(&self) -> Result<Vec<EventBatch>, WalError> {
        (0..self.block_metas().len())
            .map(|i| self.block(i))
            .collect()
    }

    /// The header as it is stored: a flush record whose files are
    /// sealed says so and leaves them to its block.
    fn stored_header(&self) -> EntryHeader {
        let body = match (&self.flush_key, &self.header.body) {
            (
                Some(key),
                EntryBody::Flush {
                    tenant,
                    dataset,
                    after,
                    through,
                    ..
                },
            ) => EntryBody::SealedFlush {
                tenant: tenant.clone(),
                dataset: dataset.clone(),
                after: *after,
                through: *through,
                key: key.clone(),
            },
            (_, body) => body.clone(),
        };
        EntryHeader {
            body,
            ..self.header.clone()
        }
    }

    /// The bytes written to the object store.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let header = serde_json::to_vec(&self.stored_header())
            .unwrap_or_else(|e| unreachable!("an entry header always serializes: {e}"));
        let mut bytes = BytesMut::with_capacity(self.blocks.len() + header.len() + TRAILER_LEN);
        bytes.extend_from_slice(&self.blocks);
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(
            &u32::try_from(header.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        bytes.extend_from_slice(MAGIC);
        bytes.freeze()
    }

    /// Parses a whole entry object.
    ///
    /// # Errors
    ///
    /// `Unreadable` if the bytes are not an entry; `UnsupportedFormat` if
    /// it was written by a newer release.
    pub fn decode(bytes: &Bytes) -> Result<Self, WalError> {
        let header = EntryHeader::decode(bytes)?;
        let header_start = bytes.len() - EntryHeader::stored_len(bytes)?;
        Ok(Entry {
            header,
            blocks: bytes.slice(..header_start),
            opened: BTreeMap::new(),
            flush_key: None,
        })
    }
}

fn same_block(a: &EventBatch, b: &EventBatch) -> bool {
    a.tenant() == b.tenant() && a.dataset() == b.dataset() && a.kind() == b.kind()
}

/// What binds a sealed block to its entry and position, so that a block
/// moved elsewhere does not open.
fn block_aad(writer: WriterId, seq: Seq, index: usize) -> Vec<u8> {
    format!("{writer}/{seq}/{index}").into_bytes()
}

fn encode_block(rows: &RecordBatch) -> Result<Vec<u8>, WalError> {
    let options =
        IpcWriteOptions::default().try_with_compression(Some(CompressionType::LZ4_FRAME))?;
    let mut buffer = Vec::new();
    let mut writer = StreamWriter::try_new_with_options(&mut buffer, &rows.schema(), options)?;
    writer.write(rows)?;
    writer.finish()?;
    Ok(buffer)
}

fn decode_block(bytes: &[u8]) -> Result<RecordBatch, WalError> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok(concat_batches(&schema, &batches)?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tessera_types::keys::KEY_LEN;
    use tessera_types::{KeyError, KeyPolicy, Keyring, LogRow, SeededRandom, TenantKey};

    use super::*;

    fn logs(tenant: &str, dataset: &str, from: i64, count: i64) -> EventBatch {
        let rows: Vec<LogRow> = (from..from + count)
            .map(|i| LogRow {
                timestamp: Timestamp::from_nanos(i),
                observed_timestamp: Timestamp::from_nanos(i + 100),
                event_id: [u8::try_from(i % 256).unwrap_or(0); 16],
                body: Some(format!("line {i}")),
                ..LogRow::default()
            })
            .collect();
        EventBatch::from_logs(
            TenantId::new(tenant).unwrap(),
            DatasetName::new(dataset).unwrap(),
            &rows,
        )
        .unwrap()
    }

    fn id() -> EntryId {
        EntryId::random(&mut SeededRandom::new(1))
    }

    /// RS-5: no bytes, however mangled, make the decoder panic.
    #[test]
    fn mangled_entries_never_panic() {
        let end = Entry::new(
            WriterId::from_u128(1),
            Seq::new(1),
            id(),
            Timestamp::UNIX_EPOCH,
            EntryBody::End,
        );
        let segment = Entry::segment(
            WriterId::from_u128(1),
            Seq::new(2),
            id(),
            Timestamp::UNIX_EPOCH,
            &[logs("a", "prod", 0, 20)],
            &Keys::none(),
        )
        .unwrap();
        let sealed_flush = Entry::record(
            WriterId::from_u128(1),
            Seq::new(3),
            id(),
            Timestamp::UNIX_EPOCH,
            flush("a"),
            &keys_for("a"),
        )
        .unwrap();
        let seeds = [end.encode(), segment.encode(), sealed_flush.encode()];
        let mut random = tessera_types::SeededRandom::new(5);
        for round in 0..1_500 {
            let input = tessera_types::mutated(&seeds[round % seeds.len()], &mut random);
            if let Ok(mut entry) = Entry::decode(&Bytes::from(input)) {
                let _ = entry.unlock(&keys_for("a"));
            }
        }
    }

    /// Names key `k` for one tenant and none for the others.
    struct Only(&'static str);

    impl KeyPolicy for Only {
        fn key_name(&self, tenant: &TenantId) -> Option<String> {
            (tenant.as_str() == self.0).then(|| "k".to_owned())
        }
    }

    fn keys_for(tenant: &'static str) -> Keys {
        Keys::new(
            Keyring::default().with("k", TenantKey::from_bytes([1; KEY_LEN])),
            Arc::new(Only(tenant)),
        )
    }

    /// A flush record of `tenant` naming one file whose body column
    /// runs from one confidential line to another.
    fn flush(tenant: &str) -> EntryBody {
        let partition = tessera_types::PartitionId {
            tenant: TenantId::new(tenant).unwrap(),
            dataset: DatasetName::new("prod").unwrap(),
            kind: Kind::Log,
            window: tessera_types::Window::containing(
                Timestamp::UNIX_EPOCH,
                tessera_types::Window::DEFAULT_LENGTH,
            )
            .unwrap(),
        };
        let text = |value: &str| Some(tessera_types::event::Scalar::Text(value.into()));
        let file = FileMeta {
            file_id: tessera_types::FileId::from_u128(7),
            partition,
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
            columns: vec![tessera_types::ColumnStats {
                name: "body".into(),
                min: text("confidential a"),
                max: text("confidential z"),
                null_count: 0,
                distinct: None,
                quantiles: Vec::new(),
            }],
            indexes: Vec::new(),
            ingested_at: None,
        };
        EntryBody::Flush {
            tenant: TenantId::new(tenant).unwrap(),
            dataset: DatasetName::new("prod").unwrap(),
            after: Seq::ZERO,
            through: Seq::new(2),
            files: vec![file],
        }
    }

    fn holds(bytes: &[u8], needle: &str) -> bool {
        bytes.windows(needle.len()).any(|w| w == needle.as_bytes())
    }

    /// SE-2: the bounds of a file's columns are the tenant's data. A
    /// keyed tenant's flush record is stored with its files sealed and
    /// says only whose it is and what it covers; unlocked with the key
    /// it is the record that was written, and it does not unlock
    /// without. Another tenant's is stored plain, as before.
    #[test]
    fn the_files_of_a_keyed_tenants_flush_record_are_sealed() {
        let keys = keys_for("a");
        let record = |tenant| {
            Entry::record(
                WriterId::from_u128(1),
                Seq::new(3),
                id(),
                Timestamp::UNIX_EPOCH,
                flush(tenant),
                &keys,
            )
            .unwrap()
        };
        let written = record("a");
        assert_eq!(
            written.body(),
            &flush("a"),
            "the writer reads what it wrote"
        );
        let stored = written.encode();
        assert!(!holds(&stored, "confidential"), "no bound is readable");
        assert!(holds(&stored, "\"kind\":\"sealed_flush\""));
        assert!(holds(&stored, "\"key\":\"k\""));

        let mut read = Entry::decode(&stored).unwrap();
        assert!(
            matches!(
                read.body(),
                EntryBody::SealedFlush { through, key, .. } if *through == Seq::new(2) && key == "k"
            ),
            "{:?}",
            read.body()
        );
        let mut without = read.clone();
        assert!(matches!(
            without.unlock(&Keys::none()),
            Err(WalError::Keys(KeyError::Missing { .. }))
        ));
        read.unlock(&keys).unwrap();
        assert_eq!(read, written);
        assert_eq!(read.encode(), stored, "stored again as it was");

        let plain = record("b");
        assert!(holds(&plain.encode(), "confidential"));
        let mut read = Entry::decode(&plain.encode()).unwrap();
        read.unlock(&keys).unwrap();
        assert_eq!(read.body(), &flush("b"));
    }

    /// A flush record is not stored plain when its tenant's key is
    /// missing, and a sealed one moved to another place does not open.
    #[test]
    fn a_flush_record_fails_rather_than_go_plain_or_open_elsewhere() {
        let lost = Keys::new(Keyring::default(), Arc::new(Only("a")));
        let refused = Entry::record(
            WriterId::from_u128(1),
            Seq::new(3),
            id(),
            Timestamp::UNIX_EPOCH,
            flush("a"),
            &lost,
        );
        assert!(matches!(
            refused,
            Err(WalError::Keys(KeyError::Missing { .. }))
        ));

        let keys = keys_for("a");
        let at = |seq| {
            Entry::record(
                WriterId::from_u128(1),
                Seq::new(seq),
                id(),
                Timestamp::UNIX_EPOCH,
                flush("a"),
                &keys,
            )
            .unwrap()
        };
        // The block of entry 3 under the header of entry 4.
        let moved = Entry {
            blocks: at(3).blocks,
            ..at(4)
        };
        let mut read = Entry::decode(&moved.encode()).unwrap();
        assert!(matches!(
            read.unlock(&keys),
            Err(WalError::Keys(KeyError::Sealed { .. }))
        ));
    }

    /// SE-2: a keyed tenant's block is sealed under its key and named
    /// for it; the writer reads what it sealed, a reader must unlock
    /// the entry with the key, and plain blocks need no key.
    #[test]
    fn blocks_of_a_keyed_tenant_are_sealed_and_open_only_with_the_key() {
        let keys = keys_for("a");
        let batches = [logs("a", "prod", 0, 3), logs("b", "prod", 10, 2)];
        let entry = Entry::segment(
            WriterId::from_u128(1),
            Seq::new(1),
            id(),
            Timestamp::UNIX_EPOCH,
            &batches,
            &keys,
        )
        .unwrap();
        let metas = entry.block_metas();
        assert_eq!(metas[0].key.as_deref(), Some("k"));
        assert_eq!(metas[1].key, None);
        assert_eq!(entry.header().format, SEALED_FORMAT);
        assert_eq!(entry.block(0).unwrap().len(), 3, "the writer reads its own");
        let stored = entry.encode();
        let mut decoded = Entry::decode(&stored).unwrap();
        assert_eq!(decoded.block(1).unwrap().len(), 2, "plain blocks open");
        assert!(matches!(decoded.block(0), Err(WalError::Sealed { .. })));
        assert!(matches!(
            decoded.clone().unlock(&Keys::none()),
            Err(WalError::Keys(KeyError::Missing { .. }))
        ));
        decoded.unlock(&keys).unwrap();
        assert_eq!(decoded, entry);
        assert_eq!(
            decoded.block(0).unwrap().rows(),
            entry.block(0).unwrap().rows()
        );
    }

    /// SE-2: a sealed block is bound to its entry and position.
    #[test]
    fn a_sealed_block_does_not_open_at_another_position() {
        let keys = keys_for("a");
        let make = |seq| {
            Entry::segment(
                WriterId::from_u128(1),
                Seq::new(seq),
                id(),
                Timestamp::UNIX_EPOCH,
                &[logs("a", "prod", 0, 3)],
                &keys,
            )
            .unwrap()
        };
        let moved = Entry {
            header: make(2).header,
            blocks: make(1).blocks,
            opened: BTreeMap::new(),
            flush_key: None,
        };
        let mut moved = Entry::decode(&moved.encode()).unwrap();
        assert!(matches!(
            moved.unlock(&keys),
            Err(WalError::Keys(KeyError::Sealed { .. }))
        ));
    }

    /// SE-2: a tenant whose key the keyring lacks is not written plain.
    #[test]
    fn a_tenant_whose_key_is_missing_is_not_written() {
        let keys = Keys::new(Keyring::default(), Arc::new(Only("a")));
        let refused = Entry::segment(
            WriterId::from_u128(1),
            Seq::new(1),
            id(),
            Timestamp::UNIX_EPOCH,
            &[logs("a", "prod", 0, 3)],
            &keys,
        );
        assert!(matches!(
            refused,
            Err(WalError::Keys(KeyError::Missing { .. }))
        ));
        assert_eq!(
            Entry::segment(
                WriterId::from_u128(1),
                Seq::new(1),
                id(),
                Timestamp::UNIX_EPOCH,
                &[logs("b", "prod", 0, 3)],
                &keys,
            )
            .unwrap()
            .header()
            .format,
            PLAIN_FORMAT,
            "an unkeyed tenant is unaffected"
        );
    }

    #[test]
    fn a_segment_round_trips_with_one_block_per_tenant_dataset_and_kind() {
        let batches = [
            logs("a", "prod", 0, 3),
            logs("b", "prod", 10, 2),
            logs("a", "prod", 20, 1),
        ];
        let entry = Entry::segment(
            WriterId::from_u128(1),
            Seq::new(1),
            id(),
            Timestamp::UNIX_EPOCH,
            &batches,
            &Keys::none(),
        )
        .unwrap();
        let decoded = Entry::decode(&entry.encode()).unwrap();
        assert_eq!(decoded, entry);
        let metas = decoded.block_metas();
        assert_eq!(
            metas.len(),
            2,
            "batches of one tenant, dataset and kind merge"
        );
        assert_eq!((metas[0].tenant.as_str(), metas[0].rows), ("a", 4));
        assert_eq!(
            (metas[0].min_observed, metas[0].max_observed),
            (Timestamp::from_nanos(100), Timestamp::from_nanos(120))
        );
        assert_eq!(
            (metas[0].min_timestamp, metas[0].max_timestamp),
            (Timestamp::from_nanos(0), Timestamp::from_nanos(20))
        );
        assert_eq!(metas[1].offset, metas[0].length);
        let a = decoded
            .blocks_for(
                &TenantId::new("a").unwrap(),
                &DatasetName::new("prod").unwrap(),
                Kind::Log,
            )
            .unwrap();
        assert_eq!(a.len(), 1);
        assert_eq!(a[0].len(), 4);
        assert_eq!(
            a[0].time_bounds(),
            (Timestamp::from_nanos(0), Timestamp::from_nanos(20))
        );
        let none = decoded
            .blocks_for(
                &TenantId::new("a").unwrap(),
                &DatasetName::new("prod").unwrap(),
                Kind::Span,
            )
            .unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn entries_without_blocks_round_trip() {
        for body in [
            EntryBody::Seal {
                by: NodeId::from_u128(9),
            },
            EntryBody::End,
        ] {
            let entry = Entry::new(
                WriterId::from_u128(1),
                Seq::new(5),
                id(),
                Timestamp::from_nanos(7),
                body,
            );
            let decoded = Entry::decode(&entry.encode()).unwrap();
            assert_eq!(decoded, entry);
            assert!(decoded.block_metas().is_empty());
            assert!(matches!(decoded.block(0), Err(WalError::NoSuchBlock(0))));
        }
    }

    #[test]
    fn an_empty_segment_is_rejected() {
        assert!(matches!(
            Entry::segment(
                WriterId::from_u128(1),
                Seq::new(1),
                id(),
                Timestamp::UNIX_EPOCH,
                &[],
                &Keys::none()
            ),
            Err(WalError::EmptySegment)
        ));
    }

    #[test]
    fn garbage_and_newer_formats_are_reported() {
        assert!(matches!(
            Entry::decode(&Bytes::from_static(b"short")),
            Err(WalError::Unreadable(_))
        ));
        assert!(matches!(
            Entry::decode(&Bytes::from_static(b"xxxxxxxxTWL1")),
            Err(WalError::Unreadable(_))
        ));
        let mut entry = Entry::new(
            WriterId::from_u128(1),
            Seq::new(1),
            id(),
            Timestamp::UNIX_EPOCH,
            EntryBody::End,
        );
        entry.header.format = FORMAT + 1;
        assert!(matches!(
            Entry::decode(&entry.encode()),
            Err(WalError::UnsupportedFormat { .. })
        ));
    }

    #[test]
    fn blocks_are_compressed() {
        let batch = logs("a", "prod", 0, 2_000);
        let entry = Entry::segment(
            WriterId::from_u128(1),
            Seq::new(1),
            id(),
            Timestamp::UNIX_EPOCH,
            std::slice::from_ref(&batch),
            &Keys::none(),
        )
        .unwrap();
        let raw = batch.rows().get_array_memory_size();
        assert!(
            entry.encode().len() * 2 < raw,
            "{} bytes for {raw} in memory",
            entry.encode().len()
        );
    }

    #[test]
    fn block_metadata_without_event_bounds_reads_as_open() {
        let json = r#"{"tenant":"a","dataset":"prod","kind":"log","offset":0,"length":1,"rows":1,"min_observed":5,"max_observed":6}"#;
        let meta: BlockMeta = serde_json::from_str(json).unwrap();
        assert_eq!(meta.min_timestamp, Timestamp::MIN);
        assert_eq!(meta.max_timestamp, Timestamp::MAX);
    }
}
