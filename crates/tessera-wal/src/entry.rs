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
//! block.

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

/// One entry of a writer log.
#[derive(Clone, PartialEq, Debug)]
pub struct Entry {
    header: EntryHeader,
    /// The blocks as stored: sealed where their metadata names a key.
    blocks: Bytes,
    /// Sealed blocks that were opened, by index: by the writer that
    /// sealed them, or by [`Entry::unlock`].
    opened: BTreeMap<usize, Bytes>,
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
        }
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
        })
    }

    /// Opens every sealed block with `keys`, so that the blocks can be
    /// read; nothing to do for an entry without sealed blocks.
    ///
    /// # Errors
    ///
    /// `Keys` when a block's key is not in the keyring or does not open
    /// the block.
    pub fn unlock(&mut self, keys: &Keys) -> Result<(), WalError> {
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

    /// The bytes written to the object store.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let header = serde_json::to_vec(&self.header)
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
        let total = bytes.len();
        if total < TRAILER_LEN || &bytes[total - 4..] != MAGIC {
            return Err(WalError::Unreadable("missing trailer".into()));
        }
        let header_len = u32::from_le_bytes(
            bytes[total - TRAILER_LEN..total - 4]
                .try_into()
                .unwrap_or_else(|_| unreachable!("four bytes")),
        ) as usize;
        let header_start = total
            .checked_sub(TRAILER_LEN + header_len)
            .ok_or_else(|| WalError::Unreadable("header length exceeds object".into()))?;
        let header: EntryHeader = serde_json::from_slice(&bytes[header_start..total - TRAILER_LEN])
            .map_err(|e| WalError::Unreadable(format!("header: {e}")))?;
        if header.format > FORMAT {
            return Err(WalError::UnsupportedFormat {
                format: header.format,
                supported: FORMAT,
            });
        }
        Ok(Entry {
            header,
            blocks: bytes.slice(..header_start),
            opened: BTreeMap::new(),
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
        let seeds = [end.encode(), segment.encode()];
        let mut random = tessera_types::SeededRandom::new(5);
        for round in 0..1_500 {
            let input = tessera_types::mutated(&seeds[round % seeds.len()], &mut random);
            let _ = Entry::decode(&Bytes::from(input));
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
