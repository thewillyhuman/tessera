// SPDX-License-Identifier: LicenseRef-Tessera-Pending

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
//! and kind, compressed with LZ4.

use std::io::Cursor;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::ipc::CompressionType;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tessera_types::{
    DatasetName, EventBatch, FileMeta, Id128, Kind, NodeId, Random, Seq, TenantId, Timestamp,
    WriterId,
};

use crate::WalError;

/// The format version of entries this release writes (OP-7).
pub const FORMAT: u32 = 1;

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
    blocks: Bytes,
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
                format: FORMAT,
                writer,
                seq,
                id,
                written_at,
                body,
            },
            blocks: Bytes::new(),
        }
    }

    /// A segment holding `batches`, one block per tenant, dataset and kind.
    /// Batches of the same tenant, dataset and kind are merged into one
    /// block.
    ///
    /// # Errors
    ///
    /// `EmptySegment` if there are no batches; `Arrow` if a block cannot
    /// be encoded.
    pub fn segment(
        writer: WriterId,
        seq: Seq,
        id: EntryId,
        written_at: Timestamp,
        batches: &[EventBatch],
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
        for (first, rows) in groups {
            let offset = bytes.len() as u64;
            let merged = concat_batches(&first.rows().schema(), rows)?;
            let batch = EventBatch::new(
                first.tenant().clone(),
                first.dataset().clone(),
                first.kind(),
                merged,
            )?;
            bytes.extend_from_slice(&encode_block(batch.rows())?);
            let (min_observed, max_observed) = batch.observed_bounds();
            blocks.push(BlockMeta {
                tenant: batch.tenant().clone(),
                dataset: batch.dataset().clone(),
                kind: batch.kind(),
                offset,
                length: bytes.len() as u64 - offset,
                rows: batch.len() as u64,
                min_observed,
                max_observed,
            });
        }
        let body = EntryBody::Segment { blocks };
        Ok(Entry {
            header: EntryHeader {
                format: FORMAT,
                writer,
                seq,
                id,
                written_at,
                body,
            },
            blocks: bytes.freeze(),
        })
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
    /// `NoSuchBlock`, or `Arrow` or `Batch` if the block is corrupt.
    pub fn block(&self, index: usize) -> Result<EventBatch, WalError> {
        let meta = self
            .block_metas()
            .get(index)
            .ok_or(WalError::NoSuchBlock(index))?;
        let start = usize::try_from(meta.offset).map_err(|_| WalError::NoSuchBlock(index))?;
        let end =
            usize::try_from(meta.offset + meta.length).map_err(|_| WalError::NoSuchBlock(index))?;
        let slice = self
            .blocks
            .get(start..end)
            .ok_or(WalError::NoSuchBlock(index))?;
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
        })
    }
}

fn same_block(a: &EventBatch, b: &EventBatch) -> bool {
    a.tenant() == b.tenant() && a.dataset() == b.dataset() && a.kind() == b.kind()
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
    use tessera_types::{LogRow, SeededRandom};

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
                &[]
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
        )
        .unwrap();
        let raw = batch.rows().get_array_memory_size();
        assert!(
            entry.encode().len() * 2 < raw,
            "{} bytes for {raw} in memory",
            entry.encode().len()
        );
    }
}
