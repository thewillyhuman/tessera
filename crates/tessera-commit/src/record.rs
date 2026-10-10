// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The commit object: what is written at `_log/{version}`.
//!
//! Commit objects carry a format version (OP-7) and are encoded as JSON so
//! that an operator can read one with no tooling. The encoding is private
//! to this module; nothing else parses commit objects.

use std::fmt;
use std::str::FromStr;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tessera_types::{Id128, InvalidId, NodeId, Random, Timestamp, Version};

use crate::Action;

/// The format version of commit objects this release writes. A node
/// refuses to read a newer format (OP-7).
pub const FORMAT: u32 = 1;

/// The identity of one commit attempt. It lets a committer recognise its
/// own commit after a lost response.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CommitId(Id128);

impl CommitId {
    /// Wraps an existing value.
    #[must_use]
    pub const fn from_u128(value: u128) -> Self {
        CommitId(Id128::from_u128(value))
    }

    /// A fresh identity from 128 random bits.
    pub fn random(random: &mut impl Random) -> Self {
        CommitId(Id128::random(random))
    }
}

impl fmt::Display for CommitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for CommitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CommitId({})", self.0)
    }
}

impl FromStr for CommitId {
    type Err = InvalidId;

    fn from_str(value: &str) -> Result<Self, InvalidId> {
        value.parse().map(CommitId)
    }
}

/// One commit object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    /// The format this object was written in.
    pub format: u32,
    /// The version this commit established.
    pub version: Version,
    /// The identity of the commit attempt.
    pub id: CommitId,
    /// The node that committed.
    pub committer: NodeId,
    /// When the committer wrote the object, by its own clock.
    pub committed_at: Timestamp,
    /// What the commit does, in order.
    pub actions: Vec<Action>,
}

/// Why bytes are not a readable commit object.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// The bytes are not a well-formed commit object.
    #[error("malformed commit object: {0}")]
    Malformed(#[from] serde_json::Error),
    /// The object was written by a newer release.
    #[error("commit object format {format} is newer than the supported format {supported}")]
    UnsupportedFormat {
        /// The format found in the object.
        format: u32,
        /// The newest format this release reads.
        supported: u32,
    },
}

impl Commit {
    /// The bytes written to the object store.
    ///
    /// # Panics
    ///
    /// Never for a well-formed commit: every field serializes to JSON
    /// without I/O, so a failure would be a bug in this crate.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        Bytes::from(serde_json::to_vec(self).expect("a commit always serializes"))
    }

    /// Parses an object written by [`encode`](Self::encode).
    ///
    /// # Errors
    ///
    /// `Malformed` for anything that is not a commit object, and
    /// `UnsupportedFormat` for one written by a newer release.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let commit: Commit = serde_json::from_slice(bytes)?;
        if commit.format > FORMAT {
            return Err(DecodeError::UnsupportedFormat {
                format: commit.format,
                supported: FORMAT,
            });
        }
        Ok(commit)
    }
}

#[cfg(test)]
mod tests {
    use tessera_types::{NodeId, SeededRandom, Timestamp, Version};

    use super::*;
    use crate::{Action, KvMutation};

    fn commit() -> Commit {
        Commit {
            format: FORMAT,
            version: Version::new(3),
            id: CommitId::random(&mut SeededRandom::new(1)),
            committer: NodeId::from_u128(7),
            committed_at: Timestamp::from_nanos(1_000),
            actions: vec![Action::Kv(KvMutation::Delete { key: "k".into() })],
        }
    }

    #[test]
    fn a_commit_round_trips_through_its_encoding() {
        let original = commit();
        let decoded = Commit::decode(&original.encode()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn a_newer_format_is_refused() {
        let mut newer = commit();
        newer.format = FORMAT + 1;
        let error = Commit::decode(&newer.encode()).unwrap_err();
        assert!(
            matches!(error, DecodeError::UnsupportedFormat { format, .. } if format == FORMAT + 1)
        );
    }

    #[test]
    fn garbage_is_reported_not_panicked() {
        assert!(matches!(
            Commit::decode(b"{not json"),
            Err(DecodeError::Malformed(_))
        ));
        assert!(matches!(
            Commit::decode(b""),
            Err(DecodeError::Malformed(_))
        ));
    }

    #[test]
    fn commit_ids_format_as_hex() {
        let id = CommitId::from_u128(0xff);
        assert_eq!(id.to_string(), "000000000000000000000000000000ff");
        assert_eq!(id.to_string().parse::<CommitId>().unwrap(), id);
    }
}
