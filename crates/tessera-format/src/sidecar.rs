// SPDX-License-Identifier: Apache-2.0

//! The sidecar index file (ST-14): secondary indexes of one data file in
//! a Tessera-specific format. A data file stays readable without it.
//!
//! Three indexes (section 8.4):
//!
//! - **term**: each token of a text column, with the row groups it
//!   appears in (QL-16);
//! - **trigram**: each trigram of a text column, with its row groups
//!   (QL-17);
//! - **Bloom**: a filter over a column's values, for the whole file.
//!
//! Postings are at row-group granularity: an index narrows which row
//! groups a scan decodes, and the exact predicate runs on the rows.
//!
//! Layout, little-endian: magic `TSC1`, the row-group count, then three
//! length-prefixed sections. A postings section is a count followed by
//! `(u16 length, bytes, u64 words...)` entries in sorted order, each with
//! `ceil(row_groups / 64)` words. The Bloom section is a count of
//! columns followed by `(u16 length, name, u8 hashes, u32 words, u64
//! words...)` entries.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, StringArray};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tessera_types::{Keys, NamedKey, stable_hash};

use crate::FormatError;
use crate::text::{tokenize, tokens_into, trigrams, trigrams_into};

const MAGIC: &[u8; 4] = b"TSC1";
/// The magic of a sidecar sealed under a tenant key (SE-2): the key's
/// name and the sealed encoding follow.
const SEALED_MAGIC: &[u8; 4] = b"TSX1";
/// What a sealed sidecar is bound to.
const SEALED_AAD: &[u8] = b"tessera sidecar";

/// The row groups a token or trigram appears in, as a bitmap.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct RowGroups(Vec<u64>);

impl RowGroups {
    fn with_capacity(row_groups: usize) -> Self {
        RowGroups(vec![0; row_groups.div_ceil(64)])
    }

    fn set(&mut self, row_group: usize) {
        if let Some(word) = self.0.get_mut(row_group / 64) {
            *word |= 1 << (row_group % 64);
        }
    }

    /// Whether `row_group` is in the set.
    #[must_use]
    pub fn contains(&self, row_group: usize) -> bool {
        self.0
            .get(row_group / 64)
            .is_some_and(|word| word & (1 << (row_group % 64)) != 0)
    }

    /// The row groups in the set, ascending.
    #[must_use]
    pub fn to_vec(&self) -> Vec<usize> {
        (0..self.0.len() * 64)
            .filter(|i| self.contains(*i))
            .collect()
    }

    /// A set of the given row groups out of `row_groups`.
    #[must_use]
    pub fn from_indices(row_groups: usize, indices: &[usize]) -> Self {
        let mut set = RowGroups::with_capacity(row_groups);
        for index in indices {
            set.set(*index);
        }
        set
    }

    /// Whether no row group is in the set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|w| *w == 0)
    }

    /// The row groups in both sets.
    #[must_use]
    pub fn intersect(&self, other: &RowGroups) -> RowGroups {
        RowGroups(self.0.iter().zip(&other.0).map(|(a, b)| a & b).collect())
    }

    fn union(&self, other: &RowGroups) -> RowGroups {
        RowGroups(self.0.iter().zip(&other.0).map(|(a, b)| a | b).collect())
    }

    fn all(row_groups: usize) -> RowGroups {
        let mut set = RowGroups::with_capacity(row_groups);
        for i in 0..row_groups {
            set.set(i);
        }
        set
    }
}

/// A Bloom filter over the values of one column, for the whole file.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Bloom {
    hashes: u8,
    words: Vec<u64>,
}

impl Bloom {
    /// A filter sized for `expected` values at about one false positive
    /// in a hundred.
    #[must_use]
    pub fn new(expected: usize) -> Self {
        let bits = (expected.max(1) * 10).next_power_of_two().max(64);
        Bloom {
            hashes: 7,
            words: vec![0; bits / 64],
        }
    }

    fn positions(&self, value: &[u8]) -> impl Iterator<Item = usize> + '_ {
        let h1 = stable_hash(value);
        let h2 = stable_hash(&h1.to_le_bytes()) | 1;
        let bits = self.words.len() as u64 * 64;
        (0..u64::from(self.hashes)).map(move |i| {
            let position = h1.wrapping_add(i.wrapping_mul(h2)) % bits;
            usize::try_from(position).unwrap_or(0)
        })
    }

    /// Adds `value`.
    pub fn insert(&mut self, value: &[u8]) {
        let positions: Vec<usize> = self.positions(value).collect();
        for p in positions {
            self.words[p / 64] |= 1 << (p % 64);
        }
    }

    /// Whether `value` may have been added; false means certainly not.
    #[must_use]
    pub fn may_contain(&self, value: &[u8]) -> bool {
        self.positions(value)
            .all(|p| self.words[p / 64] & (1 << (p % 64)) != 0)
    }
}

/// The indexes of one data file.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Sidecar {
    row_groups: usize,
    terms: BTreeMap<String, BTreeMap<String, RowGroups>>,
    trigrams: BTreeMap<String, BTreeMap<String, RowGroups>>,
    blooms: BTreeMap<String, Bloom>,
}

/// Which indexes to build, by column.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct IndexSpec {
    /// Text columns to index by token.
    pub term: Vec<String>,
    /// Text columns to index by trigram.
    pub trigram: Vec<String>,
    /// Columns to cover with a Bloom filter.
    pub bloom: Vec<String>,
}

impl Sidecar {
    /// Builds the indexes of `spec` over `row_groups`, one array per row
    /// group per column, given by `column(name, row_group)`.
    ///
    /// # Errors
    ///
    /// `Unreadable` if an indexed column has a type the index cannot
    /// cover: text for term and trigram indexes, text or fixed binary for
    /// Bloom filters.
    pub fn build(
        spec: &IndexSpec,
        row_groups: usize,
        column: impl Fn(&str, usize) -> Option<ArrayRef>,
    ) -> Result<Self, FormatError> {
        let mut sidecar = Sidecar {
            row_groups,
            ..Sidecar::default()
        };
        for name in &spec.term {
            let mut postings: BTreeMap<String, RowGroups> = BTreeMap::new();
            for rg in 0..row_groups {
                // The row group's tokens once, whatever the rows that
                // repeat them.
                let mut tokens = HashSet::new();
                for text in strings(column(name, rg).as_ref(), name)? {
                    tokens_into(text, &mut tokens);
                }
                for token in tokens {
                    postings
                        .entry(token)
                        .or_insert_with(|| RowGroups::with_capacity(row_groups))
                        .set(rg);
                }
            }
            sidecar.terms.insert(name.clone(), postings);
        }
        for name in &spec.trigram {
            let mut postings: BTreeMap<String, RowGroups> = BTreeMap::new();
            for rg in 0..row_groups {
                let mut found = HashSet::new();
                for text in strings(column(name, rg).as_ref(), name)? {
                    trigrams_into(text, &mut found);
                }
                for trigram in found {
                    postings
                        .entry(trigram)
                        .or_insert_with(|| RowGroups::with_capacity(row_groups))
                        .set(rg);
                }
            }
            sidecar.trigrams.insert(name.clone(), postings);
        }
        for name in &spec.bloom {
            let mut values: Vec<Vec<u8>> = Vec::new();
            for rg in 0..row_groups {
                values.extend(bytes_of(column(name, rg).as_ref(), name)?);
            }
            let mut bloom = Bloom::new(values.len());
            for value in &values {
                bloom.insert(value);
            }
            sidecar.blooms.insert(name.clone(), bloom);
        }
        Ok(sidecar)
    }

    /// How many row groups the file has.
    #[must_use]
    pub fn row_groups(&self) -> usize {
        self.row_groups
    }

    /// The row groups that may hold a row of `column` containing every
    /// token of `text`, or `None` when the column has no term index.
    /// Text without tokens matches every row group.
    #[must_use]
    pub fn row_groups_with_tokens(&self, column: &str, text: &str) -> Option<RowGroups> {
        let postings = self.terms.get(column)?;
        Some(self.covering(postings, tokenize(text)))
    }

    /// The row groups that may hold a row of `column` containing `text`
    /// as a substring, or `None` when the column has no trigram index.
    /// Text shorter than three characters matches every row group.
    #[must_use]
    pub fn row_groups_with_substring(&self, column: &str, text: &str) -> Option<RowGroups> {
        let postings = self.trigrams.get(column)?;
        Some(self.covering(postings, trigrams(text)))
    }

    /// Whether the file may hold `value` in `column`; `None` when the
    /// column has no Bloom filter.
    #[must_use]
    pub fn may_contain(&self, column: &str, value: &[u8]) -> Option<bool> {
        self.blooms.get(column).map(|b| b.may_contain(value))
    }

    fn covering(
        &self,
        postings: &BTreeMap<String, RowGroups>,
        keys: BTreeSet<String>,
    ) -> RowGroups {
        let mut result = RowGroups::all(self.row_groups);
        for key in keys {
            match postings.get(&key) {
                Some(set) => result = result.intersect(set),
                None => return RowGroups::with_capacity(self.row_groups),
            }
        }
        result
    }

    /// The row groups matching any of `alternatives` (each a conjunction
    /// of substrings), for regular expressions reduced to literals.
    #[must_use]
    pub fn row_groups_with_any_substring(
        &self,
        column: &str,
        alternatives: &[Vec<String>],
    ) -> Option<RowGroups> {
        let postings = self.trigrams.get(column)?;
        let mut result = RowGroups::with_capacity(self.row_groups);
        for conjunction in alternatives {
            let mut rows = RowGroups::all(self.row_groups);
            for literal in conjunction {
                rows = rows.intersect(&self.covering(postings, trigrams(literal)));
            }
            result = result.union(&rows);
        }
        Some(result)
    }

    /// Encodes the sidecar.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::new();
        out.put_slice(MAGIC);
        out.put_u32_le(u32::try_from(self.row_groups).unwrap_or(u32::MAX));
        for section in [&self.terms, &self.trigrams] {
            out.put_u32_le(u32::try_from(section.len()).unwrap_or(u32::MAX));
            for (column, postings) in section {
                put_str(&mut out, column);
                out.put_u32_le(u32::try_from(postings.len()).unwrap_or(u32::MAX));
                for (key, rows) in postings {
                    put_str(&mut out, key);
                    for word in &rows.0 {
                        out.put_u64_le(*word);
                    }
                }
            }
        }
        out.put_u32_le(u32::try_from(self.blooms.len()).unwrap_or(u32::MAX));
        for (column, bloom) in &self.blooms {
            put_str(&mut out, column);
            out.put_u8(bloom.hashes);
            out.put_u32_le(u32::try_from(bloom.words.len()).unwrap_or(u32::MAX));
            for word in &bloom.words {
                out.put_u64_le(*word);
            }
        }
        out.freeze()
    }

    /// `encoded`, a sidecar's bytes, sealed under `key` (SE-2).
    #[must_use]
    pub fn seal(encoded: &[u8], key: &NamedKey) -> Bytes {
        let mut out = BytesMut::with_capacity(encoded.len() + 64);
        out.put_slice(SEALED_MAGIC);
        put_str(&mut out, &key.name);
        out.put_slice(&key.key.seal(SEALED_AAD, encoded));
        out.freeze()
    }

    /// Decodes a sidecar, opening it with `keys` when it is sealed.
    ///
    /// # Errors
    ///
    /// `Keys` when its key is not held or does not open it;
    /// `Unreadable` if the bytes are not a sidecar this crate wrote.
    pub fn open(mut bytes: Bytes, keys: &Keys) -> Result<Self, FormatError> {
        if bytes.len() < 4 || &bytes[..4] != SEALED_MAGIC {
            return Self::decode(bytes);
        }
        bytes.advance(4);
        let name = get_str(&mut bytes)?;
        let plain = keys.open(&name, SEALED_AAD, &bytes)?;
        Self::decode(Bytes::from(plain))
    }

    /// Decodes a plain sidecar.
    ///
    /// # Errors
    ///
    /// `Unreadable` if the bytes are not a sidecar this crate wrote.
    pub fn decode(mut bytes: Bytes) -> Result<Self, FormatError> {
        let bad = |what: &str| FormatError::Unreadable(format!("sidecar: {what}"));
        if bytes.len() < 8 || &bytes[..4] != MAGIC {
            return Err(bad("missing magic"));
        }
        bytes.advance(4);
        let row_groups = bytes.get_u32_le() as usize;
        let words = row_groups.div_ceil(64);
        let mut sections = Vec::new();
        for _ in 0..2 {
            let columns = get_count(&mut bytes)?;
            let mut section = BTreeMap::new();
            for _ in 0..columns {
                let column = get_str(&mut bytes)?;
                let entries = get_count(&mut bytes)?;
                let mut postings = BTreeMap::new();
                for _ in 0..entries {
                    let key = get_str(&mut bytes)?;
                    if bytes.remaining() < words * 8 {
                        return Err(bad("truncated postings"));
                    }
                    let set: Vec<u64> = (0..words).map(|_| bytes.get_u64_le()).collect();
                    postings.insert(key, RowGroups(set));
                }
                section.insert(column, postings);
            }
            sections.push(section);
        }
        let trigram_section = sections.pop().unwrap_or_default();
        let term_section = sections.pop().unwrap_or_default();
        let columns = get_count(&mut bytes)?;
        let mut blooms = BTreeMap::new();
        for _ in 0..columns {
            let column = get_str(&mut bytes)?;
            if bytes.remaining() < 5 {
                return Err(bad("truncated bloom"));
            }
            let hashes = bytes.get_u8();
            let count = bytes.get_u32_le() as usize;
            if bytes.remaining() < count * 8 {
                return Err(bad("truncated bloom"));
            }
            let words: Vec<u64> = (0..count).map(|_| bytes.get_u64_le()).collect();
            blooms.insert(column, Bloom { hashes, words });
        }
        Ok(Sidecar {
            row_groups,
            terms: term_section,
            trigrams: trigram_section,
            blooms,
        })
    }
}

fn put_str(out: &mut BytesMut, text: &str) {
    out.put_u16_le(u16::try_from(text.len()).unwrap_or(u16::MAX));
    out.put_slice(&text.as_bytes()[..text.len().min(usize::from(u16::MAX))]);
}

fn get_count(bytes: &mut Bytes) -> Result<usize, FormatError> {
    if bytes.remaining() < 4 {
        return Err(FormatError::Unreadable("sidecar: truncated".into()));
    }
    Ok(bytes.get_u32_le() as usize)
}

fn get_str(bytes: &mut Bytes) -> Result<String, FormatError> {
    if bytes.remaining() < 2 {
        return Err(FormatError::Unreadable("sidecar: truncated".into()));
    }
    let len = usize::from(bytes.get_u16_le());
    if bytes.remaining() < len {
        return Err(FormatError::Unreadable("sidecar: truncated".into()));
    }
    let text = bytes.split_to(len);
    String::from_utf8(text.to_vec())
        .map_err(|_| FormatError::Unreadable("sidecar: text is not utf-8".into()))
}

/// The non-null strings of a text column.
fn strings<'a>(array: Option<&'a ArrayRef>, name: &str) -> Result<Vec<&'a str>, FormatError> {
    let Some(array) = array else {
        return Ok(Vec::new());
    };
    let strings = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| FormatError::Unreadable(format!("column {name} is not text")))?;
    Ok(strings.iter().flatten().collect())
}

/// The non-null values of a text or fixed binary column as bytes.
fn bytes_of(array: Option<&ArrayRef>, name: &str) -> Result<Vec<Vec<u8>>, FormatError> {
    let Some(array) = array else {
        return Ok(Vec::new());
    };
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(strings
            .iter()
            .flatten()
            .map(|s| s.as_bytes().to_vec())
            .collect());
    }
    if let Some(binary) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        return Ok((0..binary.len())
            .filter(|i| !binary.is_null(*i))
            .map(|i| binary.value(i).to_vec())
            .collect());
    }
    Err(FormatError::Unreadable(format!(
        "column {name} is neither text nor fixed binary"
    )))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn text(values: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    fn sidecar() -> Sidecar {
        let groups: Vec<ArrayRef> = vec![
            text(&["connection reset by peer", "GET /checkout ok"]),
            text(&["timeout talking to payments", "retrying checkout"]),
            text(&["all good"]),
        ];
        let ids: Vec<ArrayRef> = (0..3u8)
            .map(|rg| {
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(
                        vec![vec![rg; 16], vec![rg + 10; 16]].into_iter(),
                    )
                    .unwrap(),
                ) as ArrayRef
            })
            .collect();
        Sidecar::build(
            &IndexSpec {
                term: vec!["body".into()],
                trigram: vec!["body".into()],
                bloom: vec!["trace_id".into()],
            },
            3,
            |name, rg| match name {
                "body" => Some(Arc::clone(&groups[rg])),
                "trace_id" => Some(Arc::clone(&ids[rg])),
                _ => None,
            },
        )
        .unwrap()
    }

    #[test]
    fn term_index_finds_row_groups_with_every_token() {
        let sidecar = sidecar();
        assert_eq!(
            sidecar
                .row_groups_with_tokens("body", "checkout")
                .unwrap()
                .to_vec(),
            vec![0, 1]
        );
        assert_eq!(
            sidecar
                .row_groups_with_tokens("body", "Checkout retrying")
                .unwrap()
                .to_vec(),
            vec![1]
        );
        assert!(
            sidecar
                .row_groups_with_tokens("body", "absent")
                .unwrap()
                .to_vec()
                .is_empty()
        );
        assert_eq!(
            sidecar
                .row_groups_with_tokens("body", "...")
                .unwrap()
                .to_vec(),
            vec![0, 1, 2]
        );
        assert!(sidecar.row_groups_with_tokens("other", "x").is_none());
    }

    #[test]
    fn trigram_index_finds_substrings_and_alternatives() {
        let sidecar = sidecar();
        assert_eq!(
            sidecar
                .row_groups_with_substring("body", "ECKOUT")
                .unwrap()
                .to_vec(),
            vec![0, 1]
        );
        assert_eq!(
            sidecar
                .row_groups_with_substring("body", "payments")
                .unwrap()
                .to_vec(),
            vec![1]
        );
        assert_eq!(
            sidecar
                .row_groups_with_substring("body", "ok")
                .unwrap()
                .to_vec(),
            vec![0, 1, 2]
        );
        let either = sidecar
            .row_groups_with_any_substring("body", &[vec!["peer".into()], vec!["good".into()]])
            .unwrap();
        assert_eq!(either.to_vec(), vec![0, 2]);
    }

    #[test]
    fn bloom_filter_excludes_absent_values() {
        let sidecar = sidecar();
        assert_eq!(sidecar.may_contain("trace_id", &[1u8; 16]), Some(true));
        assert_eq!(sidecar.may_contain("trace_id", &[11u8; 16]), Some(true));
        assert_eq!(sidecar.may_contain("trace_id", &[99u8; 16]), Some(false));
        assert_eq!(sidecar.may_contain("body", b"x"), None);
    }

    #[test]
    fn round_trips_through_the_binary_format() {
        let sidecar = sidecar();
        let bytes = sidecar.encode();
        assert_eq!(&bytes[..4], b"TSC1");
        let decoded = Sidecar::decode(bytes).unwrap();
        assert_eq!(decoded, sidecar);
        assert!(Sidecar::decode(Bytes::from_static(b"nope")).is_err());
        assert!(
            Sidecar::decode(Bytes::from_static(b"TSC1\x03\x00\x00\x00\x01\x00\x00\x00")).is_err()
        );
    }

    #[test]
    fn bloom_false_positive_rate_is_low() {
        let mut bloom = Bloom::new(1_000);
        for i in 0..1_000u32 {
            bloom.insert(&i.to_le_bytes());
        }
        let false_positives = (1_000..11_000u32)
            .filter(|i| bloom.may_contain(&i.to_le_bytes()))
            .count();
        assert!(false_positives < 300, "{false_positives} of 10000");
        assert!((0..1_000u32).all(|i| bloom.may_contain(&i.to_le_bytes())));
    }

    #[test]
    fn rejects_columns_of_the_wrong_type() {
        let numbers: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1]));
        let result = Sidecar::build(
            &IndexSpec {
                term: vec!["n".into()],
                ..IndexSpec::default()
            },
            1,
            |_, _| Some(Arc::clone(&numbers)),
        );
        assert!(matches!(result, Err(FormatError::Unreadable(_))));
    }
}
