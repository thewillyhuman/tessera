// SPDX-License-Identifier: Apache-2.0

//! Loki push over protobuf (section 16): decoding a snappy-compressed
//! `logproto.PushRequest`, the form promtail and Alloy send by default,
//! into the streams the JSON form carries: a label set, and entries of
//! a time, a line and structured metadata.

use std::collections::BTreeMap;

use prost::Message;
use tessera_types::Timestamp;

/// A push request (`logproto.PushRequest`).
#[derive(Clone, PartialEq, Message)]
pub struct PushRequest {
    /// The streams.
    #[prost(message, repeated, tag = "1")]
    pub streams: Vec<StreamAdapter>,
}

/// One stream of a request.
#[derive(Clone, PartialEq, Message)]
pub struct StreamAdapter {
    /// The labels, as a selector such as `{app="checkout"}`.
    #[prost(string, tag = "1")]
    pub labels: String,
    /// The entries.
    #[prost(message, repeated, tag = "2")]
    pub entries: Vec<EntryAdapter>,
    /// The labels' hash, which the sender may leave out.
    #[prost(uint64, tag = "3")]
    pub hash: u64,
}

/// One entry of a stream.
#[derive(Clone, PartialEq, Message)]
pub struct EntryAdapter {
    /// When the line was logged.
    #[prost(message, optional, tag = "1")]
    pub timestamp: Option<ProtoTimestamp>,
    /// The line.
    #[prost(string, tag = "2")]
    pub line: String,
    /// The structured metadata.
    #[prost(message, repeated, tag = "3")]
    pub structured_metadata: Vec<LabelPairAdapter>,
}

/// `google.protobuf.Timestamp`.
#[derive(Clone, Copy, PartialEq, Message)]
pub struct ProtoTimestamp {
    /// Seconds since the epoch.
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    /// Nanoseconds within the second.
    #[prost(int32, tag = "2")]
    pub nanos: i32,
}

/// A label pair.
#[derive(Clone, PartialEq, Message)]
pub struct LabelPairAdapter {
    /// The name.
    #[prost(string, tag = "1")]
    pub name: String,
    /// The value.
    #[prost(string, tag = "2")]
    pub value: String,
}

/// A stream to store, whatever form it was pushed in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stream {
    /// The stream's labels.
    pub labels: BTreeMap<String, String>,
    /// Its entries.
    pub entries: Vec<Entry>,
}

/// One entry of a stream to store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// When the line was logged; the receiver's clock when absent.
    pub timestamp: Option<Timestamp>,
    /// The line.
    pub line: String,
    /// The structured metadata.
    pub metadata: BTreeMap<String, String>,
}

/// Why a push body could not be decoded.
#[derive(Debug, thiserror::Error)]
pub enum PushError {
    /// The body is not snappy-compressed.
    #[error("malformed snappy body: {0}")]
    Snappy(#[from] snap::Error),
    /// The body is not a push request.
    #[error("malformed push protobuf: {0}")]
    Protobuf(#[from] prost::DecodeError),
    /// A stream's labels are not a selector.
    #[error("stream labels {0:?} are not a selector such as {{app=\"x\"}}")]
    Labels(String),
}

/// Decodes a snappy-compressed `PushRequest` into its streams.
///
/// # Errors
///
/// If the body is not snappy, not a push request, or a stream's
/// labels are not a selector.
pub fn decode(body: &[u8]) -> Result<Vec<Stream>, PushError> {
    let bytes = snap::raw::Decoder::new().decompress_vec(body)?;
    let request = PushRequest::decode(bytes.as_slice())?;
    request
        .streams
        .into_iter()
        .map(|stream| {
            let labels = parse_labels(&stream.labels).ok_or(PushError::Labels(stream.labels))?;
            let entries = stream
                .entries
                .into_iter()
                .map(|entry| Entry {
                    timestamp: entry.timestamp.map(|t| {
                        Timestamp::from_nanos(
                            t.seconds
                                .saturating_mul(1_000_000_000)
                                .saturating_add(i64::from(t.nanos)),
                        )
                    }),
                    line: entry.line,
                    metadata: entry
                        .structured_metadata
                        .into_iter()
                        .map(|pair| (pair.name, pair.value))
                        .collect(),
                })
                .collect();
            Ok(Stream { labels, entries })
        })
        .collect()
}

/// The label set of a selector such as `{app="checkout", level="error"}`,
/// its values quoted as Go quotes them; `None` when the text is not
/// one.
#[must_use]
pub fn parse_labels(text: &str) -> Option<BTreeMap<String, String>> {
    let inner = text.trim().strip_prefix('{')?.strip_suffix('}')?;
    let mut labels = BTreeMap::new();
    let mut rest = inner.trim_start();
    while !rest.is_empty() {
        let equals = rest.find('=')?;
        let name = rest[..equals].trim();
        let valid = name
            .chars()
            .enumerate()
            .all(|(i, c)| c == '_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()));
        if name.is_empty() || !valid {
            return None;
        }
        let (value, after) = quoted(rest[equals + 1..].trim_start())?;
        labels.insert(name.to_owned(), value);
        rest = after.trim_start();
        match rest.strip_prefix(',') {
            Some(more) => rest = more.trim_start(),
            None if rest.is_empty() => {}
            None => return None,
        }
    }
    Some(labels)
}

/// The quoted string at the start of `text`, Go's escapes undone, and
/// what follows its closing quote.
fn quoted(text: &str) -> Option<(String, &str)> {
    let body = text.strip_prefix('"')?;
    let mut value = String::new();
    let mut chars = body.char_indices();
    while let Some((i, c)) = chars.next() {
        match c {
            '"' => return Some((value, &body[i + 1..])),
            '\\' => {
                let (_, escaped) = chars.next()?;
                value.push(match escaped {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    other => other,
                });
            }
            other => value.push(other),
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selectors_parse_with_quoted_values() {
        let labels =
            parse_labels(r#"{app="checkout", level="error", msg="say \"hi\"\n"}"#).unwrap();
        assert_eq!(labels["app"], "checkout");
        assert_eq!(labels["level"], "error");
        assert_eq!(labels["msg"], "say \"hi\"\n");
        assert_eq!(parse_labels("{}").unwrap().len(), 0);
        assert_eq!(parse_labels("{ }").unwrap().len(), 0);
        assert!(parse_labels(r#"app="checkout""#).is_none(), "no braces");
        assert!(parse_labels(r"{app=checkout}").is_none(), "unquoted");
        assert!(parse_labels(r#"{1app="x"}"#).is_none(), "bad name");
        assert!(parse_labels(r#"{app="x" level="y"}"#).is_none(), "no comma");
        assert!(parse_labels(r#"{app="x}"#).is_none(), "unterminated");
    }

    #[test]
    fn a_compressed_request_decodes_into_streams() {
        let request = PushRequest {
            streams: vec![StreamAdapter {
                labels: r#"{app="checkout"}"#.into(),
                entries: vec![
                    EntryAdapter {
                        timestamp: Some(ProtoTimestamp {
                            seconds: 1_700_000_000,
                            nanos: 5,
                        }),
                        line: "payment timeout".into(),
                        structured_metadata: vec![LabelPairAdapter {
                            name: "trace_id".into(),
                            value: "abc".into(),
                        }],
                    },
                    EntryAdapter {
                        timestamp: None,
                        line: "no time".into(),
                        structured_metadata: Vec::new(),
                    },
                ],
                hash: 7,
            }],
        };
        let body = snap::raw::Encoder::new()
            .compress_vec(&request.encode_to_vec())
            .unwrap();
        let streams = decode(&body).unwrap();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].labels["app"], "checkout");
        assert_eq!(
            streams[0].entries[0],
            Entry {
                timestamp: Some(Timestamp::from_nanos(1_700_000_000_000_000_005)),
                line: "payment timeout".into(),
                metadata: BTreeMap::from([("trace_id".to_owned(), "abc".to_owned())]),
            }
        );
        assert_eq!(streams[0].entries[1].timestamp, None);
        assert!(matches!(decode(b"not snappy"), Err(PushError::Snappy(_))));
        let bad = PushRequest {
            streams: vec![StreamAdapter {
                labels: "app=checkout".into(),
                entries: Vec::new(),
                hash: 0,
            }],
        };
        let body = snap::raw::Encoder::new()
            .compress_vec(&bad.encode_to_vec())
            .unwrap();
        assert!(matches!(decode(&body), Err(PushError::Labels(_))));
    }
}
