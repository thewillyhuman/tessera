// SPDX-License-Identifier: Apache-2.0

//! Deriving labels from text (`Extract`): JSON, logfmt, regular
//! expressions with named groups, patterns with `<name>` captures, and
//! `unpack`. The results of a stage merge into the `_extracted` column, a
//! JSON object of label name to text, later stages overriding earlier
//! ones.
//!
//! Label names follow QL-19: nested JSON properties flatten with `_` and
//! every name is sanitized, so a derived label can be referenced the way
//! a stored one is.
//!
//! A `json` or `logfmt` stage over a row whose body ingest already
//! parsed (DM-10) takes the labels from the stored `body.*` attributes
//! instead of parsing the line again (QL-10); a row without stored
//! fields is parsed.

use std::collections::BTreeMap;

use arrow::array::{Array, StringArray};
use regex::Regex;
use serde_json::{Map, Value};
use tessera_ir::ExtractStage;
use tessera_types::label_name;

use crate::KernelError;

/// What a stage produced for a batch.
#[derive(Debug)]
pub struct Extracted {
    /// The merged `_extracted` column.
    pub labels: StringArray,
    /// The new line, for stages that replace it (`unpack`).
    pub line: Option<StringArray>,
}

/// Runs `stage` over `text`, merging into `existing` labels; `stored`
/// is the rows' `_overflow`, whose `body.*` attributes serve a `json`
/// or `logfmt` stage instead of the line where they were parsed from
/// it in that stage's format (QL-10).
///
/// # Errors
///
/// `Unsupported` if a regular expression or pattern is malformed.
pub fn extract(
    stage: &ExtractStage,
    text: &StringArray,
    existing: Option<&StringArray>,
    stored: Option<&StringArray>,
) -> Result<Extracted, KernelError> {
    let parser = Parser::new(stage)?;
    let mut labels: Vec<Option<String>> = Vec::with_capacity(text.len());
    let mut lines: Vec<Option<String>> = Vec::with_capacity(text.len());
    for i in 0..text.len() {
        let mut merged: BTreeMap<String, String> = existing
            .filter(|e| !e.is_null(i))
            .and_then(|e| serde_json::from_str::<Map<String, Value>>(e.value(i)).ok())
            .map(|object| {
                object
                    .into_iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k, s.to_owned())))
                    .collect()
            })
            .unwrap_or_default();
        let mut line = None;
        let from_store = if !text.is_null(i) && reads_stored(stage, text.value(i)) {
            stored
                .filter(|s| !s.is_null(i))
                .map(|s| stored_labels(s.value(i)))
                .filter(|found| !found.is_empty())
        } else {
            None
        };
        if let Some(found) = from_store {
            merged.extend(found);
        } else if !text.is_null(i) {
            let (found, new_line) = parser.parse(text.value(i));
            merged.extend(found);
            line = new_line;
        }
        labels.push(if merged.is_empty() {
            None
        } else {
            serde_json::to_string(&merged).ok()
        });
        lines.push(line);
    }
    let line = if matches!(stage, ExtractStage::Unpack) {
        Some(StringArray::from(
            lines
                .into_iter()
                .enumerate()
                .map(|(i, l)| l.or_else(|| (!text.is_null(i)).then(|| text.value(i).to_owned())))
                .collect::<Vec<_>>(),
        ))
    } else {
        None
    };
    Ok(Extracted {
        labels: StringArray::from(labels),
        line,
    })
}

enum Parser {
    Json,
    Logfmt,
    Regexp(Regex),
    Pattern(Vec<Piece>),
    Unpack,
}

/// A piece of a pattern: literal text, a named capture, or a discard.
#[derive(Debug, PartialEq)]
enum Piece {
    Literal(String),
    Capture(String),
    Discard,
}

impl Parser {
    fn new(stage: &ExtractStage) -> Result<Self, KernelError> {
        Ok(match stage {
            ExtractStage::Json => Parser::Json,
            ExtractStage::Logfmt => Parser::Logfmt,
            ExtractStage::Regexp(pattern) => Parser::Regexp(Regex::new(pattern).map_err(|e| {
                KernelError::Unsupported(format!("invalid regular expression {pattern:?}: {e}"))
            })?),
            ExtractStage::Pattern(pattern) => Parser::Pattern(pieces(pattern)?),
            ExtractStage::Unpack => Parser::Unpack,
        })
    }

    /// The labels of one line, and the line's replacement if any.
    fn parse(&self, line: &str) -> (BTreeMap<String, String>, Option<String>) {
        match self {
            Parser::Json => (json_labels(line, false).0, None),
            Parser::Unpack => json_labels(line, true),
            Parser::Logfmt => (logfmt_labels(line), None),
            Parser::Regexp(regex) => {
                let mut labels = BTreeMap::new();
                if let Some(captures) = regex.captures(line) {
                    for name in regex.capture_names().flatten() {
                        if let Some(m) = captures.name(name) {
                            labels.insert(label_name(name), m.as_str().to_owned());
                        }
                    }
                }
                (labels, None)
            }
            Parser::Pattern(pieces) => (pattern_labels(pieces, line), None),
        }
    }
}

/// The error labels Loki sets on a line a parser could not read, so that
/// such lines stay and can be selected or dropped with `__error__`.
pub const ERROR_LABEL: &str = "__error__";
/// The detail of a parser error.
pub const ERROR_DETAILS_LABEL: &str = "__error_details__";

fn parser_error(kind: &str, details: impl std::fmt::Display) -> BTreeMap<String, String> {
    BTreeMap::from([
        (ERROR_LABEL.to_owned(), kind.to_owned()),
        (ERROR_DETAILS_LABEL.to_owned(), details.to_string()),
    ])
}

/// Whether `line` is one ingest reads as JSON: a line that opens an
/// object is a JSON object or nothing, and any other is logfmt or
/// nothing. The format a line's stored fields were parsed as therefore
/// follows from the line alone, which is how a parser stage knows
/// whether they are its own.
pub(crate) fn opens_object(line: &str) -> bool {
    line.trim_start().starts_with('{')
}

/// Whether the `body.*` fields stored for `line` are what `stage` would
/// extract from it: those of a line ingest read as JSON for a `json`
/// stage, those of any other line for a `logfmt` stage. The fields of
/// the other format are not the stage's to take: `logfmt` finds no such
/// pairs in a JSON line, and `json` fails on a logfmt one.
fn reads_stored(stage: &ExtractStage, line: &str) -> bool {
    match stage {
        ExtractStage::Json => opens_object(line),
        ExtractStage::Logfmt => !opens_object(line),
        _ => false,
    }
}

/// The labels the stored `body.*` attributes of a row give (QL-10),
/// named as the `json` stage names them; a list stays out as it would
/// from the stage.
fn stored_labels(overflow: &str) -> BTreeMap<String, String> {
    let Ok(Value::Object(object)) = serde_json::from_str::<Value>(overflow) else {
        return BTreeMap::new();
    };
    object
        .into_iter()
        .filter(|(path, _)| path.starts_with("body."))
        .filter_map(|(path, value)| match value {
            Value::String(s)
                if s.starts_with('[') && serde_json::from_str::<Vec<Value>>(&s).is_ok() =>
            {
                None
            }
            Value::String(s) => Some((label_name(&path), s)),
            Value::Null | Value::Array(_) => None,
            other => Some((label_name(&path), other.to_string())),
        })
        .collect()
}

/// The labels of a JSON object line; with `unpack`, `_entry` is the new
/// line. A line that is not a JSON object yields the error labels.
fn json_labels(line: &str, unpack: bool) -> (BTreeMap<String, String>, Option<String>) {
    let mut labels = BTreeMap::new();
    let object = match serde_json::from_str::<Value>(line) {
        Ok(Value::Object(object)) => object,
        Ok(_) => {
            return (
                parser_error("JSONParserErr", "Value is not an object"),
                None,
            );
        }
        Err(error) => return (parser_error("JSONParserErr", error), None),
    };
    let mut entry = None;
    for (key, value) in object {
        if unpack && key == "_entry" {
            entry = value.as_str().map(str::to_owned);
            continue;
        }
        flatten(&label_name(&key), value, &mut labels);
    }
    (labels, entry)
}

fn flatten(prefix: &str, value: Value, into: &mut BTreeMap<String, String>) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                flatten(&format!("{prefix}_{}", label_name(&key)), value, into);
            }
        }
        Value::Array(_) | Value::Null => {}
        Value::String(s) => {
            into.insert(prefix.to_owned(), s);
        }
        other => {
            into.insert(prefix.to_owned(), other.to_string());
        }
    }
}

/// `key=value` pairs; values may be double-quoted with `\"` escapes. A
/// line without any pair yields the error labels.
pub(crate) fn logfmt_labels(line: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    if !line.contains('=') {
        return parser_error("LogfmtParserErr", "no key=value pair");
    }
    let mut rest = line.trim_start();
    while !rest.is_empty() {
        let key_end = rest
            .find(|c: char| c == '=' || c.is_whitespace())
            .unwrap_or(rest.len());
        let key = &rest[..key_end];
        rest = &rest[key_end..];
        if !rest.starts_with('=') {
            // A bare word: a key with no value.
            rest = rest.trim_start();
            if key.is_empty() {
                rest = &rest[rest.len().min(1)..];
            }
            continue;
        }
        rest = &rest[1..];
        let value = if let Some(after) = rest.strip_prefix('"') {
            let mut value = String::new();
            let mut chars = after.char_indices();
            let mut end = after.len();
            while let Some((i, c)) = chars.next() {
                match c {
                    '\\' => {
                        if let Some((_, escaped)) = chars.next() {
                            value.push(escaped);
                        }
                    }
                    '"' => {
                        end = i + 1;
                        break;
                    }
                    other => value.push(other),
                }
            }
            rest = &after[end.min(after.len())..];
            value
        } else {
            let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
            let value = rest[..end].to_owned();
            rest = &rest[end..];
            value
        };
        if !key.is_empty() {
            labels.insert(label_name(key), value);
        }
        rest = rest.trim_start();
    }
    labels
}

/// Splits a pattern such as `<ip> - - <_> "<method> <path>"` into pieces.
fn pieces(pattern: &str) -> Result<Vec<Piece>, KernelError> {
    let mut pieces = Vec::new();
    let mut rest = pattern;
    while !rest.is_empty() {
        match rest.find('<') {
            Some(0) => {
                let end = rest.find('>').ok_or_else(|| {
                    KernelError::Unsupported(format!("invalid pattern {pattern:?}: unclosed <"))
                })?;
                let name = &rest[1..end];
                if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                    return Err(KernelError::Unsupported(format!(
                        "invalid pattern {pattern:?}: bad capture <{name}>"
                    )));
                }
                if matches!(pieces.last(), Some(Piece::Capture(_) | Piece::Discard)) {
                    return Err(KernelError::Unsupported(format!(
                        "invalid pattern {pattern:?}: captures must be separated by text"
                    )));
                }
                pieces.push(if name == "_" {
                    Piece::Discard
                } else {
                    Piece::Capture(name.to_owned())
                });
                rest = &rest[end + 1..];
            }
            Some(n) => {
                pieces.push(Piece::Literal(rest[..n].to_owned()));
                rest = &rest[n..];
            }
            None => {
                pieces.push(Piece::Literal(rest.to_owned()));
                rest = "";
            }
        }
    }
    Ok(pieces)
}

/// Matches `pieces` against `line`: each capture takes the text up to
/// the next literal. A line that does not fit yields nothing.
fn pattern_labels(pieces: &[Piece], line: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    let mut rest = line;
    let mut iter = pieces.iter().peekable();
    while let Some(piece) = iter.next() {
        match piece {
            Piece::Literal(text) => {
                let Some(stripped) = rest.strip_prefix(text.as_str()) else {
                    return BTreeMap::new();
                };
                rest = stripped;
            }
            Piece::Capture(_) | Piece::Discard => {
                let end = match iter.peek() {
                    Some(Piece::Literal(next)) => match rest.find(next.as_str()) {
                        Some(end) => end,
                        None => return BTreeMap::new(),
                    },
                    _ => rest.len(),
                };
                if let Piece::Capture(name) = piece {
                    labels.insert(label_name(name), rest[..end].to_owned());
                }
                rest = &rest[end..];
            }
        }
    }
    labels
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(stage: &ExtractStage, lines: &[&str]) -> Vec<Option<String>> {
        let text = StringArray::from(lines.to_vec());
        extract(stage, &text, None, None)
            .unwrap()
            .labels
            .iter()
            .map(|l| l.map(str::to_owned))
            .collect()
    }

    /// A stage takes stored fields only where its own parser would have
    /// made them: Loki's `logfmt` finds no pair to take in a JSON line,
    /// and its `json` fails on a logfmt one.
    #[test]
    fn a_stage_leaves_the_stored_fields_of_the_other_format() {
        let text = StringArray::from(vec![Some("{\"path\": \"/web\"}"), Some("path=/web")]);
        let stored = StringArray::from(vec![
            Some("{\"body.path\":\"/web\"}"),
            Some("{\"body.path\":\"/web\"}"),
        ]);
        let logfmt = extract(&ExtractStage::Logfmt, &text, None, Some(&stored)).unwrap();
        let of_json: Value = serde_json::from_str(logfmt.labels.value(0)).unwrap();
        assert!(of_json.get("path").is_none(), "{of_json}");
        let of_logfmt: Value = serde_json::from_str(logfmt.labels.value(1)).unwrap();
        assert_eq!(of_logfmt["path"], "/web");
        let json = extract(&ExtractStage::Json, &text, None, Some(&stored)).unwrap();
        let of_json: Value = serde_json::from_str(json.labels.value(0)).unwrap();
        assert_eq!(of_json["path"], "/web");
        let of_logfmt: Value = serde_json::from_str(json.labels.value(1)).unwrap();
        assert!(of_logfmt.get("path").is_none(), "{of_logfmt}");
        assert_eq!(of_logfmt[ERROR_LABEL], "JSONParserErr");
    }

    /// QL-10: a row with stored body fields is not parsed again; one
    /// without is.
    #[test]
    fn json_and_logfmt_stages_read_stored_body_fields_first() {
        let text = StringArray::from(vec![
            Some("{\"user\":\"from the line\",\"http\":{\"status\":200}}"),
            Some("{\"user\":\"parsed\"}"),
            Some("level=info msg=hi"),
        ]);
        let stored = StringArray::from(vec![
            Some(
                "{\"attr.k\":\"v\",\"body.user\":\"stored\",\"body.http.status\":500,\"body.tags\":\"[1,2]\"}",
            ),
            None,
            Some("{\"body.msg\":\"stored hi\"}"),
        ]);
        let json = extract(&ExtractStage::Json, &text, None, Some(&stored)).unwrap();
        let first: Value = serde_json::from_str(json.labels.value(0)).unwrap();
        assert_eq!(first["user"], "stored");
        assert_eq!(first["http_status"], "500");
        assert!(first.get("tags").is_none(), "a list is no label");
        let second: Value = serde_json::from_str(json.labels.value(1)).unwrap();
        assert_eq!(second["user"], "parsed");
        let logfmt = extract(&ExtractStage::Logfmt, &text, None, Some(&stored)).unwrap();
        let third: Value = serde_json::from_str(logfmt.labels.value(2)).unwrap();
        assert_eq!(third["msg"], "stored hi");
        let pattern = extract(
            &ExtractStage::Pattern("<level>=<rest>".into()),
            &text,
            None,
            Some(&stored),
        )
        .unwrap();
        assert!(
            pattern.labels.value(2).contains("level"),
            "other stages always parse: {}",
            pattern.labels.value(2)
        );
    }

    #[test]
    fn json_flattens_nested_objects_and_skips_arrays() {
        let out = run(
            &ExtractStage::Json,
            &[
                r#"{"status":500,"request":{"method":"GET","tags":["a"]},"ok":false,"x":null}"#,
                "not json",
            ],
        );
        assert_eq!(
            out[0].as_deref(),
            Some(r#"{"ok":"false","request_method":"GET","status":"500"}"#)
        );
        assert!(
            out[1]
                .as_deref()
                .unwrap()
                .contains(r#""__error__":"JSONParserErr""#)
        );
    }

    #[test]
    fn logfmt_handles_quotes_and_bare_words() {
        let out = run(
            &ExtractStage::Logfmt,
            &[r#"level=error msg="disk full: /var" code=5 flag"#],
        );
        assert_eq!(
            out[0].as_deref(),
            Some(r#"{"code":"5","level":"error","msg":"disk full: /var"}"#)
        );
    }

    #[test]
    fn regexp_uses_named_groups_and_pattern_uses_captures() {
        let out = run(
            &ExtractStage::Regexp(r"(?P<method>[A-Z]+) (?P<path>\S+)".into()),
            &["GET /checkout 200", "nope"],
        );
        assert_eq!(
            out[0].as_deref(),
            Some(r#"{"method":"GET","path":"/checkout"}"#)
        );
        assert_eq!(out[1], None);
        let out = run(
            &ExtractStage::Pattern("<ip> - <_> \"<method> <path>\" <status>".into()),
            &["10.0.0.1 - user \"GET /x\" 204", "garbage"],
        );
        assert_eq!(
            out[0].as_deref(),
            Some(r#"{"ip":"10.0.0.1","method":"GET","path":"/x","status":"204"}"#)
        );
        assert_eq!(out[1], None);
        assert!(
            extract(
                &ExtractStage::Pattern("<a><b>".into()),
                &StringArray::from(vec!["x"]),
                None,
                None
            )
            .is_err()
        );
        assert!(
            extract(
                &ExtractStage::Regexp("(".into()),
                &StringArray::from(vec!["x"]),
                None,
                None
            )
            .is_err()
        );
    }

    #[test]
    fn stages_merge_and_unpack_replaces_the_line() {
        let text = StringArray::from(vec![r#"{"_entry":"real line","app":"a"}"#, "plain"]);
        let existing = StringArray::from(vec![Some(r#"{"app":"old","keep":"1"}"#), None]);
        let out = extract(&ExtractStage::Unpack, &text, Some(&existing), None).unwrap();
        assert_eq!(out.labels.value(0), r#"{"app":"a","keep":"1"}"#);
        let line = out.line.unwrap();
        assert_eq!(line.value(0), "real line");
        assert_eq!(line.value(1), "plain");
        assert!(
            out.labels.value(1).contains("LogfmtParserErr")
                || out.labels.value(1).contains("JSONParserErr")
        );
    }
}
