// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The search syntax (spec section 9.6): a small filter language for
//! interactive use that applies to any kind.
//!
//! ```text
//! service:checkout severity>=error "connection reset" duration>500ms -attr.user.tier:free
//! ```
//!
//! - A bare term matches a token of `body` (QL-16, by the tokenizer of
//!   QL-18); a quoted string is a phrase, matched as a substring.
//! - `field:value` is equality; `=`, `!=`, `<`, `<=`, `>` and `>=` compare.
//! - A leading `-` negates the term.
//! - Fields resolve as section 4.4 says (DM-6): a well-known column, then
//!   `attr.`, `resource.` and `scope.` in that order; `service` is the
//!   alias of `resource.service.name`; `severity` and `level` compare
//!   severity numbers and accept the names `trace`, `debug`, `info`,
//!   `warn`, `error` and `fatal`; `trace_id` and `span_id` take hex.
//! - Values with a unit (`500ms`, `2s`, `3m`) are nanoseconds; plain
//!   numbers are numbers; anything else is text.
//!
//! The result is one predicate over the plan's expressions, so that
//! `search('...')` works inside SQL (QL-3) and over HTTP alike.

use arrow::datatypes::DataType;
use tessera_ir::{BinaryOp, Diagnostic, Expr, Literal, Span};
use tessera_types::event::{self, SEVERITY_NUMBER};
use tessera_types::{Kind, Timestamp};

/// One parsed term.
#[derive(Clone, PartialEq, Debug)]
struct Term {
    negated: bool,
    field: Option<String>,
    op: BinaryOp,
    value: String,
    quoted: bool,
    span: Span,
}

/// Parses `text` into a predicate over events of `kind`.
///
/// # Errors
///
/// The first malformed term, with its position.
pub fn parse(text: &str, kind: Kind) -> Result<Expr, Diagnostic> {
    let terms = terms(text)?;
    let predicates = terms
        .into_iter()
        .map(|t| lower(&t, kind))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Expr::all(predicates).unwrap_or_else(|| Expr::literal(true)))
}

fn terms(text: &str) -> Result<Vec<Term>, Diagnostic> {
    let bytes = text.as_bytes();
    let mut terms = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        let negated = bytes[i] == b'-';
        if negated {
            i += 1;
        }
        // A field name runs up to an operator; a term without one is a
        // bare term or a phrase.
        let name_end = text[i..]
            .find(|c: char| c.is_whitespace() || "=:<>!\"".contains(c))
            .map_or(bytes.len(), |n| i + n);
        let (field, op) = match bytes.get(name_end) {
            Some(b':' | b'=') if name_end > i => (Some(text[i..name_end].to_owned()), BinaryOp::Eq),
            Some(b'!') if bytes.get(name_end + 1) == Some(&b'=') && name_end > i => {
                (Some(text[i..name_end].to_owned()), BinaryOp::NotEq)
            }
            Some(b'<') if name_end > i => {
                if bytes.get(name_end + 1) == Some(&b'=') {
                    (Some(text[i..name_end].to_owned()), BinaryOp::LtEq)
                } else {
                    (Some(text[i..name_end].to_owned()), BinaryOp::Lt)
                }
            }
            Some(b'>') if name_end > i => {
                if bytes.get(name_end + 1) == Some(&b'=') {
                    (Some(text[i..name_end].to_owned()), BinaryOp::GtEq)
                } else {
                    (Some(text[i..name_end].to_owned()), BinaryOp::Gt)
                }
            }
            _ => (None, BinaryOp::Eq),
        };
        if field.is_some() {
            i = name_end + 1;
            if matches!(op, BinaryOp::NotEq | BinaryOp::LtEq | BinaryOp::GtEq) {
                i += 1;
            }
        }
        let (value, quoted, end) = value(text, i, start)?;
        if value.is_empty() {
            return Err(Diagnostic::at(
                Span::new(start, end),
                "a term needs a value",
            ));
        }
        terms.push(Term {
            negated,
            field,
            op,
            value,
            quoted,
            span: Span::new(start, end),
        });
        i = end;
    }
    Ok(terms)
}

/// The value starting at `i`: a quoted phrase or a run of non-blank
/// characters. Returns the value, whether it was quoted, and the end.
fn value(text: &str, i: usize, start: usize) -> Result<(String, bool, usize), Diagnostic> {
    let bytes = text.as_bytes();
    if bytes.get(i) == Some(&b'"') {
        let mut value = String::new();
        let mut j = i + 1;
        while j < bytes.len() {
            match bytes[j] {
                b'"' => return Ok((value, true, j + 1)),
                b'\\' if j + 1 < bytes.len() => {
                    // The escaped character may be more than one byte.
                    let c = text[j + 1..].chars().next().unwrap_or('?');
                    value.push(c);
                    j += 1 + c.len_utf8();
                }
                _ => {
                    let c = text[j..].chars().next().unwrap_or('?');
                    value.push(c);
                    j += c.len_utf8();
                }
            }
        }
        return Err(Diagnostic::at(
            Span::new(start, bytes.len()),
            "unterminated phrase",
        ));
    }
    let end = text[i..]
        .find(char::is_whitespace)
        .map_or(bytes.len(), |n| i + n);
    Ok((text[i..end].to_owned(), false, end))
}

fn lower(term: &Term, kind: Kind) -> Result<Expr, Diagnostic> {
    let predicate = match &term.field {
        None if term.quoted => Expr::Call {
            name: "contains".into(),
            args: vec![
                Expr::column(event::BODY),
                Expr::literal(term.value.as_str()),
            ],
        },
        None => Expr::Call {
            name: "has_token".into(),
            args: vec![
                Expr::column(event::BODY),
                Expr::literal(term.value.to_lowercase()),
            ],
        },
        Some(field) => field_predicate(field, term, kind)?,
    };
    Ok(if term.negated {
        Expr::Not(Box::new(predicate))
    } else {
        predicate
    })
}

fn field_predicate(field: &str, term: &Term, kind: Kind) -> Result<Expr, Diagnostic> {
    let schema = event::schema(kind);
    let column_type = |name: &str| {
        schema
            .field_with_name(name)
            .ok()
            .map(|f| f.data_type().clone())
    };
    let lower = field.to_ascii_lowercase();
    let (target, value) = match lower.as_str() {
        "severity" | "level" => (
            Expr::column(SEVERITY_NUMBER),
            Literal::Int(severity_number(&term.value).ok_or_else(|| {
                Diagnostic::at(term.span, format!("unknown severity {}", term.value))
            })?),
        ),
        "service" => (
            Expr::Path {
                path: "resource.service.name".into(),
                data_type: DataType::Utf8,
            },
            Literal::Text(term.value.clone()),
        ),
        "trace_id" | "span_id" => (
            Expr::column(&lower),
            Literal::Bytes(hex(&term.value).ok_or_else(|| {
                Diagnostic::at(term.span, format!("{lower} needs hex, got {}", term.value))
            })?),
        ),
        name if column_type(name).is_some() => {
            let data_type = column_type(name).unwrap_or(DataType::Utf8);
            (
                Expr::column(name),
                literal_for(&term.value, term.quoted, &data_type, term.span)?,
            )
        }
        name if event::SCOPES.iter().any(|s| name.starts_with(s)) => {
            let value = scalar_literal(&term.value, term.quoted);
            let data_type = value.data_type().unwrap_or(DataType::Utf8);
            (
                Expr::Path {
                    path: name.to_owned(),
                    data_type,
                },
                value,
            )
        }
        name => {
            let value = scalar_literal(&term.value, term.quoted);
            let data_type = value.data_type().unwrap_or(DataType::Utf8);
            (
                Expr::Label {
                    name: name.to_owned(),
                    data_type,
                },
                value,
            )
        }
    };
    Ok(target.binary(term.op, Expr::Literal(value)))
}

/// A value as the literal its text suggests: a number, a duration in
/// nanoseconds, or text. Quoted values are always text.
fn scalar_literal(value: &str, quoted: bool) -> Literal {
    if quoted {
        return Literal::Text(value.to_owned());
    }
    if let Ok(i) = value.parse::<i64>() {
        return Literal::Int(i);
    }
    if let Ok(f) = value.parse::<f64>() {
        return Literal::Float(f);
    }
    if let Some(nanos) = duration_nanos(value) {
        return Literal::Int(nanos);
    }
    match value {
        "true" => Literal::Bool(true),
        "false" => Literal::Bool(false),
        _ => Literal::Text(value.to_owned()),
    }
}

/// A value read in the type of a dedicated column.
fn literal_for(
    value: &str,
    quoted: bool,
    data_type: &DataType,
    span: Span,
) -> Result<Literal, Diagnostic> {
    let invalid = |what: &str| Diagnostic::at(span, format!("{value:?} is not {what}"));
    Ok(match data_type {
        DataType::Int32 | DataType::Int64 => {
            Literal::Int(value.parse().map_err(|_| invalid("a number"))?)
        }
        DataType::Float64 => Literal::Float(value.parse().map_err(|_| invalid("a number"))?),
        DataType::Boolean => Literal::Bool(value.parse().map_err(|_| invalid("a boolean"))?),
        DataType::Timestamp(_, _) => Literal::Timestamp(
            value
                .parse::<Timestamp>()
                .map_err(|_| invalid("a timestamp"))?,
        ),
        DataType::FixedSizeBinary(_) | DataType::Binary => {
            Literal::Bytes(hex(value).ok_or_else(|| invalid("hex"))?)
        }
        _ => scalar_literal(value, quoted),
    })
}

/// OpenTelemetry severity numbers for the usual level names, or a number.
fn severity_number(value: &str) -> Option<i64> {
    match value.to_ascii_lowercase().as_str() {
        "trace" => Some(1),
        "debug" => Some(5),
        "info" => Some(9),
        "warn" | "warning" => Some(13),
        "error" | "err" => Some(17),
        "fatal" | "critical" => Some(21),
        other => other.parse().ok(),
    }
}

/// `500ms`, `2s`, `3m`, `1h`, `2d` as nanoseconds.
fn duration_nanos(value: &str) -> Option<i64> {
    let digits = value.find(|c: char| !(c.is_ascii_digit() || c == '.'))?;
    let number: f64 = value[..digits].parse().ok()?;
    let unit = match &value[digits..] {
        "ns" => 1.0,
        "us" | "µs" => 1e3,
        "ms" => 1e6,
        "s" => 1e9,
        "m" => 60e9,
        "h" => 3600e9,
        "d" => 86_400e9,
        _ => return None,
    };
    #[allow(clippy::cast_possible_truncation)]
    Some((number * unit) as i64)
}

fn hex(text: &str) -> Option<Vec<u8>> {
    if text.len() % 2 != 0 || text.is_empty() {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lowered(text: &str) -> String {
        parse(text, Kind::Log).unwrap().to_string()
    }

    /// RS-5: no search text, however mangled, makes the parser panic.
    #[test]
    fn mangled_searches_never_panic() {
        let seeds = [
            "timeout -healthcheck service:checkout severity:error \"exact phrase\"",
            "(a OR b) AND NOT c attr.http.status:500 trace:0123456789abcdef0123456789abcdef",
        ];
        let mut random = tessera_types::SeededRandom::new(5);
        for round in 0..2_000 {
            let input = tessera_types::mutated(seeds[round % seeds.len()].as_bytes(), &mut random);
            for kind in Kind::ALL {
                let _ = parse(&String::from_utf8_lossy(&input), kind);
            }
        }
    }

    #[test]
    fn bare_terms_and_phrases_search_the_body() {
        assert_eq!(lowered("Timeout"), "has_token(body, 'timeout')");
        assert_eq!(
            lowered("\"connection reset\" -healthcheck"),
            "(contains(body, 'connection reset') AND NOT has_token(body, 'healthcheck'))"
        );
        assert_eq!(lowered(""), "true");
    }

    #[test]
    fn fields_resolve_to_columns_aliases_paths_and_labels() {
        assert_eq!(
            lowered("service:checkout"),
            "(resource.service.name = 'checkout')"
        );
        assert_eq!(lowered("severity>=error"), "(severity_number >= 17)");
        assert_eq!(lowered("level:13"), "(severity_number = 13)");
        assert_eq!(lowered("severity_text!=INFO"), "(severity_text <> 'INFO')");
        assert_eq!(lowered("attr.user.tier:free"), "(attr.user.tier = 'free')");
        assert_eq!(lowered("-attr.n>5"), "NOT (attr.n > 5)");
        assert_eq!(lowered("host:web1"), "(label(host) = 'web1')");
        assert_eq!(lowered("duration>500ms"), "(label(duration) > 500000000)");
        assert_eq!(lowered("trace_id=0102"), "(trace_id = X'0102')");
        assert_eq!(lowered("ok=true"), "(label(ok) = true)");
        assert_eq!(lowered("ratio<=0.5"), "(label(ratio) <= 0.5)");
        assert_eq!(lowered("attr.port:\"8080\""), "(attr.port = '8080')");
    }

    #[test]
    fn errors_are_positioned() {
        let error = parse("severity:loud", Kind::Log).unwrap_err();
        assert_eq!(error.message, "unknown severity loud");
        assert_eq!(error.span, Some(Span::new(0, 13)));
        let error = parse("a \"open", Kind::Log).unwrap_err();
        assert_eq!(error.message, "unterminated phrase");
        let error = parse("trace_id:zz", Kind::Log).unwrap_err();
        assert!(error.message.contains("needs hex"));
        let error = parse("severity_number:high", Kind::Log).unwrap_err();
        assert!(error.message.contains("not a number"));
        let error = parse("field:", Kind::Log).unwrap_err();
        assert_eq!(error.message, "a term needs a value");
    }
}
