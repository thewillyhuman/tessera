// SPDX-License-Identifier: Apache-2.0

//! Tokens of `TraceQL` text, each with its byte span.
//!
//! Attributes are one token: `.foo`, `span.http.method`,
//! `resource.service.name`, `span."quoted name"`. An attribute name runs
//! to whitespace or an operator character, so it may contain dots,
//! dashes and slashes. A scoped intrinsic such as `span:duration` or
//! `trace:rootName` is one token too.

use std::time::Duration;

use tessera_ir::{Diagnostic, Span};

/// A token.
#[derive(Clone, PartialEq, Debug)]
pub enum Token {
    /// An identifier, keyword or intrinsic, as written.
    Ident(String),
    /// A scoped intrinsic: `span:duration` is `("span", "duration")`.
    ScopedIntrinsic(String, String),
    /// An attribute: its scope (`span`, `resource`, ... or none for
    /// `.foo`) and its name.
    Attribute(Option<String>, String),
    /// A string literal, unescaped.
    String(String),
    /// An integer.
    Int(i64),
    /// A float.
    Float(f64),
    /// A duration such as `100ms` or `1h30m`.
    Duration(Duration),
    /// Punctuation or an operator.
    Symbol(&'static str),
}

/// A token and where it came from.
#[derive(Clone, PartialEq, Debug)]
pub struct Spanned {
    /// The token.
    pub token: Token,
    /// Its bytes in the text.
    pub span: Span,
}

/// The attribute scopes.
pub const SCOPES: [&str; 6] = [
    "span",
    "resource",
    "event",
    "link",
    "instrumentation",
    "parent",
];

/// The scopes of intrinsics written with a colon.
pub const INTRINSIC_SCOPES: [&str; 5] = ["span", "trace", "event", "link", "instrumentation"];

/// Operators and punctuation, longest first so that `!>>` beats `!>`.
const SYMBOLS: [&str; 35] = [
    "&>>", "&<<", "!>>", "!<<", ">>", "<<", "&>", "&<", "&~", "!>", "!<", "!~", "!=", "=~", ">=",
    "<=", "&&", "||", "{", "}", "(", ")", ",", "|", "=", ">", "<", "~", "+", "-", "*", "/", "%",
    "^", "!",
];

/// Whether `c` may continue an attribute name.
fn is_attribute_char(c: char) -> bool {
    !c.is_whitespace()
        && !matches!(
            c,
            '{' | '}' | '(' | ')' | '=' | '~' | '!' | '<' | '>' | '&' | '|' | '^' | ','
        )
}

/// Splits `text` into tokens.
///
/// # Errors
///
/// On an unterminated string, a malformed number or a character
/// `TraceQL` has no use for.
pub fn tokenize(text: &str) -> Result<Vec<Spanned>, Diagnostic> {
    let bytes = text.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        if c == b'"' || c == b'`' {
            let (value, end) = quoted(text, i, c)?;
            tokens.push(Spanned {
                token: Token::String(value),
                span: Span::new(start, end),
            });
            i = end;
            continue;
        }
        if c == b'.'
            && bytes
                .get(i + 1)
                .is_some_and(|&n| n == b'"' || (is_attribute_byte(n) && !n.is_ascii_digit()))
        {
            let (name, end) = attribute_name(text, i + 1)?;
            tokens.push(Spanned {
                token: Token::Attribute(None, name),
                span: Span::new(start, end),
            });
            i = end;
            continue;
        }
        if c.is_ascii_digit() || (c == b'.' && bytes.get(i + 1).is_some_and(u8::is_ascii_digit)) {
            let (token, end) = number(text, i)?;
            tokens.push(Spanned {
                token,
                span: Span::new(start, end),
            });
            i = end;
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' {
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let word = &text[start..i];
            let (token, end) = word_token(text, word, start, i)?;
            tokens.push(Spanned {
                token,
                span: Span::new(start, end),
            });
            i = end;
            continue;
        }
        if let Some(symbol) = SYMBOLS.iter().find(|s| text[i..].starts_with(**s)) {
            i += symbol.len();
            tokens.push(Spanned {
                token: Token::Symbol(symbol),
                span: Span::new(start, i),
            });
            continue;
        }
        let ch = text[i..].chars().next().unwrap_or('?');
        return Err(Diagnostic::at(
            Span::new(start, start + ch.len_utf8()),
            format!("unexpected character {ch:?}"),
        ));
    }
    Ok(tokens)
}

fn is_attribute_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// The token for the word `word` at `start..end`: an attribute when a
/// scope is followed by `.`, a scoped intrinsic when followed by `:`,
/// an identifier otherwise.
fn word_token(
    text: &str,
    word: &str,
    start: usize,
    end: usize,
) -> Result<(Token, usize), Diagnostic> {
    let bytes = text.as_bytes();
    let next = bytes.get(end).copied();
    if next == Some(b'.') && SCOPES.contains(&word) {
        let (name, end) = attribute_name(text, end + 1)?;
        return Ok((Token::Attribute(Some(word.to_owned()), name), end));
    }
    if next == Some(b':') && INTRINSIC_SCOPES.contains(&word) {
        let mut i = end + 1;
        while i < bytes.len() && is_attribute_byte(bytes[i]) {
            i += 1;
        }
        if i == end + 1 {
            return Err(Diagnostic::at(
                Span::new(start, i),
                format!("{word}: expected an intrinsic name"),
            ));
        }
        return Ok((
            Token::ScopedIntrinsic(word.to_owned(), text[end + 1..i].to_owned()),
            i,
        ));
    }
    if next == Some(b'.')
        && bytes
            .get(end + 1)
            .is_some_and(|&n| n == b'"' || is_attribute_byte(n))
    {
        return Err(Diagnostic::at(
            Span::new(start, end),
            format!(
                "unknown scope {word:?}; expected one of span, resource, event, link, instrumentation"
            ),
        ));
    }
    Ok((Token::Ident(word.to_owned()), end))
}

/// An attribute name starting at `start`: quoted, or a run of attribute
/// characters.
fn attribute_name(text: &str, start: usize) -> Result<(String, usize), Diagnostic> {
    if text.as_bytes().get(start) == Some(&b'"') {
        return quoted(text, start, b'"');
    }
    let end = text[start..]
        .char_indices()
        .find(|(_, c)| !is_attribute_char(*c))
        .map_or(text.len(), |(i, _)| start + i);
    if end == start {
        return Err(Diagnostic::at(
            Span::new(start, start),
            "expected an attribute name",
        ));
    }
    Ok((text[start..end].to_owned(), end))
}

/// An integer, a float or a duration starting at `start`, and where it
/// ends.
fn number(text: &str, start: usize) -> Result<(Token, usize), Diagnostic> {
    let bytes = text.as_bytes();
    let mut i = start;
    while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
        i += 1;
    }
    let number_end = i;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.') {
        i += 1;
    }
    let digits = &text[start..number_end];
    if i == number_end {
        if let Ok(int) = digits.parse::<i64>() {
            return Ok((Token::Int(int), i));
        }
        return digits
            .parse::<f64>()
            .map(|f| (Token::Float(f), i))
            .map_err(|_| Diagnostic::at(Span::new(start, i), format!("invalid number {digits}")));
    }
    parse_duration(&text[start..i])
        .map(|d| (Token::Duration(d), i))
        .ok_or_else(|| {
            Diagnostic::at(
                Span::new(start, i),
                format!("invalid duration {}", &text[start..i]),
            )
        })
}

/// A quoted string: double quotes with Go escapes, or raw backticks.
fn quoted(text: &str, start: usize, quote: u8) -> Result<(String, usize), Diagnostic> {
    let bytes = text.as_bytes();
    let mut value = String::new();
    let mut i = start + 1;
    while i < bytes.len() {
        let c = text[i..].chars().next().unwrap_or('?');
        if bytes[i] == quote {
            return Ok((value, i + 1));
        }
        if quote == b'"' && c == '\\' {
            let escaped = text[i + 1..].chars().next().ok_or_else(|| {
                Diagnostic::at(Span::new(start, bytes.len()), "unterminated string")
            })?;
            value.push(match escaped {
                'n' => '\n',
                't' => '\t',
                'r' => '\r',
                other => other,
            });
            i += 1 + escaped.len_utf8();
            continue;
        }
        value.push(c);
        i += c.len_utf8();
    }
    Err(Diagnostic::at(
        Span::new(start, bytes.len()),
        "unterminated string",
    ))
}

/// `100ms`, `1.5s`, `1h30m`, `2d`.
#[must_use]
pub fn parse_duration(text: &str) -> Option<Duration> {
    let mut total = 0.0f64;
    let mut rest = text;
    if rest.is_empty() {
        return None;
    }
    while !rest.is_empty() {
        let digits = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        let value: f64 = rest[..digits].parse().ok()?;
        rest = &rest[digits..];
        let unit_end = rest
            .find(|c: char| !c.is_ascii_alphabetic())
            .unwrap_or(rest.len());
        let seconds = match &rest[..unit_end] {
            "ns" => 1e-9,
            "us" | "µs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            "d" => 86_400.0,
            "w" => 604_800.0,
            "y" => 365.0 * 86_400.0,
            _ => return None,
        };
        rest = &rest[unit_end..];
        total += value * seconds;
    }
    Some(Duration::from_secs_f64(total))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(text: &str) -> Vec<Token> {
        tokenize(text)
            .unwrap()
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    #[test]
    fn lexes_attributes_intrinsics_and_operators() {
        assert_eq!(
            tokens(
                r#"{ span.http.status_code >= 500 && resource.service.name = "api" } >> { .error }"#
            ),
            vec![
                Token::Symbol("{"),
                Token::Attribute(Some("span".into()), "http.status_code".into()),
                Token::Symbol(">="),
                Token::Int(500),
                Token::Symbol("&&"),
                Token::Attribute(Some("resource".into()), "service.name".into()),
                Token::Symbol("="),
                Token::String("api".into()),
                Token::Symbol("}"),
                Token::Symbol(">>"),
                Token::Symbol("{"),
                Token::Attribute(None, "error".into()),
                Token::Symbol("}"),
            ]
        );
        assert_eq!(
            tokens("duration > 1.5s && span:name =~ `a.*` || !>> &~ status = error"),
            vec![
                Token::Ident("duration".into()),
                Token::Symbol(">"),
                Token::Duration(Duration::from_millis(1_500)),
                Token::Symbol("&&"),
                Token::ScopedIntrinsic("span".into(), "name".into()),
                Token::Symbol("=~"),
                Token::String("a.*".into()),
                Token::Symbol("||"),
                Token::Symbol("!>>"),
                Token::Symbol("&~"),
                Token::Ident("status".into()),
                Token::Symbol("="),
                Token::Ident("error".into()),
            ]
        );
    }

    #[test]
    fn attribute_names_run_to_operators_and_may_be_quoted() {
        assert_eq!(
            tokens(r#"span.k8s.pod-name/x=1 .a,.b span."with space" 2.5 -3"#),
            vec![
                Token::Attribute(Some("span".into()), "k8s.pod-name/x".into()),
                Token::Symbol("="),
                Token::Int(1),
                Token::Attribute(None, "a".into()),
                Token::Symbol(","),
                Token::Attribute(None, "b".into()),
                Token::Attribute(Some("span".into()), "with space".into()),
                Token::Float(2.5),
                Token::Symbol("-"),
                Token::Int(3),
            ]
        );
        assert_eq!(
            tokens(".99 !"),
            vec![Token::Float(0.99), Token::Symbol("!")]
        );
    }

    #[test]
    fn errors_are_positioned() {
        let error = tokenize("{ foo.bar = 1 }").unwrap_err();
        assert_eq!(error.span, Some(Span::new(2, 5)));
        assert!(error.message.contains("unknown scope"), "{}", error.message);
        assert_eq!(
            tokenize(r#"{ name = "x }"#).unwrap_err().message,
            "unterminated string"
        );
        assert!(
            tokenize("{ duration > 5x }")
                .unwrap_err()
                .message
                .contains("invalid duration")
        );
        assert_eq!(
            tokenize("{ name = @ }").unwrap_err().span,
            Some(Span::new(9, 10))
        );
        assert!(
            tokenize("{ span: }")
                .unwrap_err()
                .message
                .contains("intrinsic")
        );
    }
}
