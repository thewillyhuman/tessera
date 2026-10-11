// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Tokens of `PromQL` text, each with its byte span.
//!
//! Metric names may hold colons; keywords are case-insensitive, as in
//! Prometheus; a number followed by a unit is a duration, `Inf` and
//! `NaN` are numbers, and strings take double quotes with Go escapes,
//! single quotes, or raw backticks.

use std::time::Duration;

use tessera_ir::{Diagnostic, Span};

/// A token.
#[derive(Clone, PartialEq, Debug)]
pub enum Token {
    /// An identifier, metric name or keyword, as written.
    Ident(String),
    /// A number.
    Number(f64),
    /// A duration such as `5m` or `1h30m`.
    Duration(Duration),
    /// A string literal, unescaped.
    String(String),
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

/// Operators and punctuation, longest first so that `=~` beats `=`.
const SYMBOLS: [&str; 20] = [
    "==", "!=", "<=", ">=", "=~", "!~", "{", "}", "(", ")", "[", "]", ",", ":", "=", "<", ">", "+",
    "-", "*",
];
const MORE_SYMBOLS: [&str; 4] = ["/", "%", "^", "@"];

/// Splits `text` into tokens.
///
/// # Errors
///
/// On an unterminated string, a malformed number or a character
/// `PromQL` has no use for.
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
        if c == b'#' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        let start = i;
        if c == b'"' || c == b'\'' || c == b'`' {
            let (value, end) = quoted(text, i, c)?;
            tokens.push(Spanned {
                token: Token::String(value),
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
        // A colon starts a recording-rule name (`job:rate5m`), not a
        // subquery step (`[5m:1m]`).
        let starts_name = c.is_ascii_alphabetic()
            || c == b'_'
            || (c == b':'
                && bytes
                    .get(i + 1)
                    .is_some_and(|n| n.is_ascii_alphabetic() || *n == b'_' || *n == b':'));
        if starts_name {
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b':')
            {
                i += 1;
            }
            let word = &text[start..i];
            let token = match word.to_ascii_lowercase().as_str() {
                "inf" => Token::Number(f64::INFINITY),
                "nan" => Token::Number(f64::NAN),
                _ => Token::Ident(word.to_owned()),
            };
            tokens.push(Spanned {
                token,
                span: Span::new(start, i),
            });
            continue;
        }
        if let Some(symbol) = SYMBOLS
            .iter()
            .chain(MORE_SYMBOLS.iter())
            .find(|s| text[i..].starts_with(**s))
        {
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

/// A number or a duration starting at `start`, and where it ends.
fn number(text: &str, start: usize) -> Result<(Token, usize), Diagnostic> {
    let bytes = text.as_bytes();
    if text[start..].starts_with("0x") || text[start..].starts_with("0X") {
        let mut i = start + 2;
        while i < bytes.len() && bytes[i].is_ascii_hexdigit() {
            i += 1;
        }
        let value = i64::from_str_radix(&text[start + 2..i], 16)
            .map_err(|_| Diagnostic::at(Span::new(start, i), "invalid hexadecimal number"))?;
        #[allow(clippy::cast_precision_loss)]
        return Ok((Token::Number(value as f64), i));
    }
    let mut i = start;
    while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
        i += 1;
    }
    let exponent = i < bytes.len()
        && (bytes[i] == b'e' || bytes[i] == b'E')
        && bytes
            .get(i + 1)
            .is_some_and(|n| n.is_ascii_digit() || *n == b'+' || *n == b'-');
    if exponent {
        i += 1;
        if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
            i += 1;
        }
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
    }
    let digits_end = i;
    while i < bytes.len() && bytes[i].is_ascii_alphanumeric() {
        i += 1;
    }
    if i == digits_end {
        return text[start..i]
            .parse::<f64>()
            .map(|n| (Token::Number(n), i))
            .map_err(|_| Diagnostic::at(Span::new(start, i), "invalid number"));
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

/// A quoted string: double quotes with Go escapes, single quotes with
/// the same, or raw backticks.
fn quoted(text: &str, start: usize, quote: u8) -> Result<(String, usize), Diagnostic> {
    let bytes = text.as_bytes();
    let mut value = String::new();
    let mut i = start + 1;
    while i < bytes.len() {
        let c = text[i..].chars().next().unwrap_or('?');
        if bytes[i] == quote {
            return Ok((value, i + 1));
        }
        if quote != b'`' && c == '\\' {
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

/// `5m`, `1h30m`, `250ms`, `2d`, `1w`, `1y`: Prometheus durations.
#[must_use]
pub fn parse_duration(text: &str) -> Option<Duration> {
    let mut total = 0u64;
    let mut rest = text;
    if rest.is_empty() {
        return None;
    }
    while !rest.is_empty() {
        let digits = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        let value: u64 = rest[..digits].parse().ok()?;
        rest = &rest[digits..];
        let unit_end = rest
            .find(|c: char| !c.is_ascii_alphabetic())
            .unwrap_or(rest.len());
        let millis = match &rest[..unit_end] {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            "w" => 604_800_000,
            "y" => 31_536_000_000,
            _ => return None,
        };
        rest = &rest[unit_end..];
        total = total.checked_add(value.checked_mul(millis)?)?;
    }
    Some(Duration::from_millis(total))
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
    fn lexes_selectors_operators_and_durations() {
        assert_eq!(
            tokens(
                r#"sum by (job) (rate(http_requests_total{job=~"api.*", code!="500"}[5m] offset 1h30m)) / 2"#
            ),
            vec![
                Token::Ident("sum".into()),
                Token::Ident("by".into()),
                Token::Symbol("("),
                Token::Ident("job".into()),
                Token::Symbol(")"),
                Token::Symbol("("),
                Token::Ident("rate".into()),
                Token::Symbol("("),
                Token::Ident("http_requests_total".into()),
                Token::Symbol("{"),
                Token::Ident("job".into()),
                Token::Symbol("=~"),
                Token::String("api.*".into()),
                Token::Symbol(","),
                Token::Ident("code".into()),
                Token::Symbol("!="),
                Token::String("500".into()),
                Token::Symbol("}"),
                Token::Symbol("["),
                Token::Duration(Duration::from_secs(300)),
                Token::Symbol("]"),
                Token::Ident("offset".into()),
                Token::Duration(Duration::from_secs(5400)),
                Token::Symbol(")"),
                Token::Symbol(")"),
                Token::Symbol("/"),
                Token::Number(2.0),
            ]
        );
        let mixed = tokens("node:cpu:ratio @ 1.5e3 == bool Inf -NaN 0x1f .5 250ms");
        assert_eq!(mixed[0], Token::Ident("node:cpu:ratio".into()));
        assert_eq!(mixed[1], Token::Symbol("@"));
        assert_eq!(mixed[2], Token::Number(1500.0));
        assert_eq!(mixed[3], Token::Symbol("=="));
        assert_eq!(mixed[4], Token::Ident("bool".into()));
        assert_eq!(mixed[5], Token::Number(f64::INFINITY));
        assert_eq!(mixed[6], Token::Symbol("-"));
        assert!(matches!(mixed[7], Token::Number(n) if n.is_nan()));
        assert_eq!(mixed[8], Token::Number(31.0));
        assert_eq!(mixed[9], Token::Number(0.5));
        assert_eq!(mixed[10], Token::Duration(Duration::from_millis(250)));
    }

    #[test]
    fn strings_and_errors() {
        assert_eq!(
            tokens(r#""a\"b" 'c' `d\e`"#),
            vec![
                Token::String("a\"b".into()),
                Token::String("c".into()),
                Token::String("d\\e".into()),
            ]
        );
        let error = tokenize(r#"up{job="x}"#).unwrap_err();
        assert_eq!(error.message, "unterminated string");
        assert!(
            tokenize("up[5x]")
                .unwrap_err()
                .message
                .contains("invalid duration")
        );
        assert_eq!(tokenize("up $ 1").unwrap_err().span, Some(Span::new(3, 4)));
        assert_eq!(parse_duration("1h30m"), Some(Duration::from_secs(5400)));
        assert_eq!(parse_duration("1y"), Some(Duration::from_secs(31_536_000)));
        assert_eq!(parse_duration("5x"), None);
    }
}
