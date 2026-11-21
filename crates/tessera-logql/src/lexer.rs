// SPDX-License-Identifier: Apache-2.0

//! Tokens of `LogQL` text, each with its byte span.

use std::time::Duration;

use tessera_ir::{Diagnostic, Span};

/// A token.
#[derive(Clone, PartialEq, Debug)]
pub enum Token {
    /// An identifier or keyword, as written.
    Ident(String),
    /// A string literal, unescaped.
    String(String),
    /// A number, as written.
    Number(String),
    /// A duration such as `5m` or `1h30m`.
    Duration(Duration),
    /// A byte size such as `2KB` or `1.5MiB`, in bytes.
    Bytes(f64),
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

/// Operators and punctuation, longest first so that `|=` beats `|`.
const SYMBOLS: [&str; 24] = [
    "|=", "|~", "!=", "!~", "=~", "==", ">=", "<=", "{", "}", "(", ")", "[", "]", ",", "|", "=",
    ">", "<", "+", "-", "*", "/", "%",
];

/// Splits `text` into tokens.
///
/// # Errors
///
/// On an unterminated string or a character `LogQL` has no use for.
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
        if c == b'"' || c == b'`' {
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
        if c.is_ascii_alphabetic() || c == b'_' {
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            tokens.push(Spanned {
                token: Token::Ident(text[start..i].to_owned()),
                span: Span::new(start, i),
            });
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

/// A number, a duration or a byte size starting at `start`, and where it
/// ends.
fn number(text: &str, start: usize) -> Result<(Token, usize), Diagnostic> {
    let bytes = text.as_bytes();
    let mut i = start;
    while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
        i += 1;
    }
    let exponent = i < bytes.len()
        && (bytes[i] == b'e' || bytes[i] == b'E')
        && !text[i + 1..].starts_with(|c: char| c.is_ascii_alphabetic());
    if exponent {
        i += 1;
        if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
            i += 1;
        }
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        return Ok((Token::Number(text[start..i].to_owned()), i));
    }
    let number_end = i;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.') {
        i += 1;
    }
    if i == number_end {
        return Ok((Token::Number(text[start..i].to_owned()), i));
    }
    if let Some(duration) = parse_duration(&text[start..i]) {
        return Ok((Token::Duration(duration), i));
    }
    let factor = byte_factor(&text[number_end..i]);
    let value: f64 = text[start..number_end].parse().unwrap_or(f64::NAN);
    if factor == 0.0 || value.is_nan() {
        return Err(Diagnostic::at(
            Span::new(start, i),
            format!("invalid duration or size {}", &text[start..i]),
        ));
    }
    Ok((Token::Bytes(value * factor), i))
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

/// Bytes per unit, or 0 for text that is not a byte unit.
#[must_use]
pub fn byte_factor(unit: &str) -> f64 {
    match unit.to_ascii_lowercase().as_str() {
        "b" => 1.0,
        "kb" => 1e3,
        "mb" => 1e6,
        "gb" => 1e9,
        "tb" => 1e12,
        "kib" => 1024.0,
        "mib" => 1024.0 * 1024.0,
        "gib" => 1024.0 * 1024.0 * 1024.0,
        _ => 0.0,
    }
}

/// `5m`, `1h30m`, `250ms`, `1.5s`.
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
            "us" => 1e-6,
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
    fn lexes_a_log_query() {
        assert_eq!(
            tokens(r#"{app="checkout", level!="debug"} |= "timeout" | json | status >= 500"#),
            vec![
                Token::Symbol("{"),
                Token::Ident("app".into()),
                Token::Symbol("="),
                Token::String("checkout".into()),
                Token::Symbol(","),
                Token::Ident("level".into()),
                Token::Symbol("!="),
                Token::String("debug".into()),
                Token::Symbol("}"),
                Token::Symbol("|="),
                Token::String("timeout".into()),
                Token::Symbol("|"),
                Token::Ident("json".into()),
                Token::Symbol("|"),
                Token::Ident("status".into()),
                Token::Symbol(">="),
                Token::Number("500".into()),
            ]
        );
    }

    #[test]
    fn lexes_durations_numbers_and_raw_strings() {
        assert_eq!(
            tokens("rate({a=~`x.*`}[5m]) * 1.5e3 + 2"),
            vec![
                Token::Ident("rate".into()),
                Token::Symbol("("),
                Token::Symbol("{"),
                Token::Ident("a".into()),
                Token::Symbol("=~"),
                Token::String("x.*".into()),
                Token::Symbol("}"),
                Token::Symbol("["),
                Token::Duration(Duration::from_secs(300)),
                Token::Symbol("]"),
                Token::Symbol(")"),
                Token::Symbol("*"),
                Token::Number("1.5e3".into()),
                Token::Symbol("+"),
                Token::Number("2".into()),
            ]
        );
        assert_eq!(parse_duration("1h30m"), Some(Duration::from_secs(5400)));
        assert_eq!(tokens(r#""a\"b\n""#), vec![Token::String("a\"b\n".into())]);
    }

    #[test]
    fn errors_are_positioned() {
        let error = tokenize("{a=\"x").unwrap_err();
        assert_eq!(error.message, "unterminated string");
        assert_eq!(error.span, Some(Span::new(3, 5)));
        assert!(
            tokenize("[5q]")
                .unwrap_err()
                .message
                .starts_with("invalid duration or size")
        );
        assert_eq!(
            tokens("2KB 1.5MiB"),
            vec![Token::Bytes(2000.0), Token::Bytes(1.5 * 1024.0 * 1024.0)]
        );
        assert_eq!(tokenize("a ? b").unwrap_err().span, Some(Span::new(2, 3)));
    }
}
