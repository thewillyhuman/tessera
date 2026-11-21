// SPDX-License-Identifier: Apache-2.0

//! Tokens of SQL text, each with its byte span.
//!
//! Identifiers are case-insensitive unless double-quoted; keywords are
//! identifiers the parser recognizes, so `select` and `SELECT` are the
//! same token. String literals use single quotes with `''` for a quote.

use tessera_ir::{Diagnostic, Span};

/// A token.
#[derive(Clone, PartialEq, Debug)]
pub enum Token {
    /// A bare identifier, lowercased, or a quoted identifier as written.
    Ident {
        /// The identifier.
        name: String,
        /// Whether it was double-quoted.
        quoted: bool,
    },
    /// A numeric literal, as written.
    Number(String),
    /// A string literal, unescaped.
    String(String),
    /// Punctuation or an operator.
    Symbol(&'static str),
    /// A query parameter `$n`, numbered from 1 in the order the client
    /// binds values.
    Parameter(usize),
}

/// A token and where it came from.
#[derive(Clone, PartialEq, Debug)]
pub struct Spanned {
    /// The token.
    pub token: Token,
    /// Its bytes in the text.
    pub span: Span,
}

const SYMBOLS: [&str; 18] = [
    "<>", "!=", "<=", ">=", "::", "=", "<", ">", "+", "-", "*", "/", "%", "(", ")", ",", ".", ";",
];

/// Splits `text` into tokens.
///
/// # Errors
///
/// On an unterminated string or quoted identifier, or a character SQL
/// has no use for.
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
        if c == b'-' && bytes.get(i + 1) == Some(&b'-') {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        let start = i;
        if c == b'\'' || c == b'"' {
            let (value, end) = quoted(text, i, c)?;
            let token = if c == b'\'' {
                Token::String(value)
            } else {
                Token::Ident {
                    name: value,
                    quoted: true,
                }
            };
            tokens.push(Spanned {
                token,
                span: Span::new(start, end),
            });
            i = end;
            continue;
        }
        if c.is_ascii_digit() || (c == b'.' && bytes.get(i + 1).is_some_and(u8::is_ascii_digit)) {
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.') {
                i += 1;
            }
            tokens.push(Spanned {
                token: Token::Number(text[start..i].to_owned()),
                span: Span::new(start, i),
            });
            continue;
        }
        if c == b'$' {
            let mut end = i + 1;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            let number = text[i + 1..end].parse::<usize>().ok().filter(|n| *n > 0);
            let Some(number) = number else {
                return Err(Diagnostic::at(
                    Span::new(start, end),
                    "a parameter is $1, $2, ... in the order of binding",
                ));
            };
            tokens.push(Spanned {
                token: Token::Parameter(number),
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
                token: Token::Ident {
                    name: text[start..i].to_ascii_lowercase(),
                    quoted: false,
                },
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

/// The content of a quoted token starting at `start` with `quote`, and
/// the byte after its closing quote.
fn quoted(text: &str, start: usize, quote: u8) -> Result<(String, usize), Diagnostic> {
    let bytes = text.as_bytes();
    let mut value = String::new();
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                value.push(char::from(quote));
                i += 2;
                continue;
            }
            return Ok((value, i + 1));
        }
        let ch = text[i..].chars().next().unwrap_or('?');
        value.push(ch);
        i += ch.len_utf8();
    }
    let what = if quote == b'\'' {
        "string"
    } else {
        "quoted identifier"
    };
    Err(Diagnostic::at(
        Span::new(start, bytes.len()),
        format!("unterminated {what}"),
    ))
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

    fn ident(name: &str) -> Token {
        Token::Ident {
            name: name.into(),
            quoted: false,
        }
    }

    #[test]
    fn lowercases_bare_identifiers_and_keeps_quoted_ones() {
        assert_eq!(
            tokens(r#"SELECT Body, "Mixed.Case" FROM logs"#),
            vec![
                ident("select"),
                ident("body"),
                Token::Symbol(","),
                Token::Ident {
                    name: "Mixed.Case".into(),
                    quoted: true
                },
                ident("from"),
                ident("logs"),
            ]
        );
    }

    #[test]
    fn strings_unescape_doubled_quotes() {
        assert_eq!(
            tokens("'it''s' 'ünïcode'"),
            vec![
                Token::String("it's".into()),
                Token::String("ünïcode".into())
            ]
        );
    }

    #[test]
    fn numbers_symbols_and_comments() {
        assert_eq!(
            tokens("a >= 1.5e3 -- trailing\n<> .5::int"),
            vec![
                ident("a"),
                Token::Symbol(">="),
                Token::Number("1.5e3".into()),
                Token::Symbol("<>"),
                Token::Number(".5".into()),
                Token::Symbol("::"),
                ident("int"),
            ]
        );
    }

    #[test]
    fn spans_point_at_the_source() {
        let spanned = tokenize("ab 'c'").unwrap();
        assert_eq!(spanned[0].span, Span::new(0, 2));
        assert_eq!(spanned[1].span, Span::new(3, 6));
    }

    #[test]
    fn errors_are_positioned() {
        let error = tokenize("select 'open").unwrap_err();
        assert_eq!(error.message, "unterminated string");
        assert_eq!(error.span, Some(Span::new(7, 12)));
        let error = tokenize("a # b").unwrap_err();
        assert_eq!(error.span, Some(Span::new(2, 3)));
    }

    #[test]
    fn parameters_are_numbered_from_one() {
        assert_eq!(
            tokens("x = $1 AND y < $12"),
            vec![
                Token::Ident {
                    name: "x".into(),
                    quoted: false
                },
                Token::Symbol("="),
                Token::Parameter(1),
                Token::Ident {
                    name: "and".into(),
                    quoted: false
                },
                Token::Ident {
                    name: "y".into(),
                    quoted: false
                },
                Token::Symbol("<"),
                Token::Parameter(12),
            ]
        );
        assert!(tokenize("x = $0").is_err(), "parameters start at $1");
        assert!(tokenize("x = $").is_err(), "a bare dollar is nothing");
    }
}
