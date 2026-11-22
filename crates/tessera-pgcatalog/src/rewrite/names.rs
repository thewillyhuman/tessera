// SPDX-License-Identifier: Apache-2.0

//! The names of a statement's fields, as `PostgreSQL` gives them.

use super::SESSION_VALUES;
use super::token::{Token, is_keyword, is_name, operand_start};

/// Gives every field of a statement that begins `SELECT` the name
/// `PostgreSQL` gives it, as an alias: the store names an expression
/// by its text, where `PostgreSQL` names a call by its function, a
/// cast by what is cast or by its type, and anything else `?column?`.
/// A field that has an alias or is a column keeps its name.
pub(super) fn named(tokens: Vec<Token>) -> Vec<Token> {
    // The fields end at the first FROM outside brackets, or where a
    // clause that follows a list without one begins.
    const AFTER: [&str; 9] = [
        "from",
        "where",
        "group",
        "having",
        "order",
        "limit",
        "union",
        "except",
        "intersect",
    ];
    if !tokens.first().is_some_and(|t| t.is_word("select")) {
        return tokens;
    }
    let mut start = 1;
    if tokens
        .get(1)
        .is_some_and(|t| t.is_word("distinct") || t.is_word("all"))
    {
        start = 2;
    }
    let mut depth = 0usize;
    let mut end = tokens.len();
    let mut items: Vec<(usize, usize)> = Vec::new();
    let mut item = start;
    for (i, token) in tokens.iter().enumerate().skip(start) {
        if token.is_mark("(") || token.is_mark("[") {
            depth += 1;
        } else if token.is_mark(")") || token.is_mark("]") {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && token.is_mark(",") {
            items.push((item, i));
            item = i + 1;
        } else if depth == 0 && AFTER.iter().any(|word| token.is_word(word)) {
            end = i;
            break;
        }
    }
    items.push((item, end));
    let mut out = tokens[..start].to_vec();
    for (n, (from, to)) in items.into_iter().enumerate() {
        if n > 0 {
            out.push(Token::mark(","));
        }
        let field = &tokens[from..to];
        out.extend_from_slice(field);
        if let Some(name) = name_of(field) {
            out.push(Token::word("AS"));
            out.push(Token::Quoted(name));
        }
    }
    out.extend_from_slice(&tokens[end..]);
    out
}

/// The name to give the field `field`, when it has none of its own.
fn name_of(field: &[Token]) -> Option<String> {
    let aliased = {
        let mut depth = 0usize;
        field.iter().any(|token| {
            if token.is_mark("(") {
                depth += 1;
            } else if token.is_mark(")") {
                depth = depth.saturating_sub(1);
            }
            depth == 0 && token.is_word("as")
        })
    };
    // `expression name`: a name right after what ends an expression.
    let implicit = match field {
        [.., before, Token::Word(last) | Token::Quoted(last)] => {
            const ENDS: [&str; 5] = ["end", "null", "true", "false", "not"];
            let ends_expression = match before {
                Token::Mark(mark) => mark == ")" || mark == "]",
                Token::Word(word) => !is_keyword(before) && !word.eq_ignore_ascii_case("distinct"),
                _ => true,
            };
            ends_expression && !ENDS.iter().any(|word| last.eq_ignore_ascii_case(word))
        }
        _ => false,
    };
    if aliased || implicit || field.is_empty() {
        return None;
    }
    expression_name(field)
}

/// The name `PostgreSQL` gives the unnamed expression `field`; none
/// for a column or a star, which the store names the same way.
fn expression_name(field: &[Token]) -> Option<String> {
    // A cast is named by what is cast when that has a name, else by
    // its type.
    if let Some(at) = field.iter().rposition(|t| t.is_mark("::")) {
        // The cast is the field when what it casts begins the field
        // and nothing but its type follows.
        let type_only = field[at + 1..].iter().all(|t| match t {
            Token::Mark(mark) => [".", "(", ")", "[", "]"].contains(&mark.as_str()),
            Token::Text(_) | Token::Parameter(_) => false,
            _ => true,
        });
        let whole = type_only && operand_start(field, at) == 0;
        if whole {
            let inner = match &field[..at] {
                [Token::Word(_) | Token::Quoted(_)] => None,
                [.., dot, Token::Word(column) | Token::Quoted(column)] if dot.is_mark(".") => {
                    Some(column.clone())
                }
                operand => expression_name(operand).filter(|name| name != "?column?"),
            };
            let type_name = field[at + 1..].iter().rev().find_map(|t| match t {
                Token::Word(word) | Token::Quoted(word) => Some(word.to_ascii_lowercase()),
                _ => None,
            });
            return match (&field[..at], inner) {
                ([Token::Word(column) | Token::Quoted(column)], _) => Some(column.clone()),
                (_, Some(name)) => Some(name),
                (_, None) => type_name,
            };
        }
        return Some("?column?".into());
    }
    match field {
        [star] if star.is_mark("*") => None,
        [.., dot, star] if dot.is_mark(".") && star.is_mark("*") => None,
        // A value of the session written bare becomes a call here,
        // and is named as it was written.
        [Token::Word(value)] if SESSION_VALUES.iter().any(|v| value.eq_ignore_ascii_case(v)) => {
            Some(value.to_ascii_lowercase())
        }
        // A column, with or without its qualifiers.
        _ if field.len() % 2 == 1
            && field.iter().enumerate().all(|(i, token)| {
                if i % 2 == 0 {
                    is_name(token)
                } else {
                    token.is_mark(".")
                }
            }) =>
        {
            None
        }
        // A call, with or without its schema: the function's name.
        _ if field.last().is_some_and(|t| t.is_mark(")")) && {
            let start = operand_start(field, field.len());
            start == 0 && !field[0].is_mark("(")
        } =>
        {
            let open = field.iter().position(|t| t.is_mark("("))?;
            match open.checked_sub(1).map(|i| &field[i]) {
                Some(Token::Word(function) | Token::Quoted(function)) => {
                    Some(function.to_ascii_lowercase())
                }
                _ => Some("?column?".into()),
            }
        }
        // A session value written bare is named as it is written.
        _ => Some("?column?".into()),
    }
}
