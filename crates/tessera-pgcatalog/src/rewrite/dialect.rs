// SPDX-License-Identifier: Apache-2.0

//! `PostgreSQL`'s words for what the store says otherwise: what is
//! under `pg_catalog`, casts, the pattern operators, the functions
//! written with words between their arguments and the session's
//! values.

use super::SESSION_VALUES;
use super::token::{Rule, Token, apply, is_name, operand_end, operand_start, top_level};

/// What a cast to the type named `name` becomes around its operand:
/// the function that takes it, or the type the store casts it to;
/// neither where the store needs no cast.
fn cast(name: &str) -> (Option<&'static str>, Option<&'static str>) {
    match name {
        "regclass" => (Some("regclass"), None),
        "regtype" => (Some("regtype"), None),
        "regnamespace" => (Some("regnamespace"), None),
        "bool" | "boolean" => (Some("pg_bool"), None),
        "text" | "varchar" | "name" | "char" | "bpchar" | "character" | "cstring" => {
            (Some("CAST"), Some("TEXT"))
        }
        "int2" | "int4" | "int8" | "int" | "integer" | "bigint" | "smallint" | "oid" | "xid" => {
            (Some("CAST"), Some("INTEGER"))
        }
        "float4" | "float8" | "numeric" | "real" | "double" | "decimal" => {
            (Some("CAST"), Some("REAL"))
        }
        _ => (None, None),
    }
}

/// `operand` cast to the type named `name`, an array type when
/// `array`: in the brackets of the function or the cast the store
/// has for it, or alone in brackets where it has none.
fn cast_of(operand: &[Token], name: &str, array: bool) -> Vec<Token> {
    let (function, target) = if array { (None, None) } else { cast(name) };
    let mut out: Vec<Token> = function.map(Token::word).into_iter().collect();
    out.push(Token::mark("("));
    out.extend_from_slice(operand);
    if let Some(target) = target {
        out.push(Token::word("AS"));
        out.push(Token::word(target));
    }
    out.push(Token::mark(")"));
    out
}

/// `CAST(x AS type)` with the type as the store is to cast to it.
fn explicit_cast(tokens: &[Token], i: usize, out: &mut Vec<Token>) -> Option<usize> {
    if !(tokens[i].is_word("cast") && tokens.get(i + 1)?.is_mark("(")) {
        return None;
    }
    let end = operand_end(tokens, i + 1);
    let inner = &tokens[i + 2..end - 1];
    let at = inner.len() - 1 - top_level(&reversed(inner), |t| t.is_word("as"))?;
    let named = &inner[at + 1..];
    let name = named.iter().find_map(|t| match t {
        Token::Word(word) if word != "pg_catalog" => Some(word.clone()),
        Token::Quoted(name) => Some(name.clone()),
        _ => None,
    })?;
    let array = named.iter().any(|t| t.is_mark("["));
    out.extend(cast_of(&explicit_casts(&inner[..at]), &name, array));
    Some(end)
}

/// `tokens` backwards, each bracket as the one that closes it, so
/// that what is outside brackets is found from the end.
fn reversed(tokens: &[Token]) -> Vec<Token> {
    tokens
        .iter()
        .rev()
        .map(|token| match token {
            Token::Mark(mark) => Token::mark(match mark.as_str() {
                "(" => ")",
                ")" => "(",
                "[" => "]",
                "]" => "[",
                other => other,
            }),
            other => other.clone(),
        })
        .collect()
}

fn explicit_casts(tokens: &[Token]) -> Vec<Token> {
    apply(tokens, &[explicit_cast])
}

/// Rewrites every `operand::type` in `tokens`.
fn casts(mut tokens: Vec<Token>) -> Vec<Token> {
    while let Some(at) = tokens.iter().position(|t| t.is_mark("::")) {
        // The type: an optional schema, a name of one or more words,
        // an optional length and optional array brackets.
        let mut end = at + 1;
        if end + 1 < tokens.len() && is_name(&tokens[end]) && tokens[end + 1].is_mark(".") {
            end += 2;
        }
        let name = match tokens.get(end) {
            Some(Token::Word(name) | Token::Quoted(name)) => name.clone(),
            _ => String::new(),
        };
        end = (end + 1).min(tokens.len());
        while matches!(tokens.get(end), Some(Token::Word(word))
            if ["varying", "precision", "with", "without", "time", "zone"].contains(&word.as_str()))
        {
            end += 1;
        }
        if tokens.get(end).is_some_and(|t| t.is_mark("(")) {
            end = operand_end(&tokens, end);
        }
        let mut array = false;
        while tokens.get(end).is_some_and(|t| t.is_mark("["))
            && tokens.get(end + 1).is_some_and(|t| t.is_mark("]"))
        {
            array = true;
            end += 2;
        }
        let start = operand_start(&tokens, at);
        let mut next = tokens[..start].to_vec();
        next.extend(cast_of(&tokens[start..at], &name, array));
        next.extend_from_slice(&tokens[end..]);
        tokens = next;
    }
    tokens
}

/// `OPERATOR(pg_catalog.~)` is the operator.
fn operator_form(tokens: &[Token], i: usize, out: &mut Vec<Token>) -> Option<usize> {
    if !(tokens[i].is_word("operator") && tokens.get(i + 1)?.is_mark("(")) {
        return None;
    }
    let close = operand_end(tokens, i + 1);
    let operator = tokens[i + 1..close]
        .iter()
        .rev()
        .find(|t| matches!(t, Token::Mark(mark) if mark != ")" && mark != "." && mark != "("))?;
    out.push(operator.clone());
    Some(close)
}

/// `COLLATE x` and `COLLATE pg_catalog.x` are nothing.
fn collation(tokens: &[Token], i: usize, _: &mut Vec<Token>) -> Option<usize> {
    if !tokens[i].is_word("collate") {
        return None;
    }
    let qualified = tokens.get(i + 2).is_some_and(|t| t.is_mark("."));
    Some(i + if qualified { 4 } else { 2 })
}

/// `pg_catalog.f(...)` and `information_schema.f(...)` are `f(...)`.
fn qualified_call(tokens: &[Token], i: usize, _: &mut Vec<Token>) -> Option<usize> {
    let qualifies = (tokens[i].is_word("pg_catalog") || tokens[i].is_word("information_schema"))
        && tokens.get(i + 1)?.is_mark(".")
        && is_name(tokens.get(i + 2)?)
        && tokens.get(i + 3)?.is_mark("(");
    qualifies.then_some(i + 2)
}

/// The set of keywords is a table here: `pg_get_keywords()` is its
/// name.
fn keywords(tokens: &[Token], i: usize, out: &mut Vec<Token>) -> Option<usize> {
    let called = tokens[i].is_word("pg_get_keywords")
        && tokens.get(i + 1)?.is_mark("(")
        && tokens.get(i + 2)?.is_mark(")");
    called.then(|| {
        out.push(tokens[i].clone());
        i + 3
    })
}

/// `substring(x from a for b)` is `substr(x, a, b)`, and
/// `trim(both c from x)` is `trim(x, c)`.
fn worded_call(tokens: &[Token], i: usize, out: &mut Vec<Token>) -> Option<usize> {
    let worded = (tokens[i].is_word("substring") || tokens[i].is_word("trim"))
        && tokens.get(i + 1)?.is_mark("(");
    worded.then(|| {
        let end = operand_end(tokens, i + 1);
        let inner = unqualified(&tokens[i + 2..end - 1]);
        out.extend(spelled(&tokens[i], &inner));
        end
    })
}

/// A call of `name` with `arguments`.
fn call(name: &str, arguments: &[&[Token]]) -> Vec<Token> {
    let mut out = vec![Token::word(name), Token::mark("(")];
    for (n, argument) in arguments.iter().enumerate() {
        if n > 0 {
            out.push(Token::mark(","));
        }
        out.extend_from_slice(argument);
    }
    out.push(Token::mark(")"));
    out
}

/// A call of `substring` or `trim` with `PostgreSQL`'s words between
/// its arguments, as a call with commas; `inner` is what its
/// brackets hold.
fn spelled(function: &Token, inner: &[Token]) -> Vec<Token> {
    let at = |tokens: &[Token], word: &str| top_level(tokens, |t| t.is_word(word));
    if function.is_word("substring") {
        let one = [Token::Number("1".into())];
        return match (at(inner, "from"), at(inner, "for")) {
            (Some(from), Some(length)) if from < length => call(
                "substr",
                &[
                    &inner[..from],
                    &inner[from + 1..length],
                    &inner[length + 1..],
                ],
            ),
            (Some(from), None) => call("substr", &[&inner[..from], &inner[from + 1..]]),
            (None, Some(length)) => call("substr", &[&inner[..length], &one, &inner[length + 1..]]),
            _ => call("substr", &[inner]),
        };
    }
    let (name, rest) = match inner.first() {
        Some(side) if side.is_word("leading") => ("ltrim", &inner[1..]),
        Some(side) if side.is_word("trailing") => ("rtrim", &inner[1..]),
        Some(side) if side.is_word("both") => ("trim", &inner[1..]),
        _ => ("trim", inner),
    };
    match at(rest, "from") {
        Some(0) => call(name, &[&rest[1..]]),
        Some(from) => call(name, &[&rest[from + 1..], &rest[..from]]),
        None => call(name, &[rest]),
    }
}

/// Whether the token at `i` follows a dot: a column, or a member of
/// a schema.
fn after_dot(tokens: &[Token], i: usize) -> bool {
    i > 0 && tokens[i - 1].is_mark(".")
}

/// Whether a bracket or a dot follows the token at `i`.
fn is_called(tokens: &[Token], i: usize) -> bool {
    tokens.get(i + 1).is_some_and(|t| t.is_mark("("))
}

/// The values of the session are functions here: `current_user` is
/// `current_user()`, and `user` alone is the same unless it names a
/// field.
fn session_value(tokens: &[Token], i: usize, out: &mut Vec<Token>) -> Option<usize> {
    if after_dot(tokens, i) || is_called(tokens, i) {
        return None;
    }
    let user = tokens[i].is_word("user")
        && !(i > 0 && tokens[i - 1].is_word("as"))
        && !tokens.get(i + 1).is_some_and(|t| t.is_mark("."));
    let name = if user {
        "current_user"
    } else {
        SESSION_VALUES
            .iter()
            .find(|value| tokens[i].is_word(value))?
    };
    out.extend(call(name, &[]));
    Some(i + 1)
}

/// The functions and operators the store has under another name; a
/// list is what an array is here, and a boolean is one or zero.
fn renamed(tokens: &[Token], i: usize, out: &mut Vec<Token>) -> Option<usize> {
    const FUNCTIONS: [(&str, &str); 5] = [
        ("json_build_object", "json_object"),
        ("jsonb_build_object", "json_object"),
        ("array_agg", "json_group_array"),
        ("bool_and", "min"),
        ("bool_or", "max"),
    ];
    if tokens[i].is_word("ilike") {
        out.push(Token::word("LIKE"));
        return Some(i + 1);
    }
    let (_, name) = FUNCTIONS.iter().find(|(from, _)| tokens[i].is_word(from))?;
    (is_called(tokens, i) && !after_dot(tokens, i)).then(|| {
        out.push(Token::word(name));
        i + 1
    })
}

/// `IS [NOT] DISTINCT FROM` is `IS [NOT]`, the other way round.
fn distinct_from(tokens: &[Token], i: usize, out: &mut Vec<Token>) -> Option<usize> {
    if !(tokens[i].is_word("distinct") && tokens.get(i + 1)?.is_word("from")) {
        return None;
    }
    let negated = out.last().is_some_and(|t| t.is_word("not"));
    let is_at = out.len().checked_sub(1 + usize::from(negated))?;
    if !out[is_at].is_word("is") {
        return None;
    }
    out.truncate(is_at + 1);
    if !negated {
        out.push(Token::word("NOT"));
    }
    Some(i + 2)
}

/// Drops what the store has no word for and says the rest its way.
fn unqualified(tokens: &[Token]) -> Vec<Token> {
    const RULES: [Rule; 8] = [
        operator_form,
        collation,
        qualified_call,
        keywords,
        worded_call,
        session_value,
        renamed,
        distinct_from,
    ];
    apply(tokens, &RULES)
}

/// The pattern operators as the store names them: `~` is `REGEXP`,
/// `~~` is `LIKE`, and the ones that ignore case say so in the
/// pattern.
fn pattern_operator(tokens: &[Token], i: usize, out: &mut Vec<Token>) -> Option<usize> {
    let Token::Mark(mark) = &tokens[i] else {
        return None;
    };
    let (name, ignores_case) = match mark.as_str() {
        "~" => ("REGEXP", false),
        "!~" => ("NOT REGEXP", false),
        "~*" => ("REGEXP", true),
        "!~*" => ("NOT REGEXP", true),
        "~~" | "~~*" => ("LIKE", false),
        "!~~" | "!~~*" => ("NOT LIKE", false),
        _ => return None,
    };
    out.push(Token::word(name));
    if !ignores_case {
        return Some(i + 1);
    }
    let end = operand_end(tokens, i + 1);
    out.push(Token::mark("("));
    out.push(Token::Text("(?i)".into()));
    out.push(Token::mark("||"));
    out.extend_from_slice(&tokens[i + 1..end]);
    out.push(Token::mark(")"));
    Some(end)
}

/// `tokens` with `PostgreSQL`'s words as the store's.
pub(super) fn worded(tokens: &[Token]) -> Vec<Token> {
    let cast = casts(explicit_casts(&unqualified(tokens)));
    apply(&cast, &[pattern_operator])
}
