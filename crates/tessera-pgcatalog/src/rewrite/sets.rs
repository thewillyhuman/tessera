// SPDX-License-Identifier: Apache-2.0

//! Arrays and the sets a FROM can hold besides tables. An array is a
//! JSON list here: `ARRAY[a, b]` and `ARRAY(SELECT ...)` build one,
//! `x[i]` reads one, `= ANY(x)` looks in one, and `unnest(x)`,
//! `generate_series(a, b)` and any other function in a FROM are rows.

use super::token::{
    Token, is_keyword, is_name, operand_end, operand_start, push_words, rendered, top_level_comma,
};

/// What a set in a FROM is called, and its one column.
struct Named {
    set: Token,
    column: Token,
}

/// The name a set in a FROM is given at `at`, after an optional
/// `AS`, and where the tokens go on; `otherwise` when it has none.
/// `name(column)` names the set and its column.
fn alias(tokens: &[Token], at: usize, otherwise: &str) -> (Named, usize) {
    const FOLLOWS: [&str; 12] = [
        "where", "group", "order", "limit", "left", "right", "inner", "cross", "full", "natural",
        "union", "having",
    ];
    let after = at + usize::from(tokens.get(at).is_some_and(|t| t.is_word("as")));
    let named = tokens.get(after).filter(|name| {
        is_name(name) && !is_keyword(name) && !FOLLOWS.iter().any(|word| name.is_word(word))
    });
    let Some(name) = named else {
        let name = Token::word(otherwise);
        return (
            Named {
                set: name.clone(),
                column: name,
            },
            at,
        );
    };
    match tokens.get(after + 1..after + 4) {
        Some([open, column, close])
            if open.is_mark("(") && is_name(column) && close.is_mark(")") =>
        {
            (
                Named {
                    set: name.clone(),
                    column: column.clone(),
                },
                after + 4,
            )
        }
        _ => (
            Named {
                set: name.clone(),
                column: name.clone(),
            },
            after + 1,
        ),
    }
}

/// The statement being rewritten and what is known of its tokens.
struct Context<'a> {
    tokens: &'a [Token],
    /// Whether a set can begin at each token: it follows FROM or
    /// JOIN, or a comma in a FROM, where a comma comes before a set
    /// and not before a field.
    sets: Vec<bool>,
}

impl<'a> Context<'a> {
    fn of(tokens: &'a [Token]) -> Self {
        const ENDS_FROM: [&str; 8] = [
            "select", "where", "group", "order", "having", "limit", "union", "on",
        ];
        let mut in_from = vec![false];
        let mut sets = Vec::with_capacity(tokens.len());
        let mut before: Option<&Token> = None;
        for token in tokens {
            let here = in_from.last().copied().unwrap_or(false);
            sets.push(before.is_some_and(|t| {
                t.is_word("from") || t.is_word("join") || (t.is_mark(",") && here)
            }));
            if token.is_mark("(") {
                in_from.push(false);
            } else if token.is_mark(")") {
                if in_from.len() > 1 {
                    in_from.pop();
                }
            } else if let Some(here) = in_from.last_mut() {
                if token.is_word("from") || token.is_word("join") {
                    *here = true;
                } else if ENDS_FROM.iter().any(|word| token.is_word(word)) {
                    *here = false;
                }
            }
            before = Some(token);
        }
        Context { tokens, sets }
    }

    /// Whether the token at `i` is the word `word` with a bracket
    /// after it.
    fn calls(&self, i: usize, word: &str) -> bool {
        self.tokens[i].is_word(word) && self.tokens.get(i + 1).is_some_and(|t| t.is_mark("("))
    }

    /// What the brackets that open after the token at `i` hold,
    /// rewritten, and where they end.
    fn arguments(&self, i: usize) -> (Vec<Token>, usize) {
        let end = operand_end(self.tokens, i + 1);
        (listed(&self.tokens[i + 2..end - 1]), end)
    }
}

/// One way of rewriting what begins at a token of a statement.
type SetRule = fn(&Context<'_>, usize, &mut Vec<Token>) -> Option<usize>;

/// `ARRAY[a, b]` is the list of a and b.
fn array_of_values(ctx: &Context<'_>, i: usize, out: &mut Vec<Token>) -> Option<usize> {
    let tokens = ctx.tokens;
    if !(tokens[i].is_word("array") && tokens.get(i + 1)?.is_mark("[")) {
        return None;
    }
    let end = closing_bracket(tokens, i + 1);
    out.push(Token::word("json_array"));
    out.push(Token::mark("("));
    out.extend(listed(&tokens[i + 2..end]));
    out.push(Token::mark(")"));
    Some(end + 1)
}

/// Where the square bracket that opens at `open` closes.
fn closing_bracket(tokens: &[Token], open: usize) -> usize {
    let mut depth = 0usize;
    for (i, token) in tokens.iter().enumerate().skip(open) {
        if token.is_mark("[") {
            depth += 1;
        } else if token.is_mark("]") {
            depth -= 1;
            if depth == 0 {
                return i;
            }
        }
    }
    tokens.len()
}

/// `ARRAY(SELECT ...)` is the list of what the query returns.
fn array_of_query(ctx: &Context<'_>, i: usize, out: &mut Vec<Token>) -> Option<usize> {
    if !(ctx.calls(i, "array") && ctx.tokens.get(i + 2)?.is_word("select")) {
        return None;
    }
    let (query, end) = ctx.arguments(i);
    out.push(Token::mark("("));
    push_words(out, &["WITH", "pg_list"]);
    out.push(Token::mark("("));
    out.push(Token::word("v"));
    out.push(Token::mark(")"));
    out.push(Token::word("AS"));
    out.push(Token::mark("("));
    out.extend(query);
    out.push(Token::mark(")"));
    push_words(out, &["SELECT", "json_group_array"]);
    out.push(Token::mark("("));
    out.push(Token::word("v"));
    out.push(Token::mark(")"));
    push_words(out, &["FROM", "pg_list"]);
    out.push(Token::mark(")"));
    Some(end)
}

/// `name[i]` is the i-th of a list.
fn subscript(ctx: &Context<'_>, i: usize, out: &mut Vec<Token>) -> Option<usize> {
    let tokens = ctx.tokens;
    let subscripts = tokens[i].is_mark("[") && (is_name(out.last()?) || out.last()?.is_mark(")"));
    if !subscripts {
        return None;
    }
    let end = closing_bracket(tokens, i);
    let operand = out.split_off(operand_start(out, out.len()));
    out.push(Token::word("pg_element"));
    out.push(Token::mark("("));
    out.extend(operand);
    out.push(Token::mark(","));
    out.extend(listed(&tokens[i + 1..end]));
    out.push(Token::mark(")"));
    Some(end + 1)
}

/// The numbers from the first of `bounds` to the second, as a query
/// whose one column is `v`.
fn series(bounds: &[Token]) -> Option<Token> {
    let comma = top_level_comma(bounds)?;
    Some(Token::Word(format!(
        "WITH RECURSIVE pg_series ( v ) AS ( SELECT {from} UNION ALL SELECT v + 1 \
         FROM pg_series WHERE v < ( {to} ) ) SELECT v",
        from = rendered(&bounds[..comma]),
        to = rendered(&bounds[comma + 1..]),
    )))
}

/// `SELECT generate_series(a, b) [AS] name` is the numbers from a to
/// b, a row each, where a query selects nothing else.
fn series_selected(ctx: &Context<'_>, i: usize, out: &mut Vec<Token>) -> Option<usize> {
    if !(ctx.calls(i, "generate_series") && out.last()?.is_word("select")) {
        return None;
    }
    let (bounds, end) = ctx.arguments(i);
    let series = series(&bounds)?;
    let (name, after) = alias(ctx.tokens, end, "generate_series");
    push_words(out, &["v", "AS"]);
    out.push(name.column);
    out.push(Token::word("FROM"));
    out.push(Token::mark("("));
    out.push(series);
    push_words(out, &["FROM", "pg_series"]);
    out.push(Token::mark(")"));
    Some(after)
}

/// `generate_series(a, b) [AS] name`, in a FROM, is the numbers from
/// a to b in a column called name.
fn series_set(ctx: &Context<'_>, i: usize, out: &mut Vec<Token>) -> Option<usize> {
    if !(ctx.calls(i, "generate_series") && ctx.sets[i]) {
        return None;
    }
    let (bounds, end) = ctx.arguments(i);
    let series = series(&bounds)?;
    let (name, after) = alias(ctx.tokens, end, "generate_series");
    out.push(Token::mark("("));
    out.push(series);
    out.push(Token::word("AS"));
    out.push(name.column);
    push_words(out, &["FROM", "pg_series"]);
    out.push(Token::mark(")"));
    out.push(name.set);
    Some(after)
}

/// `unnest(x) [AS] name`, in a FROM, is the rows of the list x in a
/// column called name.
fn unnested_set(ctx: &Context<'_>, i: usize, out: &mut Vec<Token>) -> Option<usize> {
    if !(ctx.calls(i, "unnest") && ctx.sets[i]) {
        return None;
    }
    let (list, end) = ctx.arguments(i);
    let (name, after) = alias(ctx.tokens, end, "unnest");
    out.push(Token::mark("("));
    push_words(out, &["SELECT", "value", "AS"]);
    out.push(name.column);
    out.extend(rows_of(list));
    out.push(Token::mark(")"));
    out.push(name.set);
    Some(after)
}

/// `FROM json_each(pg_array(list))`: the elements of `list`, a row
/// each, in a column called `value`.
fn rows_of(list: Vec<Token>) -> Vec<Token> {
    let mut out = vec![Token::word("FROM"), Token::word("json_each")];
    out.push(Token::mark("("));
    out.push(Token::word("pg_array"));
    out.push(Token::mark("("));
    out.extend(list);
    out.push(Token::mark(")"));
    out.push(Token::mark(")"));
    out
}

/// Any other function in a FROM is one row with what it returns, in
/// a column called as the set is.
fn function_set(ctx: &Context<'_>, i: usize, out: &mut Vec<Token>) -> Option<usize> {
    let token = &ctx.tokens[i];
    let Token::Word(function) = token else {
        return None;
    };
    let is_function = ctx.sets[i]
        && ctx.calls(i, function)
        && !is_keyword(token)
        && function != "lateral"
        && function != "pg_get_keywords";
    if !is_function {
        return None;
    }
    let (arguments, end) = ctx.arguments(i);
    let (name, after) = alias(ctx.tokens, end, function);
    out.push(Token::mark("("));
    out.push(Token::word("SELECT"));
    out.push(token.clone());
    out.push(Token::mark("("));
    out.extend(arguments);
    out.push(Token::mark(")"));
    out.push(Token::word("AS"));
    out.push(name.column);
    out.push(Token::mark(")"));
    out.push(name.set);
    Some(after)
}

/// `= ANY(x)` is `IN` the list x or the query x, and `<> ALL(x)` is
/// `NOT IN` it.
fn quantified(ctx: &Context<'_>, i: usize, out: &mut Vec<Token>) -> Option<usize> {
    let quantifies = ["any", "all", "some"].iter().any(|word| ctx.calls(i, word));
    let compared = out.last()?;
    let negated = compared.is_mark("<>") || compared.is_mark("!=");
    if !(quantifies && (negated || compared.is_mark("="))) {
        return None;
    }
    out.pop();
    if negated {
        out.push(Token::word("NOT"));
    }
    out.push(Token::word("IN"));
    let (inner, end) = ctx.arguments(i);
    out.push(Token::mark("("));
    if inner.first().is_some_and(|t| t.is_word("select")) {
        out.extend(inner);
    } else {
        push_words(out, &["SELECT", "value"]);
        out.extend(rows_of(inner));
    }
    out.push(Token::mark(")"));
    Some(end)
}

/// `tokens` with arrays as lists and the sets of a FROM as queries.
pub(super) fn listed(tokens: &[Token]) -> Vec<Token> {
    const RULES: [SetRule; 8] = [
        array_of_values,
        array_of_query,
        subscript,
        series_selected,
        series_set,
        unnested_set,
        function_set,
        quantified,
    ];
    let ctx = Context::of(tokens);
    let mut out = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        if let Some(next) = RULES.iter().find_map(|rule| rule(&ctx, i, &mut out)) {
            i = next;
        } else {
            out.push(tokens[i].clone());
            i += 1;
        }
    }
    out
}
