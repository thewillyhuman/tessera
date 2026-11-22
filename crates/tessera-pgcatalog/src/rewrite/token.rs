// SPDX-License-Identifier: Apache-2.0

//! The tokens of a statement, and where an operand begins and ends
//! among them.

/// One token of a statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Token {
    /// A name or keyword, as written.
    Word(String),
    /// A quoted name, without its quotes.
    Quoted(String),
    /// A string, without its quotes and escapes.
    Text(String),
    /// A number, as written.
    Number(String),
    /// A parameter, by its position from one.
    Parameter(String),
    /// An operator or a punctuation mark.
    Mark(String),
}

impl Token {
    pub(super) fn mark(text: &str) -> Token {
        Token::Mark(text.to_owned())
    }

    pub(super) fn word(text: &str) -> Token {
        Token::Word(text.to_owned())
    }

    pub(super) fn is_mark(&self, text: &str) -> bool {
        matches!(self, Token::Mark(mark) if mark == text)
    }

    /// Whether this is the word `text`, whatever its case.
    pub(super) fn is_word(&self, text: &str) -> bool {
        matches!(self, Token::Word(word) if word.eq_ignore_ascii_case(text))
    }

    pub(super) fn render(&self, out: &mut String) {
        match self {
            Token::Word(text) | Token::Number(text) | Token::Mark(text) => out.push_str(text),
            Token::Quoted(name) => {
                out.push('"');
                out.push_str(&name.replace('"', "\"\""));
                out.push('"');
            }
            Token::Text(text) => {
                out.push('\'');
                out.push_str(&text.replace('\'', "''"));
                out.push('\'');
            }
            Token::Parameter(position) => {
                out.push('?');
                out.push_str(position);
            }
        }
    }
}

/// The operators made of marks, longest first.
const OPERATORS: [&str; 20] = [
    "!~~*", "!~~", "~~*", "!~*", "::", "~~", "!~", "~*", "<>", "!=", "<=", ">=", "||", "->>", "->",
    "<<", ">>", "@>", "<@", "&&",
];

/// The tokens of `sql`, comments left out. A string written `E'...'`
/// has its backslash escapes resolved.
pub(super) fn tokens(sql: &str) -> Vec<Token> {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '-' && chars.get(i + 1) == Some(&'-') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i < chars.len() && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                i += 1;
            }
            i = (i + 2).min(chars.len());
        } else if c == '\'' {
            let escapes = matches!(out.last(), Some(Token::Word(word)) if word.eq_ignore_ascii_case("e"))
                && i > 0
                && !chars[i - 1].is_whitespace();
            if escapes {
                out.pop();
            }
            let (text, next) = quoted(&chars, i, '\'', escapes);
            out.push(Token::Text(text));
            i = next;
        } else if c == '"' {
            let (name, next) = quoted(&chars, i, '"', false);
            out.push(Token::Quoted(name));
            i = next;
        } else if c == '$' && chars.get(i + 1).is_some_and(char::is_ascii_digit) {
            let start = i + 1;
            i = start;
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
            }
            out.push(Token::Parameter(chars[start..i].iter().collect()));
        } else if c.is_ascii_digit() {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '.') {
                i += 1;
            }
            out.push(Token::Number(chars[start..i].iter().collect()));
        } else if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len()
                && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '$')
            {
                i += 1;
            }
            // An unquoted name is its lower case, as PostgreSQL has it.
            let word: String = chars[start..i].iter().collect();
            out.push(Token::Word(word.to_lowercase()));
        } else {
            let rest: String = chars[i..chars.len().min(i + 4)].iter().collect();
            let operator = OPERATORS.iter().find(|op| rest.starts_with(**op));
            let mark = operator.map_or_else(|| c.to_string(), |op| (*op).to_owned());
            i += mark.chars().count();
            out.push(Token::Mark(mark));
        }
    }
    out
}

/// What is quoted with `quote` from position `start`, where the quote
/// is, and the position after it. A doubled quote stands for one;
/// with `escapes`, a backslash escapes what follows.
fn quoted(chars: &[char], start: usize, quote: char, escapes: bool) -> (String, usize) {
    let mut text = String::new();
    let mut i = start + 1;
    while i < chars.len() {
        let c = chars[i];
        if c == quote {
            if chars.get(i + 1) == Some(&quote) {
                text.push(quote);
                i += 2;
                continue;
            }
            return (text, i + 1);
        }
        if escapes && c == '\\' && i + 1 < chars.len() {
            text.push(match chars[i + 1] {
                'n' => '\n',
                't' => '\t',
                'r' => '\r',
                other => other,
            });
            i += 2;
            continue;
        }
        text.push(c);
        i += 1;
    }
    (text, i)
}

/// Where the operand that ends before `end` starts: a name with its
/// qualifiers, a literal, or what is in brackets with the name of the
/// function that takes it.
pub(super) fn operand_start(tokens: &[Token], end: usize) -> usize {
    if end == 0 {
        return 0;
    }
    let last = end - 1;
    let mut start = last;
    let closes = |open: &str, close: &str| {
        let mut depth = 0usize;
        let mut i = last;
        loop {
            if tokens[i].is_mark(close) {
                depth += 1;
            } else if tokens[i].is_mark(open) {
                depth -= 1;
                if depth == 0 {
                    return i;
                }
            }
            if i == 0 {
                return 0;
            }
            i -= 1;
        }
    };
    if tokens[last].is_mark(")") {
        start = closes("(", ")");
        // A function call takes its name with it; a keyword before a
        // bracket is not a name.
        if start > 0 && is_name(&tokens[start - 1]) && !is_keyword(&tokens[start - 1]) {
            start -= 1;
        }
    } else if tokens[last].is_mark("]") {
        start = closes("[", "]");
        if start > 0 && is_name(&tokens[start - 1]) {
            start -= 1;
        }
    }
    // The qualifiers of a name: `a.b.c`.
    while start >= 2 && tokens[start - 1].is_mark(".") && is_name(&tokens[start - 2]) {
        start -= 2;
    }
    start
}

/// Where the operand that starts at `start` ends, exclusive.
pub(super) fn operand_end(tokens: &[Token], start: usize) -> usize {
    let mut i = start;
    // A name with its qualifiers.
    while i + 2 < tokens.len() && is_name(&tokens[i]) && tokens[i + 1].is_mark(".") {
        i += 2;
    }
    if i >= tokens.len() {
        return tokens.len();
    }
    let brackets = |from: usize| {
        let mut depth = 0usize;
        for (j, token) in tokens.iter().enumerate().skip(from) {
            if token.is_mark("(") {
                depth += 1;
            } else if token.is_mark(")") {
                depth -= 1;
                if depth == 0 {
                    return j + 1;
                }
            }
        }
        tokens.len()
    };
    if tokens[i].is_mark("(") {
        return brackets(i);
    }
    if is_name(&tokens[i]) && tokens.get(i + 1).is_some_and(|t| t.is_mark("(")) {
        return brackets(i + 1);
    }
    i + 1
}

pub(super) fn is_name(token: &Token) -> bool {
    matches!(token, Token::Word(_) | Token::Quoted(_))
}

/// Whether `token` is a keyword a bracket may follow without being a
/// call of it.
pub(super) fn is_keyword(token: &Token) -> bool {
    const KEYWORDS: [&str; 22] = [
        "and", "or", "not", "in", "on", "where", "when", "then", "else", "select", "from", "as",
        "by", "having", "join", "using", "any", "all", "exists", "values", "union", "case",
    ];
    KEYWORDS.iter().any(|keyword| token.is_word(keyword))
}

/// Where the first comma outside brackets is in `tokens`.
pub(super) fn top_level_comma(tokens: &[Token]) -> Option<usize> {
    top_level(tokens, |token| token.is_mark(","))
}

/// Where the first token outside brackets that `is` is in `tokens`.
pub(super) fn top_level(tokens: &[Token], is: impl Fn(&Token) -> bool) -> Option<usize> {
    let mut depth = 0usize;
    tokens.iter().position(|token| {
        if token.is_mark("(") || token.is_mark("[") {
            depth += 1;
        } else if token.is_mark(")") || token.is_mark("]") {
            depth = depth.saturating_sub(1);
        }
        depth == 0 && is(token)
    })
}

/// `tokens` as text, a space between each two.
pub(super) fn rendered(tokens: &[Token]) -> String {
    let mut out = String::new();
    for token in tokens {
        if !out.is_empty() {
            out.push(' ');
        }
        token.render(&mut out);
    }
    out
}

/// One way of rewriting what begins at a token: where it finds its
/// shape at the position it is given, it writes the store's form to
/// the output and says where the tokens go on; it writes nothing
/// where it does not.
pub(super) type Rule = fn(&[Token], usize, &mut Vec<Token>) -> Option<usize>;

/// `tokens` with the first of `rules` that applies at each token
/// applied there; a token no rule takes is kept.
pub(super) fn apply(tokens: &[Token], rules: &[Rule]) -> Vec<Token> {
    let mut out = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        if let Some(next) = rules.iter().find_map(|rule| rule(tokens, i, &mut out)) {
            i = next;
        } else {
            out.push(tokens[i].clone());
            i += 1;
        }
    }
    out
}

/// Pushes `words` to `out`, each a word.
pub(super) fn push_words(out: &mut Vec<Token>, words: &[&str]) {
    out.extend(words.iter().map(|word| Token::word(word)));
}
