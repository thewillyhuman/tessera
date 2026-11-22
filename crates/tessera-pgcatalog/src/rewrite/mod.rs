// SPDX-License-Identifier: Apache-2.0

//! `PostgreSQL` as clients write it for the catalogs, rewritten into
//! what the store that holds them reads: casts, the operators and
//! functions under `pg_catalog`, arrays, the sets of a FROM and the
//! session's values.
//!
//! The rewrite works on tokens, so that nothing inside a string or a
//! quoted name is touched, and it is deliberately small: it knows the
//! shapes catalog queries are written in, not the language. An
//! unquoted name is read in lower case, as `PostgreSQL` reads it.

mod dialect;
mod names;
mod sets;
mod token;

use names::named;
use token::{Token, rendered, tokens};

/// A name or keyword of a statement, for telling what it is about.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Word {
    /// The word, in lower case.
    pub text: String,
    /// Whether it follows a dot: a column or a member of a schema.
    /// The members of `pg_catalog` and `information_schema` do not
    /// count as qualified, since those schemas are what is asked for.
    pub qualified: bool,
    /// Whether an opening bracket follows it.
    pub called: bool,
}

/// The names of the fields of a statement that begins `SELECT`, as
/// far as they can be read from its text: an alias, a column, or what
/// [`rewrite`] would name the field.
#[must_use]
pub fn field_names(sql: &str) -> Vec<String> {
    let tokens = named(tokens(sql));
    let mut names = Vec::new();
    let mut depth = 0usize;
    let mut last: Option<String> = None;
    for token in tokens.iter().skip(1) {
        if token.is_mark("(") || token.is_mark("[") {
            depth += 1;
        } else if token.is_mark(")") || token.is_mark("]") {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && (token.is_mark(",") || token.is_word("from")) {
            names.extend(last.take());
            if token.is_word("from") {
                return names;
            }
        } else if depth == 0
            && let Token::Word(name) | Token::Quoted(name) = token
        {
            last = Some(name.clone());
        }
    }
    names.extend(last);
    names
}

/// How many parameters `sql` takes: the highest `$n` it names.
#[must_use]
pub fn parameters(sql: &str) -> usize {
    tokens(sql)
        .iter()
        .filter_map(|token| match token {
            Token::Parameter(position) => position.parse().ok(),
            _ => None,
        })
        .max()
        .unwrap_or(0)
}

/// The unquoted words of `sql`, in order.
pub(crate) fn words(sql: &str) -> Vec<Word> {
    let tokens = tokens(sql);
    tokens
        .iter()
        .enumerate()
        .filter_map(|(i, token)| {
            let Token::Word(word) = token else {
                return None;
            };
            let after_dot = i >= 1 && tokens[i - 1].is_mark(".");
            let of_catalogs = i >= 2
                && (tokens[i - 2].is_word("pg_catalog")
                    || tokens[i - 2].is_word("information_schema"));
            Some(Word {
                text: word.to_ascii_lowercase(),
                qualified: after_dot && !of_catalogs,
                called: tokens.get(i + 1).is_some_and(|t| t.is_mark("(")),
            })
        })
        .collect()
}

/// `sql` as the store that holds the catalogs reads it.
#[must_use]
pub fn rewrite(sql: &str) -> String {
    let mut tokens = tokens(sql);
    if tokens.last().is_some_and(|t| t.is_mark(";")) {
        tokens.pop();
    }
    if let Some(shown) = show(&tokens) {
        return shown;
    }
    rendered(&translated(&named(tokens)))
}

/// `tokens` in the store's dialect.
fn translated(tokens: &[Token]) -> Vec<Token> {
    sets::listed(&dialect::worded(tokens))
}

/// `SHOW name` as a query for the setting.
fn show(tokens: &[Token]) -> Option<String> {
    if !tokens.first()?.is_word("show") {
        return None;
    }
    let words: Vec<String> = tokens[1..]
        .iter()
        .filter_map(|token| match token {
            Token::Word(word) | Token::Quoted(word) => Some(word.to_ascii_lowercase()),
            _ => None,
        })
        .collect();
    let name = match words.join(" ").as_str() {
        "all" => {
            return Some(
                "SELECT name, setting, short_desc AS description FROM pg_catalog.pg_settings \
                 ORDER BY name"
                    .to_owned(),
            );
        }
        "transaction isolation level" => "transaction_isolation".to_owned(),
        "time zone" => "timezone".to_owned(),
        other => other.replace(' ', "_"),
    };
    Some(format!(
        "SELECT current_setting('{name}') AS \"{name}\"",
        name = name.replace(['\'', '"'], "")
    ))
}

/// The values of the session `PostgreSQL` lets a statement name
/// without brackets.
const SESSION_VALUES: [&str; 6] = [
    "current_user",
    "session_user",
    "current_catalog",
    "current_role",
    "current_schema",
    "current_database",
];

#[cfg(test)]
mod tests {
    use super::*;

    /// `sql` in the store's dialect, its fields left unnamed.
    fn plain(sql: &str) -> String {
        rendered(&translated(&tokens(sql.trim_end_matches(';'))))
    }

    #[test]
    fn casts_wrap_their_operand() {
        assert_eq!(
            plain("SELECT 'pg_namespace'::regclass, c.oid::text, f(a, b)::int4"),
            "select regclass ( 'pg_namespace' ) , CAST ( c . oid AS TEXT ) , CAST ( f ( a , b ) AS INTEGER )"
        );
        assert_eq!(
            plain("select c.reloftype::pg_catalog.regtype::pg_catalog.text"),
            "select CAST ( regtype ( c . reloftype ) AS TEXT )"
        );
        assert_eq!(
            plain("select current_setting('server_version_num')::int/100"),
            "select CAST ( current_setting ( 'server_version_num' ) AS INTEGER ) / 100"
        );
        assert_eq!(
            plain("select x::character varying(64), y::text[], ($1)::oid"),
            "select CAST ( x AS TEXT ) , ( y ) , CAST ( ( ?1 ) AS INTEGER )"
        );
    }

    #[test]
    fn what_is_under_pg_catalog_is_called_without_it() {
        assert_eq!(
            plain(
                "SELECT pg_catalog.pg_get_userbyid(c.relowner) FROM pg_catalog.pg_class c \
                 WHERE c.relname OPERATOR(pg_catalog.~) '^(logs)$' COLLATE pg_catalog.default \
                 AND pg_catalog.pg_table_is_visible(c.oid);"
            ),
            "select pg_get_userbyid ( c . relowner ) from pg_catalog . pg_class c \
             where c . relname REGEXP '^(logs)$' and pg_table_is_visible ( c . oid )"
        );
    }

    #[test]
    fn pattern_operators_are_named() {
        assert_eq!(
            plain("select a !~ '^pg_', b ~* 'x', c ~~ 'y%', d ILIKE 'z', e !~~ 'w'"),
            "select a NOT REGEXP '^pg_' , b REGEXP ( '(?i)' || 'x' ) , c LIKE 'y%' , d LIKE 'z' , e NOT LIKE 'w'"
        );
    }

    #[test]
    fn arrays_are_lists() {
        assert_eq!(
            plain("select 1 where n.nspname = ANY (current_schemas(true))"),
            "select 1 where n . nspname IN ( SELECT value FROM json_each ( pg_array ( current_schemas ( true ) ) ) )"
        );
        assert_eq!(
            plain("select 1 where k <> ALL (ARRAY['i', 'c']) and o = any($1)"),
            "select 1 where k NOT IN ( SELECT value FROM json_each ( pg_array ( json_array ( 'i' , 'c' ) ) ) ) \
             and o IN ( SELECT value FROM json_each ( pg_array ( ?1 ) ) )"
        );
        assert_eq!(
            plain("select 1 where a = any (select b from t)"),
            "select 1 where a IN ( select b from t )"
        );
        assert_eq!(
            plain("select array(select n from r where o = any (p.roles) order by 1)"),
            "select ( WITH pg_list ( v ) AS ( select n from r where o IN ( SELECT value FROM json_each \
             ( pg_array ( p . roles ) ) ) order by 1 ) SELECT json_group_array ( v ) FROM pg_list )"
        );
        assert_eq!(
            plain(
                "select string_agg(a, ', ') from generate_series(0, array_upper(p.attrs, 1)) s, t where n = attrs[s]"
            ),
            "select string_agg ( a , ', ' ) from ( WITH RECURSIVE pg_series ( v ) AS ( SELECT 0 UNION ALL \
             SELECT v + 1 FROM pg_series WHERE v < ( array_upper ( p . attrs , 1 ) ) ) SELECT v AS s \
             FROM pg_series ) s , t where n = pg_element ( attrs , s )"
        );
        assert_eq!(
            plain(
                "select CAST('tuple' AS pg_catalog.text), cast(a as int4), cast(b as regclass), cast(c as text[])"
            ),
            "select CAST ( 'tuple' AS TEXT ) , CAST ( a AS INTEGER ) , regclass ( b ) , ( c )"
        );
        assert_eq!(
            plain("select 'toast.' || x from pg_catalog.unnest(tc.reloptions) x"),
            "select 'toast.' || x from ( SELECT value AS x FROM json_each ( pg_array ( tc . reloptions ) ) ) x"
        );
    }

    #[test]
    fn the_sessions_values_are_functions() {
        assert_eq!(
            plain("SELECT current_schema(), session_user, current_user, t.current_user"),
            "select current_schema ( ) , session_user ( ) , current_user ( ) , t . current_user"
        );
        assert_eq!(
            plain("select a is distinct from b, c is not distinct from d"),
            "select a is NOT b , c is d"
        );
    }

    #[test]
    fn show_asks_for_a_setting() {
        assert_eq!(
            rewrite("SHOW search_path"),
            "SELECT current_setting('search_path') AS \"search_path\""
        );
        assert_eq!(
            rewrite("show transaction isolation level;"),
            "SELECT current_setting('transaction_isolation') AS \"transaction_isolation\""
        );
        assert!(rewrite("SHOW ALL").contains("pg_settings"));
    }

    #[test]
    fn a_field_is_named_as_postgresql_names_it() {
        let names = |sql: &str| -> Vec<String> {
            let tokens = named(tokens(sql));
            tokens
                .windows(2)
                .filter_map(|pair| match pair {
                    [as_word, Token::Quoted(name)] if as_word.is_word("as") => Some(name.clone()),
                    _ => None,
                })
                .collect()
        };
        assert_eq!(
            names(
                "select version(), pg_catalog.format_type(a.atttypid, a.atttypmod), 1 + 2 from t"
            ),
            ["version", "format_type", "?column?"]
        );
        assert_eq!(
            names("select c.oid::regclass, 'x'::text, f(a)::int4, current_schema(), f(a)::int / 2"),
            ["oid", "text", "f", "current_schema", "?column?"]
        );
        // What has a name keeps it.
        assert!(
            names("select relname, c.relname, pg_catalog.pg_class.relname, c.*, * from t")
                .is_empty()
        );
        assert!(
            names("select f(x) as \"Access privileges\", g(y) n, case when a then b end from t")
                .contains(&"?column?".to_owned())
        );
        assert_eq!(
            names("select f(x) as \"Access privileges\", g(y) n from t"),
            ["Access privileges"]
        );
        assert!(names("with a as (select f(x)) select * from a").is_empty());
    }

    #[test]
    fn functions_with_words_between_their_arguments_take_commas() {
        assert_eq!(
            plain("select substring(f(x) from 0 for 6), substring(a for 3), substring(a from 2)"),
            "select substr ( f ( x ) , 0 , 6 ) , substr ( a , 1 , 3 ) , substr ( a , 2 )"
        );
        assert_eq!(
            plain("select trim(both '\"' from pg_catalog.g(a, b)), trim(leading from c), trim(d)"),
            "select trim ( g ( a , b ) , '\"' ) , ltrim ( c ) , trim ( d )"
        );
        assert_eq!(
            plain("select string_agg(word, ',') from pg_catalog.pg_get_keywords() where a"),
            "select string_agg ( word , ',' ) from pg_get_keywords where a"
        );
        assert_eq!(
            plain(
                "select trim(s[i]), user from generate_series(1, 2) as i, string_to_array(x, ',') s"
            ),
            "select trim ( pg_element ( s , i ) ) , current_user ( ) from ( WITH RECURSIVE pg_series ( v ) AS \
             ( SELECT 1 UNION ALL SELECT v + 1 FROM pg_series WHERE v < ( 2 ) ) SELECT v AS i FROM pg_series ) i , \
             ( SELECT string_to_array ( x , ',' ) AS s ) s"
        );
        assert_eq!(
            plain("select e.n from (select generate_series(0, 3) as n) as e"),
            "select e . n from ( select v AS n FROM ( WITH RECURSIVE pg_series ( v ) AS ( SELECT 0 UNION ALL \
             SELECT v + 1 FROM pg_series WHERE v < ( 3 ) ) SELECT v FROM pg_series ) ) as e"
        );
        // A comma before a field is not one before a set.
        assert_eq!(
            plain(
                "select a, unnest(b) as c, array_agg(d order by e), bool_and(f) from t, unnest(g) h where i"
            ),
            "select a , unnest ( b ) as c , json_group_array ( d order by e ) , min ( f ) from t , \
             ( SELECT value AS h FROM json_each ( pg_array ( g ) ) ) h where i"
        );
        assert_eq!(
            plain("select 1 from generate_series(1, 32) pos(n), unnest(a) as s(r)"),
            "select 1 from ( WITH RECURSIVE pg_series ( v ) AS ( SELECT 1 UNION ALL SELECT v + 1 \
             FROM pg_series WHERE v < ( 32 ) ) SELECT v AS n FROM pg_series ) pos , \
             ( SELECT value AS r FROM json_each ( pg_array ( a ) ) ) s"
        );
    }

    #[test]
    fn the_fields_of_a_statement_are_read_from_its_text() {
        assert_eq!(
            field_names(
                "SELECT result.TABLE_CAT, result.\"Key Seq\", f(x) AS PK_NAME, g(y) FROM (SELECT 1) result"
            ),
            ["table_cat", "Key Seq", "pk_name", "g"]
        );
    }

    #[test]
    fn strings_and_comments_are_left_alone() {
        assert_eq!(
            plain("select 'a::text ~ b', E'it\\'s', \"x::y\" -- c::d\n/* e::f */ from t"),
            "select 'a::text ~ b' , 'it''s' , \"x::y\" from t"
        );
    }
}
