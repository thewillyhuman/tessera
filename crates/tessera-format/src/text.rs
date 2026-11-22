// SPDX-License-Identifier: Apache-2.0

//! Tokenization and trigrams, fixed per layout epoch (QL-18) so that an
//! index and a query always agree.
//!
//! Text is lowercased and split on characters that are neither letters
//! nor digits. A run that also contains dots, hyphens or underscores
//! (`http.request`, `connection-reset`, `user_id`) yields the compound as
//! one more token, so that both `request` and `http.request` find it.

use std::collections::{BTreeSet, HashSet};
use std::hash::BuildHasher;

/// Characters that join the parts of a compound identifier.
const JOINERS: [char; 3] = ['.', '-', '_'];

/// The distinct tokens of `text`, lowercased.
#[must_use]
pub fn tokenize(text: &str) -> BTreeSet<String> {
    let mut tokens = HashSet::new();
    tokens_into(text, &mut tokens);
    tokens.into_iter().collect()
}

/// Adds the tokens of `text` ([`tokenize`]) to `tokens`. A token the
/// set already holds costs a look, not a copy: over the rows of a
/// file, where the same words come back in row after row, that is
/// nearly every one of them.
pub fn tokens_into<S: BuildHasher>(text: &str, tokens: &mut HashSet<String, S>) {
    let mut add = |token: &str| {
        if !tokens.contains(token) {
            tokens.insert(token.to_owned());
        }
    };
    let lower = text.to_lowercase();
    for run in lower.split(|c: char| !(c.is_alphanumeric() || JOINERS.contains(&c))) {
        let run = run.trim_matches(|c: char| JOINERS.contains(&c));
        if run.is_empty() {
            continue;
        }
        let mut parts = 0;
        for part in run.split(|c: char| JOINERS.contains(&c)) {
            if !part.is_empty() {
                add(part);
                parts += 1;
            }
        }
        if parts > 1 {
            add(run);
        }
    }
}

/// Whether `text` has the token `token` (lowercase), by the rules of
/// [`tokenize`], without building the whole set.
#[must_use]
pub fn has_token(text: &str, token: &str) -> bool {
    let lower = text.to_lowercase();
    lower
        .split(|c: char| !(c.is_alphanumeric() || JOINERS.contains(&c)))
        .map(|run| run.trim_matches(|c: char| JOINERS.contains(&c)))
        .filter(|run| !run.is_empty())
        .any(|run| {
            run == token
                || run
                    .split(|c: char| JOINERS.contains(&c))
                    .any(|p| p == token)
        })
}

/// The distinct trigrams of `text`, lowercased: every three consecutive
/// characters. Text shorter than three characters has none.
#[must_use]
pub fn trigrams(text: &str) -> BTreeSet<String> {
    let mut found = HashSet::new();
    trigrams_into(text, &mut found);
    found.into_iter().collect()
}

/// Adds the trigrams of `text` ([`trigrams`]) to `found`; one the set
/// already holds costs a look, not a copy.
pub fn trigrams_into<S: BuildHasher>(text: &str, found: &mut HashSet<String, S>) {
    let chars: Vec<char> = text.to_lowercase().chars().collect();
    let mut trigram = String::with_capacity(12);
    for window in chars.windows(3) {
        trigram.clear();
        trigram.extend(window);
        if !found.contains(trigram.as_str()) {
            found.insert(trigram.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| (*s).to_owned()).collect()
    }

    /// What a file's indexes are built from: the tokens and trigrams of
    /// many texts gathered in one set are those of each text, together.
    #[test]
    fn the_tokens_and_trigrams_of_many_texts_gather_in_one_set() {
        let texts = [
            "Connection reset by peer: ECONNRESET (54)",
            "connection refused by auth-service.internal",
            "GET /v1/items/42 status=500",
            "",
        ];
        let (mut tokens, mut found) = (HashSet::new(), HashSet::new());
        let (mut each, mut each_found) = (BTreeSet::new(), BTreeSet::new());
        for text in texts {
            tokens_into(text, &mut tokens);
            trigrams_into(text, &mut found);
            each.extend(tokenize(text));
            each_found.extend(trigrams(text));
        }
        assert_eq!(tokens.into_iter().collect::<BTreeSet<_>>(), each);
        assert_eq!(found.into_iter().collect::<BTreeSet<_>>(), each_found);
        assert!(each.contains("auth-service.internal") && each.contains("auth"));
        assert!(each_found.contains("con") && each_found.contains("500"));
    }

    #[test]
    fn lowercases_and_splits_on_non_alphanumerics() {
        assert_eq!(
            tokenize("Connection reset by peer: ECONNRESET (54)"),
            set(&["connection", "reset", "by", "peer", "econnreset", "54"])
        );
    }

    #[test]
    fn compounds_yield_their_parts_and_themselves() {
        assert_eq!(
            tokenize("GET http.request.method user-id=42 __init__"),
            set(&[
                "get",
                "http",
                "request",
                "method",
                "http.request.method",
                "user",
                "id",
                "user-id",
                "42",
                "init",
            ])
        );
        assert!(tokenize("...").is_empty());
    }

    #[test]
    fn trigrams_slide_over_lowercased_characters() {
        assert_eq!(trigrams("Abcd"), set(&["abc", "bcd"]));
        assert!(trigrams("ab").is_empty());
        assert_eq!(trigrams("ñaña"), set(&["ñañ", "aña"]));
    }

    #[test]
    fn has_token_agrees_with_tokenize() {
        let text = "GET http.request.method user-id=42";
        for token in tokenize(text) {
            assert!(has_token(text, &token), "{token}");
        }
        assert!(!has_token(text, "http.request"));
        assert!(!has_token(text, "ge"));
        assert!(has_token("Connection Reset", "reset"));
    }
}
