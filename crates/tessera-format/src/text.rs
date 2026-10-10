// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Tokenization and trigrams, fixed per layout epoch (QL-18) so that an
//! index and a query always agree.
//!
//! Text is lowercased and split on characters that are neither letters
//! nor digits. A run that also contains dots, hyphens or underscores
//! (`http.request`, `connection-reset`, `user_id`) yields the compound as
//! one more token, so that both `request` and `http.request` find it.

use std::collections::BTreeSet;

/// Characters that join the parts of a compound identifier.
const JOINERS: [char; 3] = ['.', '-', '_'];

/// The distinct tokens of `text`, lowercased.
#[must_use]
pub fn tokenize(text: &str) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    let lower = text.to_lowercase();
    for run in lower.split(|c: char| !(c.is_alphanumeric() || JOINERS.contains(&c))) {
        let run = run.trim_matches(|c: char| JOINERS.contains(&c));
        if run.is_empty() {
            continue;
        }
        let mut parts = 0;
        for part in run.split(|c: char| JOINERS.contains(&c)) {
            if !part.is_empty() {
                tokens.insert(part.to_owned());
                parts += 1;
            }
        }
        if parts > 1 {
            tokens.insert(run.to_owned());
        }
    }
    tokens
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
    let chars: Vec<char> = text.to_lowercase().chars().collect();
    chars
        .windows(3)
        .map(|w| w.iter().collect::<String>())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| (*s).to_owned()).collect()
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
