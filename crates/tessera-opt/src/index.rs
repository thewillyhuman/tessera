// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Pruning with sidecar indexes (section 10.4, stage 5; QL-16, QL-17).
//!
//! A scan's conjuncts are matched against the indexes a file has:
//!
//! - `has_token(column, 'text')` against a term index;
//! - `contains(column, 'text')`, `LIKE '%text%'` and `regexp_match` with
//!   required literals against a trigram index;
//! - `column = X'..'` against a Bloom filter, which decides the whole
//!   file.
//!
//! Every conjunct narrows the row groups further; a conjunct the file
//! has no index for narrows nothing. A query gives the same answer with
//! or without an index (AD-8): the index only chooses what to decode,
//! and the predicate still runs on the rows.

use tessera_format::{RowGroups, Sidecar};
use tessera_ir::{BinaryOp, Expr, Literal};
use tessera_types::{FileMeta, IndexKind};

/// What the indexes of a file say about a scan.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Decision {
    /// No row of the file can match.
    SkipFile,
    /// Only these row groups can hold a match; `None` means every one.
    RowGroups(Option<Vec<usize>>),
}

/// Decides, from `sidecar`, which row groups of `file` the conjuncts
/// `predicates` can match.
#[must_use]
pub fn decide(file: &FileMeta, sidecar: &Sidecar, predicates: &[Expr]) -> Decision {
    let mut narrowed: Option<RowGroups> = None;
    for predicate in predicates {
        let Some(found) = row_groups_for(file, sidecar, predicate) else {
            continue;
        };
        match found {
            Decision::SkipFile => return Decision::SkipFile,
            Decision::RowGroups(Some(groups)) => {
                let set = RowGroups::from_indices(sidecar.row_groups(), &groups);
                narrowed = Some(match narrowed {
                    Some(current) => current.intersect(&set),
                    None => set,
                });
            }
            Decision::RowGroups(None) => {}
        }
    }
    match narrowed {
        None => Decision::RowGroups(None),
        Some(set) if set.is_empty() => Decision::SkipFile,
        Some(set) => Decision::RowGroups(Some(set.to_vec())),
    }
}

/// The decision one conjunct makes on its own, or `None` when it uses
/// no index of the file.
fn row_groups_for(file: &FileMeta, sidecar: &Sidecar, predicate: &Expr) -> Option<Decision> {
    let groups = |set: Option<RowGroups>| set.map(|s| Decision::RowGroups(Some(s.to_vec())));
    match predicate {
        Expr::Call { name, args } => match (name.as_str(), args.as_slice()) {
            ("has_token", [Expr::Column(column), Expr::Literal(Literal::Text(text))])
                if file.has_index(IndexKind::Term, column) =>
            {
                groups(sidecar.row_groups_with_tokens(column, text))
            }
            ("contains", [Expr::Column(column), Expr::Literal(Literal::Text(text))])
                if file.has_index(IndexKind::Trigram, column) =>
            {
                groups(sidecar.row_groups_with_substring(column, text))
            }
            ("regexp_match", [Expr::Column(column), Expr::Literal(Literal::Text(pattern))])
                if file.has_index(IndexKind::Trigram, column) =>
            {
                let literals = regex_literals(pattern)?;
                groups(sidecar.row_groups_with_any_substring(column, &literals))
            }
            _ => None,
        },
        Expr::Like {
            expr,
            pattern,
            negated: false,
            ..
        } => match expr.as_ref() {
            Expr::Column(column) if file.has_index(IndexKind::Trigram, column) => {
                let literals = like_literals(pattern);
                if literals.is_empty() {
                    return None;
                }
                groups(sidecar.row_groups_with_any_substring(column, &[literals]))
            }
            _ => None,
        },
        Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } => {
            let ((Expr::Column(column) | Expr::Path { path: column, .. }, Expr::Literal(literal))
            | (Expr::Literal(literal), Expr::Column(column) | Expr::Path { path: column, .. })) =
                (left.as_ref(), right.as_ref())
            else {
                return None;
            };
            let bytes: &[u8] = match literal {
                Literal::Bytes(bytes) => bytes,
                Literal::Text(text) => text.as_bytes(),
                _ => return None,
            };
            if !file.has_index(IndexKind::Bloom, column) {
                return None;
            }
            match sidecar.may_contain(column, bytes) {
                Some(false) => Some(Decision::SkipFile),
                _ => None,
            }
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            // A list of ids (the traces a trace group reads): the file is
            // skipped only when the filter rules out every one of them.
            let Expr::Column(column) = expr.as_ref() else {
                return None;
            };
            if list.is_empty() || !file.has_index(IndexKind::Bloom, column) {
                return None;
            }
            let all_absent = list.iter().all(|item| match item {
                Literal::Bytes(bytes) => sidecar.may_contain(column, bytes) == Some(false),
                _ => false,
            });
            all_absent.then_some(Decision::SkipFile)
        }
        _ => None,
    }
}

/// The literal runs of a `LIKE` pattern that every match must contain:
/// the text between wildcards, at least three characters long.
#[must_use]
pub fn like_literals(pattern: &str) -> Vec<String> {
    pattern
        .split(['%', '_'])
        .filter(|run| run.chars().count() >= 3)
        .map(str::to_owned)
        .collect()
}

/// The literals a regular expression requires (QL-17), as alternatives
/// of conjunctions: `foo|bar` gives `[[foo], [bar]]`, `foo.*bar` gives
/// `[[foo, bar]]`. `None` when some alternative requires no literal of
/// three characters or more, or the pattern uses alternation inside a
/// group, where the analysis gives up rather than guess.
#[must_use]
pub fn regex_literals(pattern: &str) -> Option<Vec<Vec<String>>> {
    let pattern = pattern.strip_prefix("(?i)").unwrap_or(pattern);
    let mut alternatives = Vec::new();
    for alternative in split_top_level(pattern)? {
        let literals = required_literals(&alternative)?;
        if literals.is_empty() {
            return None;
        }
        alternatives.push(literals);
    }
    Some(alternatives)
}

/// Splits on `|` outside groups; `None` if a group contains `|`.
fn split_top_level(pattern: &str) -> Option<Vec<String>> {
    let mut alternatives = vec![String::new()];
    let mut depth = 0usize;
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                let escaped = chars.next()?;
                let last = alternatives.last_mut()?;
                last.push('\\');
                last.push(escaped);
            }
            '(' => {
                depth += 1;
                alternatives.last_mut()?.push(c);
            }
            ')' => {
                depth = depth.checked_sub(1)?;
                alternatives.last_mut()?.push(c);
            }
            '|' if depth == 0 => alternatives.push(String::new()),
            '|' => return None,
            _ => alternatives.last_mut()?.push(c),
        }
    }
    Some(alternatives)
}

/// The literal runs every match of one alternative must contain.
fn required_literals(alternative: &str) -> Option<Vec<String>> {
    let mut literals = Vec::new();
    let mut run = String::new();
    let mut chars = alternative.chars().peekable();
    let end_run = |run: &mut String, literals: &mut Vec<String>| {
        if run.chars().count() >= 3 {
            literals.push(std::mem::take(run));
        } else {
            run.clear();
        }
    };
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                let escaped = chars.next()?;
                if escaped.is_ascii_alphanumeric() {
                    // A class such as \d or \w: not a literal.
                    end_run(&mut run, &mut literals);
                } else {
                    run.push(escaped);
                }
            }
            '[' => {
                // Skip the class.
                for inner in chars.by_ref() {
                    if inner == ']' {
                        break;
                    }
                }
                end_run(&mut run, &mut literals);
            }
            '.' | '^' | '$' | '(' | ')' | '+' => end_run(&mut run, &mut literals),
            '*' | '?' | '{' => {
                // The previous character is optional or repeated: drop it.
                run.pop();
                end_run(&mut run, &mut literals);
                if c == '{' {
                    for inner in chars.by_ref() {
                        if inner == '}' {
                            break;
                        }
                    }
                }
            }
            other => run.push(other),
        }
    }
    end_run(&mut run, &mut literals);
    Some(literals)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, FixedSizeBinaryArray, StringArray};
    use tessera_format::IndexSpec;
    use tessera_types::event::{BODY, TRACE_ID};
    use tessera_types::{
        DatasetName, FileId, FileSource, IndexRef, Kind, Level, PartitionId, PoolId, Seq, TenantId,
        Timestamp, Window, WriterId,
    };

    use super::*;

    fn file() -> FileMeta {
        FileMeta {
            file_id: FileId::from_u128(1),
            partition: PartitionId {
                tenant: TenantId::DEFAULT,
                dataset: DatasetName::new("prod").unwrap(),
                kind: Kind::Log,
                window: Window::containing(Timestamp::UNIX_EPOCH, Window::DEFAULT_LENGTH).unwrap(),
            },
            pool: PoolId::new("root").unwrap(),
            level: Level::L2,
            source: FileSource::Writer {
                writer: WriterId::from_u128(1),
                after: Seq::ZERO,
                through: Seq::new(1),
            },
            min_timestamp: Timestamp::UNIX_EPOCH,
            max_timestamp: Timestamp::UNIX_EPOCH,
            row_count: 1,
            size_bytes: 1,
            layout_epoch: 0,
            sort_key: vec![],
            columns: vec![],
            indexes: vec![
                IndexRef {
                    kind: IndexKind::Term,
                    column: BODY.into(),
                },
                IndexRef {
                    kind: IndexKind::Trigram,
                    column: BODY.into(),
                },
                IndexRef {
                    kind: IndexKind::Bloom,
                    column: TRACE_ID.into(),
                },
            ],
        }
    }

    fn sidecar() -> Sidecar {
        let bodies: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["connection reset by peer"])),
            Arc::new(StringArray::from(vec!["GET /checkout ok"])),
            Arc::new(StringArray::from(vec!["timeout talking to payments"])),
        ];
        let ids: Vec<ArrayRef> = (0..3u8)
            .map(|rg| {
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(vec![vec![rg; 16]].into_iter()).unwrap(),
                ) as ArrayRef
            })
            .collect();
        Sidecar::build(
            &IndexSpec {
                term: vec![BODY.into()],
                trigram: vec![BODY.into()],
                bloom: vec![TRACE_ID.into()],
            },
            3,
            |name, rg| match name {
                BODY => Some(Arc::clone(&bodies[rg])),
                TRACE_ID => Some(Arc::clone(&ids[rg])),
                _ => None,
            },
        )
        .unwrap()
    }

    fn call(name: &str, text: &str) -> Expr {
        Expr::Call {
            name: name.into(),
            args: vec![Expr::column(BODY), Expr::literal(text)],
        }
    }

    #[test]
    fn conjuncts_narrow_row_groups_together() {
        let (file, sidecar) = (file(), sidecar());
        assert_eq!(
            decide(&file, &sidecar, &[call("has_token", "checkout")]),
            Decision::RowGroups(Some(vec![1]))
        );
        assert_eq!(
            decide(
                &file,
                &sidecar,
                &[call("contains", "eset"), call("has_token", "peer")]
            ),
            Decision::RowGroups(Some(vec![0]))
        );
        assert_eq!(
            decide(
                &file,
                &sidecar,
                &[call("contains", "eset"), call("has_token", "checkout")]
            ),
            Decision::SkipFile
        );
        assert_eq!(
            decide(
                &file,
                &sidecar,
                &[Expr::column("severity_number").binary(BinaryOp::Eq, Expr::literal(1))]
            ),
            Decision::RowGroups(None)
        );
    }

    #[test]
    fn like_and_regex_use_their_literals() {
        let (file, sidecar) = (file(), sidecar());
        let like = Expr::Like {
            expr: Box::new(Expr::column(BODY)),
            pattern: "%timeout%payments%".into(),
            negated: false,
            case_insensitive: false,
        };
        assert_eq!(
            decide(&file, &sidecar, &[like]),
            Decision::RowGroups(Some(vec![2]))
        );
        assert_eq!(
            decide(&file, &sidecar, &[call("regexp_match", "peer|checkout")]),
            Decision::RowGroups(Some(vec![0, 1]))
        );
        assert_eq!(
            decide(&file, &sidecar, &[call("regexp_match", "^t.*ments$")]),
            Decision::RowGroups(Some(vec![2]))
        );
        assert_eq!(
            decide(&file, &sidecar, &[call("regexp_match", "a|b")]),
            Decision::RowGroups(None),
            "no required literal: no pruning"
        );
    }

    #[test]
    fn bloom_filters_skip_whole_files() {
        let (file, sidecar) = (file(), sidecar());
        let present = Expr::column(TRACE_ID)
            .binary(BinaryOp::Eq, Expr::Literal(Literal::Bytes(vec![2u8; 16])));
        assert_eq!(
            decide(&file, &sidecar, &[present]),
            Decision::RowGroups(None)
        );
        let absent = Expr::column(TRACE_ID)
            .binary(BinaryOp::Eq, Expr::Literal(Literal::Bytes(vec![9u8; 16])));
        assert_eq!(decide(&file, &sidecar, &[absent]), Decision::SkipFile);
    }

    #[test]
    fn bloom_filters_skip_files_for_id_lists() {
        let (file, sidecar) = (file(), sidecar());
        let list = |ids: Vec<u8>| Expr::InList {
            expr: Box::new(Expr::column(TRACE_ID)),
            list: ids
                .into_iter()
                .map(|b| Literal::Bytes(vec![b; 16]))
                .collect(),
            negated: false,
        };
        assert_eq!(
            decide(&file, &sidecar, &[list(vec![9, 2])]),
            Decision::RowGroups(None),
            "one id may be present"
        );
        assert_eq!(
            decide(&file, &sidecar, &[list(vec![9, 10])]),
            Decision::SkipFile
        );
        assert_eq!(
            decide(&file, &sidecar, &[list(vec![])]),
            Decision::RowGroups(None)
        );
    }

    #[test]
    fn files_without_the_index_are_not_pruned() {
        let mut bare = file();
        bare.indexes.clear();
        assert_eq!(
            decide(&bare, &sidecar(), &[call("has_token", "nothing")]),
            Decision::RowGroups(None)
        );
    }

    #[test]
    fn regex_literal_analysis_is_conservative() {
        assert_eq!(
            regex_literals("foo.*bar"),
            Some(vec![vec!["foo".into(), "bar".into()]])
        );
        assert_eq!(
            regex_literals("(?i)error|timeout"),
            Some(vec![vec!["error".into()], vec!["timeout".into()]])
        );
        assert_eq!(regex_literals("colou?r"), Some(vec![vec!["colo".into()]]));
        assert_eq!(regex_literals("ab(c|d)"), None);
        assert_eq!(regex_literals("x+"), None);
        assert_eq!(
            regex_literals(r"user\.id=\d+"),
            Some(vec![vec!["user.id=".into()]])
        );
        assert_eq!(
            regex_literals("err[oa]r"),
            Some(vec![vec!["err".into()]]),
            "the run before a class is still required"
        );
        assert_eq!(
            regex_literals("status{2,3}"),
            Some(vec![vec!["statu".into()]]),
            "the repeated character is dropped, the rest stays"
        );
        assert_eq!(like_literals("%con%ne_ct%"), vec!["con"]);
    }
}
