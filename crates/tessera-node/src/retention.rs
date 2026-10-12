// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Which retention policy applies to a partition and what it does
//! (section 13.3, CO-9): the first clause whose condition can match the
//! partition's kind wins; its actions that are due at the partition's
//! age amount to a drop of the whole partition (MF-6), when the clause
//! has no row condition and a drop is due, or to a rewrite to L3 with
//! the rows the condition matches removed, sampled or downsampled.
//! Nothing is reduced by value without an enabled policy (CO-10): a
//! clause on `value_score` matches only once the advisor has scored the
//! kind.

use std::time::Duration;

use tessera_compact::Retain;
use tessera_ir::{BindContext, Frontend, Plan, Statement};
use tessera_sql::SqlFrontend;
use tessera_types::{DatasetName, Kind, RetentionAction, RetentionPolicy, TenantId, Timestamp};

/// What to do with a partition now.
#[derive(Clone, Debug, PartialEq)]
pub enum Decision {
    /// Leave it as it is.
    Keep,
    /// Drop the whole partition (MF-6).
    Drop,
    /// Rewrite it to L3 applying `retain`; `after` is the age the earliest
    /// due action named, for the planner.
    Retire {
        /// What the rewrite does.
        retain: Retain,
        /// The age the rewrite is due from.
        after: Duration,
    },
}

/// Decides for a partition of `kind` whose window ended `age` ago, with
/// the kind's value score when the advisor has one.
#[must_use]
pub fn decide(
    policies: &[RetentionPolicy],
    tenant: &TenantId,
    dataset: &DatasetName,
    kind: Kind,
    age: Duration,
    value_score: Option<f64>,
) -> Decision {
    for policy in policies
        .iter()
        .filter(|p| p.enabled && p.dataset == dataset.as_str())
    {
        for clause in &policy.clauses {
            let Some(condition) = resolve(clause.condition.as_deref(), kind, value_score) else {
                continue;
            };
            let predicate = match &condition {
                Condition::Always => None,
                Condition::Rows(text) => {
                    let Some(predicate) = lower(text, tenant, dataset, kind) else {
                        tracing::warn!(policy = %policy.name, condition = %text, "retention condition does not lower; clause skipped");
                        continue;
                    };
                    Some(predicate)
                }
            };
            let due: Vec<&RetentionAction> = clause
                .actions
                .iter()
                .filter(|a| Duration::from_secs(a.after_seconds()) <= age)
                .collect();
            if due.is_empty() {
                return Decision::Keep;
            }
            let drop = due
                .iter()
                .any(|a| matches!(a, RetentionAction::Drop { .. }));
            if drop && predicate.is_none() {
                return Decision::Drop;
            }
            let mut retain = Retain {
                predicate,
                ..Retain::default()
            };
            let mut after = Duration::MAX;
            for action in due {
                after = after.min(Duration::from_secs(action.after_seconds()));
                match action {
                    RetentionAction::Drop { .. } => retain.remove = true,
                    RetentionAction::Sample {
                        rate,
                        except_errors,
                        ..
                    } => {
                        retain.sample = Some(*rate);
                        retain.except_errors = *except_errors;
                    }
                    RetentionAction::Downsample {
                        interval_seconds, ..
                    } => retain.downsample = Some(Duration::from_secs(*interval_seconds)),
                }
            }
            return Decision::Retire { retain, after };
        }
    }
    Decision::Keep
}

/// A clause's condition with `kind` and `value_score` settled.
enum Condition {
    Always,
    Rows(String),
}

/// Settles `kind = '...'` and `value_score` in a condition: a clause on
/// another kind does not apply; a clause on an unscored kind does not
/// apply either (CO-10); what remains is a row condition, or nothing.
fn resolve(condition: Option<&str>, kind: Kind, value_score: Option<f64>) -> Option<Condition> {
    let Some(text) = condition else {
        return Some(Condition::Always);
    };
    let mut text = text.to_owned();
    if text.contains("value_score") {
        let score = value_score?;
        text = text.replace("value_score", &format!("{score}"));
    }
    if text.contains("kind") {
        for other in Kind::ALL {
            let names = format!("kind = '{other}'");
            let truth = if other == kind { "TRUE" } else { "FALSE" };
            text = text.replace(&names, truth);
        }
        if text.contains("kind") {
            return None;
        }
    }
    if text.trim().eq_ignore_ascii_case("TRUE") || constant(&text) == Some(true) {
        return Some(Condition::Always);
    }
    if text.trim().eq_ignore_ascii_case("FALSE") || constant(&text) == Some(false) {
        return None;
    }
    Some(Condition::Rows(text))
}

/// A condition made only of settled constants, compared.
fn constant(text: &str) -> Option<bool> {
    let text = text.trim();
    for (op, test) in [
        (" < ", (|a: f64, b: f64| a < b) as fn(f64, f64) -> bool),
        (" <= ", |a, b| a <= b),
        (" > ", |a, b| a > b),
        (" >= ", |a, b| a >= b),
        (" = ", |a, b| (a - b).abs() < f64::EPSILON),
    ] {
        if let Some((left, right)) = text.split_once(op)
            && let (Ok(a), Ok(b)) = (left.trim().parse::<f64>(), right.trim().parse::<f64>())
        {
            return Some(test(a, b));
        }
    }
    None
}

/// The condition as a predicate over the kind's rows, settled constants
/// folded away by the planner; `None` when it does not lower or is
/// constant false.
fn lower(
    condition: &str,
    tenant: &TenantId,
    dataset: &DatasetName,
    kind: Kind,
) -> Option<tessera_ir::Expr> {
    let table = match kind {
        Kind::Log => "logs",
        Kind::Span => "spans",
        Kind::Metric => "metrics",
        Kind::Event => "events",
        Kind::Alert => "alerts",
    };
    let ctx = BindContext::new(tenant.clone(), dataset.clone(), Timestamp::from_nanos(0));
    let sql = format!("SELECT 1 FROM {table} WHERE {condition}");
    let Statement::Query(plan) = SqlFrontend.lower(&sql, &ctx).ok()? else {
        return None;
    };
    let mut node = &plan;
    loop {
        match node {
            Plan::Filter { predicate, .. } => return Some(predicate.clone()),
            Plan::Project { input, .. } | Plan::Limit { input, .. } => node = input,
            _ => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use tessera_types::RetentionClause;

    use super::*;

    fn policy(clauses: Vec<RetentionClause>) -> RetentionPolicy {
        RetentionPolicy {
            name: "p".into(),
            dataset: "prod".into(),
            clauses,
            enabled: true,
        }
    }

    fn clause(condition: Option<&str>, actions: Vec<RetentionAction>) -> RetentionClause {
        RetentionClause {
            condition: condition.map(ToOwned::to_owned),
            actions,
        }
    }

    const DAY: u64 = 86_400;

    #[test]
    fn the_first_matching_clause_decides_by_age() {
        let policies = [policy(vec![
            clause(
                Some("kind = 'log' AND severity_number < 13"),
                vec![
                    RetentionAction::Sample {
                        rate: 0.1,
                        after_seconds: 3 * DAY,
                        except_errors: false,
                    },
                    RetentionAction::Drop {
                        after_seconds: 30 * DAY,
                    },
                ],
            ),
            clause(
                Some("kind = 'metric'"),
                vec![RetentionAction::Downsample {
                    interval_seconds: 300,
                    after_seconds: 30 * DAY,
                }],
            ),
            clause(
                None,
                vec![RetentionAction::Drop {
                    after_seconds: 90 * DAY,
                }],
            ),
        ])];
        let dataset = DatasetName::new("prod").unwrap();
        let decide = |kind, days: u64| {
            decide(
                &policies,
                &TenantId::DEFAULT,
                &dataset,
                kind,
                Duration::from_secs(days * DAY),
                Some(0.5),
            )
        };
        assert_eq!(decide(Kind::Log, 1), Decision::Keep);
        let Decision::Retire { retain, after } = decide(Kind::Log, 5) else {
            panic!("retire");
        };
        assert_eq!(retain.sample, Some(0.1));
        assert!(retain.predicate.is_some(), "only the quiet rows");
        assert!(!retain.remove);
        assert_eq!(after, Duration::from_secs(3 * DAY));
        let Decision::Retire { retain, .. } = decide(Kind::Log, 40) else {
            panic!("retire");
        };
        assert!(retain.remove, "the conditioned drop removes matching rows");
        assert_eq!(decide(Kind::Metric, 10), Decision::Keep);
        let Decision::Retire { retain, .. } = decide(Kind::Metric, 40) else {
            panic!("retire");
        };
        assert_eq!(retain.downsample, Some(Duration::from_secs(300)));
        assert!(
            retain.predicate.is_none(),
            "the kind settled the whole condition"
        );
        assert_eq!(decide(Kind::Span, 89), Decision::Keep);
        assert_eq!(
            decide(Kind::Span, 91),
            Decision::Drop,
            "the ELSE drops whole partitions"
        );
    }

    #[test]
    fn value_scores_gate_their_clauses_and_other_datasets_are_untouched() {
        let policies = [policy(vec![clause(
            Some("value_score < 0.01"),
            vec![RetentionAction::Drop { after_seconds: DAY }],
        )])];
        let dataset = DatasetName::new("prod").unwrap();
        let age = Duration::from_secs(2 * DAY);
        assert_eq!(
            decide(
                &policies,
                &TenantId::DEFAULT,
                &dataset,
                Kind::Log,
                age,
                None
            ),
            Decision::Keep,
            "no score, no reduction (CO-10)"
        );
        assert_eq!(
            decide(
                &policies,
                &TenantId::DEFAULT,
                &dataset,
                Kind::Log,
                age,
                Some(0.001)
            ),
            Decision::Drop,
            "a low score settles the clause for every row, so the partition goes whole"
        );
        assert_eq!(
            decide(
                &policies,
                &TenantId::DEFAULT,
                &dataset,
                Kind::Log,
                age,
                Some(0.5)
            ),
            Decision::Keep
        );
        let other = DatasetName::new("other").unwrap();
        assert_eq!(
            decide(
                &policies,
                &TenantId::DEFAULT,
                &other,
                Kind::Log,
                age,
                Some(0.001)
            ),
            Decision::Keep
        );
    }
}
