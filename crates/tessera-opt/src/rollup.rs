// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Rollup substitution (AD-13, QE-6): an aggregation over a tumbling
//! window whose source, filter, keys and aggregates match a rollup, and
//! whose time range the rollup covers in whole windows, is rewritten to
//! aggregate the rollup's output events instead. Counts and sums are
//! summed again, minima and maxima taken again, so the result equals
//! the un-rewritten query exactly, with narrow integers widened to 64
//! bits; a query is left alone when any part of its shape differs or
//! its range is not covered in whole windows, and a caller may turn the
//! pass off.

use std::time::Duration;

use tessera_ir::rollup::RollupEntry;
use tessera_ir::{AggregateExpr, BinaryOp, Expr, Literal, NamedExpr, Plan, Scan};
use tessera_types::event::{NAME, TIMESTAMP};
use tessera_types::{Kind, TimeRange};

/// `plan` with every matching aggregation reading its rollup.
#[must_use]
pub fn substitute(plan: Plan, rollups: &[RollupEntry]) -> Plan {
    if rollups.is_empty() {
        return plan;
    }
    match plan {
        Plan::Aggregate {
            input,
            group_by,
            aggregates,
        } => match rewrite(&input, &group_by, &aggregates, rollups) {
            Some(rewritten) => rewritten,
            None => Plan::Aggregate {
                input: Box::new(substitute(*input, rollups)),
                group_by,
                aggregates,
            },
        },
        Plan::Filter { input, predicate } => Plan::Filter {
            input: Box::new(substitute(*input, rollups)),
            predicate,
        },
        Plan::Project { input, exprs } => Plan::Project {
            input: Box::new(substitute(*input, rollups)),
            exprs,
        },
        Plan::Sort { input, keys } => Plan::Sort {
            input: Box::new(substitute(*input, rollups)),
            keys,
        },
        Plan::Limit {
            input,
            offset,
            count,
        } => Plan::Limit {
            input: Box::new(substitute(*input, rollups)),
            offset,
            count,
        },
        Plan::TopK { input, keys, k } => Plan::TopK {
            input: Box::new(substitute(*input, rollups)),
            keys,
            k,
        },
        other => other,
    }
}

/// The aggregation over `input` reading a rollup, if one matches.
fn rewrite(
    input: &Plan,
    group_by: &[NamedExpr],
    aggregates: &[AggregateExpr],
    rollups: &[RollupEntry],
) -> Option<Plan> {
    let (scan, conjuncts) = source(input)?;
    let window = group_by.iter().find_map(|key| window_length(&key.expr))?;
    rollups.iter().find_map(|rollup| {
        if rollup.dataset != scan.dataset
            || rollup.kind != scan.kind
            || !covers(rollup, scan)
            || !same_filter(rollup, &conjuncts)
            || window.as_nanos() % rollup.window.as_nanos().max(1) != 0
        {
            return None;
        }
        let mut keys = 0;
        let group_by: Vec<NamedExpr> = group_by
            .iter()
            .map(|key| {
                if window_length(&key.expr).is_some() {
                    return Some(NamedExpr {
                        expr: Expr::Call {
                            name: "tumble".into(),
                            args: vec![
                                Expr::column(TIMESTAMP),
                                Expr::Literal(Literal::Duration(window)),
                            ],
                        },
                        name: key.name.clone(),
                    });
                }
                keys += 1;
                let stored = rollup.keys.iter().find(|k| k.expr == key.expr)?;
                Some(NamedExpr {
                    expr: Expr::Path {
                        path: format!("attr.{}", stored.name),
                        data_type: stored.data_type.clone(),
                    },
                    name: key.name.clone(),
                })
            })
            .collect::<Option<_>>()?;
        if keys != rollup.keys.len() {
            return None;
        }
        let aggregates: Vec<AggregateExpr> = aggregates
            .iter()
            .map(|aggregate| {
                let stored = rollup.aggregates.iter().find(|a| {
                    a.aggregate.function == aggregate.function && a.aggregate.args == aggregate.args
                })?;
                Some(AggregateExpr {
                    function: merge_of(&aggregate.function)?.to_owned(),
                    args: vec![Expr::Path {
                        path: format!("attr.{}", stored.aggregate.name),
                        data_type: stored.data_type.clone(),
                    }],
                    name: aggregate.name.clone(),
                })
            })
            .collect::<Option<_>>()?;
        let mut target = Scan::all(scan.tenant.clone(), rollup.target.clone(), Kind::Event);
        target.time_range = scan.time_range;
        target.predicates = vec![Expr::column(NAME).binary(
            BinaryOp::Eq,
            Expr::Literal(Literal::Text(rollup.name.clone())),
        )];
        Some(Plan::Aggregate {
            input: Box::new(Plan::Scan(target)),
            group_by,
            aggregates,
        })
    })
}

/// The scan under `input` through its filters, with the conjuncts of
/// every filter and of the scan itself, less the ones over the
/// timestamp alone, which the time range carries.
fn source(input: &Plan) -> Option<(&Scan, Vec<Expr>)> {
    let mut conjuncts = Vec::new();
    let mut node = input;
    loop {
        match node {
            Plan::Filter { input, predicate } => {
                conjuncts.extend(predicate.clone().conjuncts());
                node = input;
            }
            Plan::Scan(scan) => {
                for predicate in &scan.predicates {
                    conjuncts.extend(predicate.clone().conjuncts());
                }
                conjuncts.retain(|c| !is_time_only(c));
                return Some((scan, conjuncts));
            }
            _ => return None,
        }
    }
}

/// Whether `expr` reads nothing but the timestamp.
fn is_time_only(expr: &Expr) -> bool {
    let references = expr.references();
    !references.is_empty() && references.iter().all(|r| r == TIMESTAMP)
}

/// The length of a tumbling window over the timestamp, for a key that
/// is one.
fn window_length(expr: &Expr) -> Option<Duration> {
    let Expr::Call { name, args } = expr else {
        return None;
    };
    let (time, interval) = match name.as_str() {
        "tumble" => (args.first()?, args.get(1)?),
        "time_bucket" => (args.get(1)?, args.first()?),
        _ => return None,
    };
    if !matches!(time, Expr::Column(c) if c == TIMESTAMP) {
        return None;
    }
    match interval {
        Expr::Literal(Literal::Duration(d)) if !d.is_zero() => Some(*d),
        _ => None,
    }
}

/// Whether the rollup's output covers the scan's range in whole windows.
fn covers(rollup: &RollupEntry, scan: &Scan) -> bool {
    let range = scan.time_range;
    if range == TimeRange::ALL || range.is_empty() {
        return false;
    }
    let window = i128::try_from(rollup.window.as_nanos().max(1)).unwrap_or(i128::MAX);
    let aligned = |at: tessera_types::Timestamp| i128::from(at.as_nanos()).rem_euclid(window) == 0;
    range.start >= rollup.covered.start
        && range.end <= rollup.covered.end
        && aligned(range.start)
        && aligned(range.end)
}

/// Whether the query filters exactly as the rollup does.
fn same_filter(rollup: &RollupEntry, conjuncts: &[Expr]) -> bool {
    let mut theirs: Vec<String> = rollup
        .predicate
        .clone()
        .map(Expr::conjuncts)
        .unwrap_or_default()
        .iter()
        .filter(|c| !is_time_only(c))
        .map(ToString::to_string)
        .collect();
    let mut ours: Vec<String> = conjuncts.iter().map(ToString::to_string).collect();
    theirs.sort();
    ours.sort();
    theirs == ours
}

/// The aggregate that merges stored values of `function`.
fn merge_of(function: &str) -> Option<&'static str> {
    match function {
        "count" | "count_raw" | "sum" | "sum_raw" => Some("sum"),
        "min" => Some("min"),
        "max" => Some("max"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use tessera_ir::rollup::{RollupAggregate, RollupKey, stored_type};
    use tessera_ir::{BindContext, Frontend, Registry, Statement};
    use tessera_sql::SqlFrontend;
    use tessera_types::{DatasetName, TenantId, Timestamp};

    use super::*;
    use crate::optimize;

    const MINUTE: i64 = 60_000_000_000;

    fn lowered(sql: &str) -> Plan {
        let ctx = BindContext {
            tenant: TenantId::DEFAULT,
            dataset: DatasetName::new("prod").unwrap(),
            now: Timestamp::from_nanos(0),
        };
        let Statement::Query(plan) = SqlFrontend.lower(sql, &ctx).unwrap() else {
            panic!("not a query");
        };
        optimize(plan, &Registry::builtin()).unwrap()
    }

    /// A rollup entry built from the shape of `sql`, as the evaluator does.
    fn entry(sql: &str, covered: TimeRange) -> RollupEntry {
        let plan = lowered(sql);
        let mut node = &plan;
        let (input, group_by, aggregates) = loop {
            match node {
                Plan::Project { input, .. } => node = input,
                Plan::Aggregate {
                    input,
                    group_by,
                    aggregates,
                } => break (input, group_by, aggregates),
                other => panic!("unexpected {other}"),
            }
        };
        let (scan, conjuncts) = source(input).unwrap();
        let schema = node.schema(&Registry::builtin()).unwrap();
        let keys = group_by
            .iter()
            .enumerate()
            .filter(|(_, key)| window_length(&key.expr).is_none())
            .map(|(i, key)| RollupKey {
                name: key.name.clone(),
                expr: key.expr.clone(),
                data_type: stored_type(schema.field(i).data_type()),
            })
            .collect();
        let aggregates = aggregates
            .iter()
            .enumerate()
            .map(|(i, aggregate)| RollupAggregate {
                aggregate: aggregate.clone(),
                data_type: stored_type(schema.field(group_by.len() + i).data_type()),
            })
            .collect();
        RollupEntry {
            name: "errors".into(),
            target: DatasetName::new("rollup_errors").unwrap(),
            dataset: scan.dataset.clone(),
            kind: scan.kind,
            predicate: conjuncts.into_iter().reduce(Expr::and),
            keys,
            window: group_by
                .iter()
                .find_map(|k| window_length(&k.expr))
                .unwrap(),
            aggregates,
            covered,
        }
    }

    const ROLLUP: &str = "SELECT attr.service AS service, count(*) AS n, max(severity_number) AS worst \
                          FROM logs WHERE severity_number >= 17 \
                          GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')";

    fn covered() -> TimeRange {
        TimeRange::new(
            Timestamp::from_nanos(0),
            Timestamp::from_nanos(100 * MINUTE),
        )
    }

    fn range(from_minute: i64, to_minute: i64) -> String {
        let at = |m: i64| {
            format!(
                "TIMESTAMP '{}'",
                tessera_types::Timestamp::from_nanos(m * MINUTE)
            )
        };
        format!(
            "timestamp >= {} AND timestamp < {}",
            at(from_minute),
            at(to_minute)
        )
    }

    #[test]
    fn a_covered_query_of_the_same_shape_reads_the_rollup() {
        let query = lowered(&format!(
            "SELECT attr.service AS service, count(*) AS n FROM logs \
             WHERE severity_number >= 17 AND {} \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '5 minutes') ORDER BY n DESC",
            range(10, 20)
        ));
        let rewritten = substitute(query.clone(), &[entry(ROLLUP, covered())]);
        assert_ne!(rewritten, query);
        let text = rewritten.to_string();
        assert!(text.contains("rollup_errors"), "{text}");
        assert!(text.contains("attr.n"), "{text}");
        assert!(text.contains("sum("), "{text}");
        assert!(!text.contains("prod/log"), "{text}");
        assert!(optimize(rewritten, &Registry::builtin()).is_ok());
    }

    #[test]
    fn differences_in_shape_or_coverage_leave_the_query_alone() {
        let rollup = entry(ROLLUP, covered());
        let same = |sql: &str| {
            let query = lowered(sql);
            assert_eq!(
                substitute(query.clone(), std::slice::from_ref(&rollup)),
                query,
                "{sql}"
            );
        };
        // A different filter.
        same(&format!(
            "SELECT count(*) AS n FROM logs WHERE severity_number >= 9 AND {} \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')",
            range(10, 20)
        ));
        // A key the rollup does not have.
        same(&format!(
            "SELECT count(*) AS n FROM logs WHERE severity_number >= 17 AND {} \
             GROUP BY attr.host, tumble(timestamp, INTERVAL '1 minute')",
            range(10, 20)
        ));
        // A window the rollup's does not divide.
        same(&format!(
            "SELECT count(*) AS n FROM logs WHERE severity_number >= 17 AND {} \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '90 seconds')",
            range(10, 20)
        ));
        // An aggregate that cannot be merged.
        same(&format!(
            "SELECT avg(severity_number) AS s FROM logs WHERE severity_number >= 17 AND {} \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')",
            range(10, 20)
        ));
        // A range past the progress mark, and one not aligned to windows.
        same(&format!(
            "SELECT count(*) AS n FROM logs WHERE severity_number >= 17 AND {} \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')",
            range(10, 200)
        ));
        same(
            "SELECT count(*) AS n FROM logs WHERE severity_number >= 17 \
             AND timestamp >= TIMESTAMP '1970-01-01T00:10:30Z' AND timestamp < TIMESTAMP '1970-01-01T00:20:00Z' \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')",
        );
        // No range at all.
        same(
            "SELECT count(*) AS n FROM logs WHERE severity_number >= 17 \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')",
        );
    }

    #[test]
    fn stored_types_follow_the_rollup_output() {
        let rollup = entry(ROLLUP, covered());
        assert_eq!(rollup.keys[0].data_type, DataType::Utf8);
        assert_eq!(
            rollup.aggregates[0].data_type,
            DataType::Float64,
            "weighted count"
        );
        assert_eq!(
            rollup.aggregates[1].data_type,
            DataType::Int64,
            "max of a 32-bit integer, widened to what an attribute holds"
        );
    }
}
