// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Rollup substitution (AD-13, QE-6): an aggregation over a tumbling
//! window whose source, filter, keys and aggregates match a rollup is
//! rewritten to aggregate the rollup's output events for the whole
//! windows of its range the rollup covers, and the raw rows as before
//! for what is left on either side, the parts in one union. Counts and
//! sums are summed again, minima and maxima taken again, and the
//! numbers the rollup stores widened cast back to the query's types,
//! so the result equals the un-rewritten query exactly; a query is
//! left alone when any part of its shape differs or the rollup covers
//! no whole window of its range, and a caller may turn the pass off.

use std::time::Duration;

use tessera_ir::rollup::RollupEntry;
use tessera_ir::{AggregateExpr, BinaryOp, Expr, Literal, NamedExpr, Plan, Registry, Scan};
use tessera_types::event::{NAME, TIMESTAMP};
use tessera_types::{Kind, TimeRange, Timestamp};

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
        Plan::Window {
            input,
            partition_by,
            order_by,
            functions,
        } => substitute(*input, rollups).window(partition_by, order_by, functions),
        Plan::Limit {
            input,
            offset,
            count,
        } => Plan::Limit {
            input: Box::new(substitute(*input, rollups)),
            offset,
            count,
        },
        Plan::TopK {
            input,
            keys,
            k,
            partition_by,
        } => substitute(*input, rollups).top_k(keys, k, partition_by),
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
            || !same_filter(rollup, &conjuncts)
            || !window
                .as_nanos()
                .is_multiple_of(rollup.window.as_nanos().max(1))
        {
            return None;
        }
        let part = covered_part(rollup, scan.time_range, window)?;
        let mut keys = 0;
        let rollup_keys: Vec<NamedExpr> = group_by
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
        let merged: Vec<AggregateExpr> = aggregates
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
        target.time_range = part;
        target.predicates = vec![Expr::column(NAME).binary(
            BinaryOp::Eq,
            Expr::Literal(Literal::Text(rollup.name.clone())),
        )];
        let from_rollup = restored(
            Plan::Aggregate {
                input: Box::new(Plan::Scan(target)),
                group_by: rollup_keys,
                aggregates: merged,
            },
            input,
            group_by,
            aggregates,
        )?;
        Some(split(
            input,
            group_by,
            aggregates,
            scan.time_range,
            part,
            from_rollup,
        ))
    })
}

/// `from_rollup`, its columns cast back to the types the aggregation
/// over the raw rows gives them, since a rollup stores narrow numbers
/// widened to 64 bits; none when either side does not type.
fn restored(
    from_rollup: Plan,
    input: &Plan,
    group_by: &[NamedExpr],
    aggregates: &[AggregateExpr],
) -> Option<Plan> {
    let registry = Registry::builtin();
    let wanted = Plan::Aggregate {
        input: Box::new(input.clone()),
        group_by: group_by.to_vec(),
        aggregates: aggregates.to_vec(),
    }
    .schema(&registry)
    .ok()?;
    let held = from_rollup.schema(&registry).ok()?;
    if wanted == held {
        return Some(from_rollup);
    }
    let exprs = wanted
        .fields()
        .iter()
        .zip(held.fields())
        .map(|(wanted, held)| {
            let column = Expr::column(wanted.name());
            let expr = if wanted.data_type() == held.data_type() {
                column
            } else {
                Expr::Cast {
                    expr: Box::new(column),
                    to: wanted.data_type().clone(),
                }
            };
            NamedExpr::new(expr, wanted.name())
        })
        .collect();
    Some(from_rollup.project(exprs))
}

/// The aggregation over `range` with `from_rollup` answering `part` of
/// it: the raw rows aggregated as before for what is left of the
/// range on either side, all in one union, or the rollup alone when
/// the part is the whole.
fn split(
    input: &Plan,
    group_by: &[NamedExpr],
    aggregates: &[AggregateExpr],
    range: TimeRange,
    part: TimeRange,
    from_rollup: Plan,
) -> Plan {
    let raw = |range: TimeRange| Plan::Aggregate {
        input: Box::new(with_range(input, range)),
        group_by: group_by.to_vec(),
        aggregates: aggregates.to_vec(),
    };
    let mut inputs = Vec::new();
    let before = TimeRange::new(range.start, part.start);
    if !before.is_empty() {
        inputs.push(raw(before));
    }
    inputs.push(from_rollup);
    let after = TimeRange::new(part.end, range.end);
    if !after.is_empty() {
        inputs.push(raw(after));
    }
    if inputs.len() == 1 {
        inputs.remove(0)
    } else {
        Plan::Union { inputs }
    }
}

/// `input`, its filters as they are, over its scan narrowed to `range`.
fn with_range(input: &Plan, range: TimeRange) -> Plan {
    match input {
        Plan::Scan(scan) => {
            let mut scan = scan.clone();
            scan.time_range = range;
            Plan::Scan(scan)
        }
        Plan::Filter { input, predicate } => Plan::Filter {
            input: Box::new(with_range(input, range)),
            predicate: predicate.clone(),
        },
        other => other.clone(),
    }
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

/// The part of `range` the rollup answers, in whole windows of its
/// own: an end of the range the rollup covers and that falls on one
/// of its windows stands; any other end is cut to the query's
/// `window`, since a cut between the rollup and the raw rows must
/// fall on a boundary of the query's own buckets, or a bucket would
/// come out once from each side. None when nothing is left.
fn covered_part(rollup: &RollupEntry, range: TimeRange, window: Duration) -> Option<TimeRange> {
    let inside = range.intersect(rollup.covered);
    if inside.is_empty() {
        return None;
    }
    let fine = i128::try_from(rollup.window.as_nanos().max(1)).unwrap_or(i128::MAX);
    let coarse = i128::try_from(window.as_nanos().max(1)).unwrap_or(i128::MAX);
    let nanos = |at: Timestamp| i128::from(at.as_nanos());
    let ceiling = |n: i128, length: i128| n + (-n).rem_euclid(length);
    let floor = |n: i128, length: i128| n - n.rem_euclid(length);
    let start =
        if inside.start == range.start && floor(nanos(range.start), fine) == nanos(range.start) {
            nanos(range.start)
        } else {
            ceiling(nanos(inside.start), coarse)
        };
    let end = if inside.end == range.end && floor(nanos(range.end), fine) == nanos(range.end) {
        nanos(range.end)
    } else {
        floor(nanos(inside.end), coarse)
    };
    if start >= end {
        return None;
    }
    let (start, end) = (i64::try_from(start).ok()?, i64::try_from(end).ok()?);
    Some(TimeRange::new(
        Timestamp::from_nanos(start),
        Timestamp::from_nanos(end),
    ))
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
        let ctx = BindContext::new(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Timestamp::from_nanos(0),
        );
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
        assert_eq!(
            rewritten.schema(&Registry::builtin()),
            query.schema(&Registry::builtin()),
            "the rollup's widened numbers come back as the query's types"
        );
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
        // A range the rollup has nothing of, and one too short for a
        // whole window.
        same(&format!(
            "SELECT count(*) AS n FROM logs WHERE severity_number >= 17 AND {} \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')",
            range(200, 300)
        ));
        same(
            "SELECT count(*) AS n FROM logs WHERE severity_number >= 17 \
             AND timestamp >= TIMESTAMP '1970-01-01T00:10:30Z' AND timestamp < TIMESTAMP '1970-01-01T00:11:30Z' \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')",
        );
    }

    /// AD-13: a range the rollup covers in part reads the rollup for the
    /// whole windows it covers and the raw rows for the rest, in a
    /// union, so that the result still equals the un-rewritten query;
    /// a range it covers entirely but past the progress mark, or not
    /// aligned to windows, splits the same way, and no range at all
    /// reads the rollup to its progress mark.
    #[test]
    fn a_partly_covered_range_splits_between_the_rollup_and_the_rows() {
        let rollup = entry(ROLLUP, covered());
        let query = |filter: &str| {
            lowered(&format!(
                "SELECT count(*) AS n FROM logs WHERE severity_number >= 17 AND {filter} \
                 GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')"
            ))
        };
        let past = substitute(query(&range(10, 200)), std::slice::from_ref(&rollup));
        let text = past.to_string();
        assert!(text.contains("Union"), "{text}");
        let narrow = lowered(&format!(
            "SELECT max(severity_number) AS worst FROM logs WHERE severity_number >= 17 AND {} \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')",
            range(10, 200)
        ));
        let split = substitute(narrow.clone(), std::slice::from_ref(&rollup));
        assert_eq!(
            split.schema(&Registry::builtin()),
            narrow.schema(&Registry::builtin()),
            "both parts of the union give the query's types: {split}"
        );
        assert!(optimize(split, &Registry::builtin()).is_ok());
        assert!(
            text.contains("rollup_errors/event [1970-01-01T00:10:00Z, 1970-01-01T01:40:00Z)"),
            "the rollup to its progress mark: {text}"
        );
        assert!(
            text.contains("prod/log [1970-01-01T01:40:00Z, 1970-01-01T03:20:00Z)"),
            "the rows from there: {text}"
        );
        assert!(optimize(past, &Registry::builtin()).is_ok());
        let unaligned = substitute(
            query(
                "timestamp >= TIMESTAMP '1970-01-01T00:10:30Z' \
                 AND timestamp < TIMESTAMP '1970-01-01T00:20:00Z'",
            ),
            std::slice::from_ref(&rollup),
        );
        let text = unaligned.to_string();
        assert!(
            text.contains("rollup_errors/event [1970-01-01T00:11:00Z, 1970-01-01T00:20:00Z)"),
            "{text}"
        );
        assert!(
            text.contains("prod/log [1970-01-01T00:10:30Z, 1970-01-01T00:11:00Z)"),
            "{text}"
        );
        // A query bucketing by two minutes over the rollup's one: a range
        // the rollup covers whole reads it alone, and a cut falls on the
        // query's own bucket boundary, not the rollup's.
        let coarse = |filter: &str, covered: TimeRange| {
            substitute(
                lowered(&format!(
                    "SELECT count(*) AS n FROM logs WHERE severity_number >= 17 AND {filter} \
                     GROUP BY attr.service, tumble(timestamp, INTERVAL '2 minutes')"
                )),
                &[entry(ROLLUP, covered)],
            )
        };
        let whole = coarse(&range(13, 15), covered());
        let text = whole.to_string();
        assert!(
            !text.contains("Union") && !text.contains("prod/log"),
            "{text}"
        );
        let odd_mark = coarse(
            &range(10, 200),
            TimeRange::new(
                Timestamp::from_nanos(0),
                Timestamp::from_nanos(101 * MINUTE),
            ),
        );
        let text = odd_mark.to_string();
        assert!(
            text.contains("rollup_errors/event [1970-01-01T00:10:00Z, 1970-01-01T01:40:00Z)")
                && text.contains("prod/log [1970-01-01T01:40:00Z, 1970-01-01T03:20:00Z)"),
            "{text}"
        );
        let open = substitute(
            lowered(
                "SELECT count(*) AS n FROM logs WHERE severity_number >= 17 \
                 GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')",
            ),
            std::slice::from_ref(&rollup),
        );
        let text = open.to_string();
        assert!(
            text.contains("rollup_errors/event [1970-01-01T00:00:00Z, 1970-01-01T01:40:00Z)"),
            "{text}"
        );
        assert!(
            text.contains("prod/log [1970-01-01T01:40:00Z, ..)"),
            "{text}"
        );
        assert!(
            text.contains("prod/log [.., 1970-01-01T00:00:00Z)"),
            "and the rows before what the rollup covers, as the query reads them: {text}"
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
