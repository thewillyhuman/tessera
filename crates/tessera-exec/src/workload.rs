// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! What a query asked of each table it scanned (AD-1): the columns it
//! referenced, the predicates it applied with their observed
//! selectivity, how it grouped and ordered, and what the scan cost. The
//! node writes one workload event per scan to `_system.queries`; the
//! advisor reads them back, weighted by age (AD-2).

use std::collections::BTreeSet;
use std::fmt::Write;

use serde::{Deserialize, Serialize};
use tessera_ir::{AggregateExpr, BinaryOp, Expr, NamedExpr, Plan, Scan};
use tessera_types::{DatasetName, Kind, TenantId};

use crate::engine::Analysis;
use crate::scan::ScanAnalysis;

/// One predicate a query applied to a scanned table.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PredicateUse {
    /// The column or attribute path.
    pub column: String,
    /// The operator, as `=`, `<`, `like`, `in`, `search` or `other`.
    pub op: String,
}

/// What a query asked of one scanned table.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanUse {
    /// The tenant.
    pub tenant: TenantId,
    /// The dataset.
    pub dataset: DatasetName,
    /// The kind of event.
    pub kind: Kind,
    /// Columns and attribute paths referenced anywhere in the query.
    pub columns: BTreeSet<String>,
    /// The predicates on the scan's rows.
    pub predicates: Vec<PredicateUse>,
    /// The columns grouped by above the scan.
    pub group_by: Vec<String>,
    /// The columns ordered by above the scan.
    pub order_by: Vec<String>,
    /// Files in the partitions the time range kept.
    pub files_considered: u64,
    /// Files pruned by statistics and indexes before reading.
    pub files_pruned: u64,
    /// Bytes fetched from files, plus the bytes of the tail rows scanned.
    pub bytes_scanned: u64,
    /// Object-store requests made.
    pub requests: u64,
    /// Rows in the row groups read, before the predicates.
    pub rows_scanned: u64,
    /// Rows the scan produced.
    pub rows_returned: u64,
    /// The share of scanned rows the predicates kept, when anything was
    /// scanned.
    pub selectivity: Option<f64>,
    /// The query's aggregation shape as a rollup query (section 8.7),
    /// when the scan feeds a grouped aggregation over one tumbling
    /// window whose aggregates a rollup can merge: the filter without
    /// its time range, the keys, the aggregates and the window.
    #[serde(default)]
    pub rollup_shape: Option<String>,
}

/// The uses of every scan in `plan`, paired with `analysis` in plan
/// order.
#[must_use]
pub fn uses(plan: &Plan, analysis: &Analysis) -> Vec<ScanUse> {
    let mut found = Vec::new();
    let mut paths = BTreeSet::new();
    collect_paths(plan, &mut paths);
    let mut context = Context::default();
    walk(plan, &mut context, &mut found);
    found
        .into_iter()
        .enumerate()
        .map(|(i, (scan, context))| {
            let stats = analysis.scans.get(i);
            use_of(scan, &context, &paths, stats)
        })
        .collect()
}

/// What stands above a scan on the way down.
#[derive(Clone, Default)]
struct Context {
    predicates: Vec<Expr>,
    group_by: Vec<String>,
    order_by: Vec<String>,
    /// The aggregation nearest the scan, with no filter between the two
    /// but the scan's own.
    aggregation: Option<(Vec<NamedExpr>, Vec<AggregateExpr>)>,
}

fn walk<'a>(plan: &'a Plan, context: &mut Context, found: &mut Vec<(&'a Scan, Context)>) {
    match plan {
        Plan::Scan(scan) => found.push((scan, context.clone())),
        Plan::Filter { input, predicate } => {
            let mut inner = context.clone();
            inner.predicates.push(predicate.clone());
            walk(input, &mut inner, found);
        }
        Plan::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let mut inner = context.clone();
            if inner.group_by.is_empty() {
                inner.group_by = group_by.iter().flat_map(|k| names(&k.expr)).collect();
            }
            inner.aggregation = Some((group_by.clone(), aggregates.clone()));
            inner.predicates.clear();
            walk(input, &mut inner, found);
        }
        Plan::Sort { input, keys, .. } | Plan::TopK { input, keys, .. } => {
            let mut inner = context.clone();
            if inner.order_by.is_empty() {
                inner.order_by = keys.iter().flat_map(|k| names(&k.expr)).collect();
            }
            walk(input, &mut inner, found);
        }
        other => {
            for input in other.inputs() {
                walk(input, &mut context.clone(), found);
            }
        }
    }
}

/// The column or path names an expression reads: attribute paths as
/// themselves, not as the variant column they are extracted from.
fn names(expr: &Expr) -> Vec<String> {
    let mut paths = BTreeSet::new();
    collect_expr_paths(expr, &mut paths);
    let mut out: Vec<String> = expr
        .references()
        .into_iter()
        .filter(|c| {
            paths.is_empty()
                || (c != tessera_types::event::OVERFLOW && c != tessera_types::event::EXTRACTED)
        })
        .collect();
    out.extend(paths);
    out
}

fn collect_paths(plan: &Plan, into: &mut BTreeSet<String>) {
    match plan {
        Plan::Scan(scan) => {
            for predicate in &scan.predicates {
                collect_expr_paths(predicate, into);
            }
        }
        Plan::Filter { input, predicate } => {
            collect_expr_paths(predicate, into);
            collect_paths(input, into);
        }
        Plan::Project { input, exprs, .. } => {
            for e in exprs {
                collect_expr_paths(&e.expr, into);
            }
            collect_paths(input, into);
        }
        Plan::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            for k in group_by {
                collect_expr_paths(&k.expr, into);
            }
            for a in aggregates {
                for arg in &a.args {
                    collect_expr_paths(arg, into);
                }
            }
            collect_paths(input, into);
        }
        Plan::Sort { input, keys, .. } | Plan::TopK { input, keys, .. } => {
            for k in keys {
                collect_expr_paths(&k.expr, into);
            }
            collect_paths(input, into);
        }
        other => {
            for input in other.inputs() {
                collect_paths(input, into);
            }
        }
    }
}

fn collect_expr_paths(expr: &Expr, into: &mut BTreeSet<String>) {
    match expr {
        Expr::Path { path, .. } => {
            into.insert(path.clone());
        }
        Expr::Label { name, .. } => {
            into.insert(format!("label:{name}"));
        }
        Expr::Binary { left, right, .. } => {
            collect_expr_paths(left, into);
            collect_expr_paths(right, into);
        }
        Expr::Not(inner) | Expr::Negate(inner) => collect_expr_paths(inner, into),
        Expr::IsNull { expr, .. } | Expr::Like { expr, .. } | Expr::InList { expr, .. } => {
            collect_expr_paths(expr, into);
        }
        Expr::Cast { expr, .. } => collect_expr_paths(expr, into),
        Expr::Call { args, .. } => {
            for arg in args {
                collect_expr_paths(arg, into);
            }
        }
        Expr::Column(_) | Expr::Literal(_) => {}
    }
}

/// The conjuncts of a predicate as column and operator.
fn predicate_uses(expr: &Expr, into: &mut Vec<PredicateUse>) {
    match expr {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            predicate_uses(left, into);
            predicate_uses(right, into);
        }
        Expr::Binary { op, left, right } => {
            let column = operand(left).or_else(|| operand(right));
            let constant =
                matches!(**left, Expr::Literal(_)) || matches!(**right, Expr::Literal(_));
            if let Some(column) = column.filter(|_| constant) {
                into.push(PredicateUse {
                    column,
                    op: match op {
                        BinaryOp::Eq => "=",
                        BinaryOp::NotEq => "<>",
                        BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => "range",
                        _ => "other",
                    }
                    .to_owned(),
                });
            }
        }
        Expr::Like { expr, .. } => {
            if let Some(column) = operand(expr) {
                into.push(PredicateUse {
                    column,
                    op: "like".into(),
                });
            }
        }
        Expr::InList { expr, .. } => {
            if let Some(column) = operand(expr) {
                into.push(PredicateUse {
                    column,
                    op: "in".into(),
                });
            }
        }
        Expr::Call { name, args } if name == "search" || name == "matches" => {
            for arg in args {
                if let Some(column) = operand(arg) {
                    into.push(PredicateUse {
                        column,
                        op: "search".into(),
                    });
                }
            }
        }
        Expr::IsNull { expr, .. } => {
            if let Some(column) = operand(expr) {
                into.push(PredicateUse {
                    column,
                    op: "null".into(),
                });
            }
        }
        _ => {}
    }
}

fn operand(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(c) => Some(c.clone()),
        Expr::Path { path, .. } => Some(path.clone()),
        Expr::Label { name, .. } => Some(format!("label:{name}")),
        Expr::Cast { expr, .. } => operand(expr),
        _ => None,
    }
}

fn use_of(
    scan: &Scan,
    context: &Context,
    paths: &BTreeSet<String>,
    stats: Option<&ScanAnalysis>,
) -> ScanUse {
    let mut columns: BTreeSet<String> = scan
        .projection
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    for predicate in &scan.predicates {
        columns.extend(predicate.references());
    }
    columns.extend(paths.iter().cloned());
    let mut predicates = Vec::new();
    for predicate in scan.predicates.iter().chain(&context.predicates) {
        predicate_uses(predicate, &mut predicates);
    }
    let (files_considered, files_pruned, bytes_scanned, rows_scanned, rows_returned, requests) =
        match stats {
            Some(s) => (
                s.files.after_time_range as u64,
                (s.files.after_time_range.saturating_sub(s.files_read)) as u64,
                s.bytes_read + s.bytes_from_tail,
                s.rows_scanned,
                (s.rows_from_files + s.rows_from_tail) as u64,
                s.requests as u64,
            ),
            None => (0, 0, 0, 0, 0, 0),
        };
    #[allow(clippy::cast_precision_loss)]
    let selectivity = (rows_scanned > 0).then(|| rows_returned as f64 / rows_scanned as f64);
    ScanUse {
        tenant: scan.tenant.clone(),
        dataset: scan.dataset.clone(),
        kind: scan.kind,
        columns,
        predicates,
        group_by: context.group_by.clone(),
        order_by: context.order_by.clone(),
        files_considered,
        files_pruned,
        bytes_scanned,
        requests,
        rows_scanned,
        rows_returned,
        selectivity,
        rollup_shape: shape_sql(scan, context),
    }
}

/// The SQL table of a kind.
fn table_of(kind: Kind) -> &'static str {
    match kind {
        Kind::Log => "logs",
        Kind::Span => "spans",
        Kind::Metric => "metrics",
        Kind::Event => "events",
        Kind::Alert => "alerts",
    }
}

/// The length of a tumbling window over the timestamp, for a key that
/// is one.
fn window_of(expr: &Expr) -> Option<std::time::Duration> {
    let Expr::Call { name, args } = expr else {
        return None;
    };
    let (time, interval) = match name.as_str() {
        "tumble" => (args.first()?, args.get(1)?),
        "time_bucket" => (args.get(1)?, args.first()?),
        _ => return None,
    };
    if !matches!(time, Expr::Column(c) if c == tessera_types::event::TIMESTAMP) {
        return None;
    }
    match interval {
        Expr::Literal(tessera_ir::Literal::Duration(d)) if !d.is_zero() => Some(*d),
        _ => None,
    }
}

/// The rollup query of the aggregation over `scan`, if there is one a
/// rollup can take: one window over the timestamp, aggregates among
/// count, sum, min and max.
fn shape_sql(scan: &Scan, context: &Context) -> Option<String> {
    let (group_by, aggregates) = context.aggregation.as_ref()?;
    let window = group_by.iter().find_map(|k| window_of(&k.expr))?;
    if aggregates.is_empty()
        || !aggregates
            .iter()
            .all(|a| matches!(a.function.as_str(), "count" | "sum" | "min" | "max"))
    {
        return None;
    }
    let keys: Vec<&NamedExpr> = group_by
        .iter()
        .filter(|k| window_of(&k.expr).is_none())
        .collect();
    let mut select: Vec<String> = keys
        .iter()
        .map(|k| format!("{} AS {}", k.expr, k.name))
        .collect();
    select.extend(aggregates.iter().map(|a| {
        let args = if a.args.is_empty() {
            "*".to_owned()
        } else {
            a.args
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!("{}({args}) AS {}", a.function, a.name)
    }));
    let conjuncts: Vec<String> = context
        .predicates
        .iter()
        .chain(&scan.predicates)
        .flat_map(|p| p.clone().conjuncts())
        .filter(|c| {
            let references = c.references();
            references.is_empty()
                || !references
                    .iter()
                    .all(|r| r == tessera_types::event::TIMESTAMP)
        })
        .map(|c| c.to_string())
        .collect();
    let mut sql = format!("SELECT {} FROM {}", select.join(", "), table_of(scan.kind));
    if !conjuncts.is_empty() {
        let _ = write!(sql, " WHERE {}", conjuncts.join(" AND "));
    }
    let mut groups: Vec<String> = keys.iter().map(|k| k.expr.to_string()).collect();
    groups.push(format!(
        "tumble(timestamp, INTERVAL '{} seconds')",
        window.as_secs().max(1)
    ));
    let _ = write!(sql, " GROUP BY {}", groups.join(", "));
    Some(sql)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use tessera_ir::{BindContext, Frontend, Registry, SortKey, Statement};
    use tessera_sql::SqlFrontend;

    use super::*;

    fn lowered(sql: &str) -> Plan {
        let ctx = BindContext::new(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            tessera_types::Timestamp::from_nanos(0),
        );
        let Statement::Query(plan) = SqlFrontend.lower(sql, &ctx).unwrap() else {
            panic!("not a query");
        };
        tessera_opt::optimize(plan, &Registry::builtin()).unwrap()
    }

    #[test]
    fn a_windowed_aggregation_leaves_its_shape_without_the_time_range() {
        let plan = lowered(
            "SELECT attr.service AS service, count(*) AS n, max(severity_number) AS worst \
             FROM logs WHERE severity_number >= 17 AND timestamp > now() - INTERVAL '1 hour' \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '5 minutes') ORDER BY n DESC LIMIT 10",
        );
        let found = uses(&plan, &Analysis::default());
        let shape = found[0].rollup_shape.as_deref().unwrap();
        assert_eq!(
            shape,
            "SELECT attr.service AS service, count(*) AS n, max(severity_number) AS worst \
             FROM logs WHERE (severity_number >= 17) \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '300 seconds')"
        );
        // The shape is itself a query the engine takes.
        let _ = lowered(shape);
        let ungrouped = lowered("SELECT count(*) AS n FROM logs WHERE severity_number >= 17");
        assert_eq!(
            uses(&ungrouped, &Analysis::default())[0].rollup_shape,
            None,
            "no window"
        );
        let averaged = lowered(
            "SELECT avg(severity_number) AS s FROM logs GROUP BY tumble(timestamp, INTERVAL '1 minute')",
        );
        assert_eq!(
            uses(&averaged, &Analysis::default())[0].rollup_shape,
            None,
            "avg does not merge"
        );
    }

    #[test]
    fn a_grouped_filtered_scan_is_described() {
        let scan = Scan {
            predicates: vec![
                Expr::column("severity_number").binary(BinaryOp::GtEq, Expr::literal(17)),
            ],
            ..Scan::all(
                TenantId::DEFAULT,
                DatasetName::new("prod").unwrap(),
                Kind::Log,
            )
        };
        let host = Expr::Path {
            path: "attr.host".into(),
            data_type: DataType::Utf8,
        };
        let plan = Plan::Sort {
            input: Box::new(Plan::Aggregate {
                input: Box::new(Plan::Filter {
                    input: Box::new(Plan::Scan(scan)),
                    predicate: host.clone().binary(BinaryOp::Eq, Expr::literal("h1")),
                }),
                group_by: vec![NamedExpr::new(host, "host")],
                aggregates: vec![AggregateExpr {
                    function: "count".into(),
                    args: vec![],
                    name: "n".into(),
                }],
            }),
            keys: vec![SortKey {
                expr: Expr::column("n"),
                descending: true,
            }],
        };
        let mut analysis = Analysis::default();
        let mut scanned = ScanAnalysis::default();
        scanned.files.after_time_range = 10;
        scanned.files_read = 4;
        scanned.bytes_read = 4_096;
        scanned.rows_scanned = 1_000;
        scanned.rows_from_files = 200;
        scanned.rows_from_tail = 50;
        scanned.bytes_from_tail = 1_024;
        analysis.scans.push(scanned);
        let found = uses(&plan, &analysis);
        assert_eq!(found.len(), 1);
        let scan = &found[0];
        assert_eq!(scan.bytes_scanned, 5_120, "files and tail");
        assert!(scan.columns.contains("severity_number"));
        assert!(scan.columns.contains("attr.host"));
        assert_eq!(
            scan.predicates,
            [
                PredicateUse {
                    column: "severity_number".into(),
                    op: "range".into()
                },
                PredicateUse {
                    column: "attr.host".into(),
                    op: "=".into()
                }
            ]
        );
        assert_eq!(scan.group_by, ["attr.host"]);
        assert_eq!(scan.order_by, ["n"]);
        assert_eq!((scan.files_considered, scan.files_pruned), (10, 6));
        assert_eq!(scan.rows_returned, 250);
        assert!((scan.selectivity.unwrap() - 0.25).abs() < f64::EPSILON);
    }
}
