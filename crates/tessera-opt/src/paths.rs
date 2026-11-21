// SPDX-License-Identifier: Apache-2.0

//! Attribute paths as scan columns (AD-4): every path an expression of
//! the plan reads is asked of every scan, which outputs it as a column
//! named by the path, from a file's dedicated column where it has one
//! and from `_overflow` elsewhere. Above the scan the kernels find the
//! column by name before they open the variant column.

use std::collections::BTreeMap;

use arrow::datatypes::DataType;
use tessera_ir::trace::TRACE_FIELDS;
use tessera_ir::{Expr, JoinSide, PathColumn, Plan, qualify, split_qualifier};

use crate::rewrite::rebuild_inputs;

/// Asks every scan of `plan` for the paths the plan reads.
#[must_use]
pub fn promote_paths(plan: Plan) -> Plan {
    let mut paths = BTreeMap::new();
    collect_plan(&plan, &mut paths);
    let columns: Vec<PathColumn> = paths
        .into_iter()
        .map(|(path, data_type)| PathColumn { path, data_type })
        .collect();
    assign(plan, &columns)
}

fn assign(plan: Plan, columns: &[PathColumn]) -> Plan {
    match plan {
        Plan::Scan(mut scan) => {
            scan.paths = columns.to_vec();
            Plan::Scan(scan)
        }
        Plan::Join {
            left,
            right,
            on,
            kind,
            asof,
        } => {
            // A side reads the paths qualified by its name, unqualified,
            // and the ones below the join.
            let side = |side: JoinSide| {
                let own: Vec<PathColumn> = columns
                    .iter()
                    .filter_map(|c| match split_qualifier(&c.path) {
                        Some((name, path)) if name == side.name => Some(PathColumn {
                            path: path.to_owned(),
                            data_type: c.data_type.clone(),
                        }),
                        Some(_) => None,
                        None => Some(c.clone()),
                    })
                    .collect();
                JoinSide::new(assign(*side.plan, &own), side.name)
            };
            Plan::Join {
                left: side(left),
                right: side(right),
                on,
                kind,
                asof,
            }
        }
        Plan::TraceGroup {
            candidates,
            spans,
            spanset,
        } => {
            // The spans carry the shape a completed trace stores (QL-14)
            // where a file has it, read like a path: null elsewhere.
            let mut stored = columns.to_vec();
            stored.extend(TRACE_FIELDS.iter().map(|(name, data_type)| PathColumn {
                path: (*name).to_owned(),
                data_type: data_type.clone(),
            }));
            Plan::TraceGroup {
                candidates: candidates.map(|c| Box::new(assign(*c, columns))),
                spans: Box::new(assign(*spans, &stored)),
                spanset,
            }
        }
        other => rebuild_inputs(other, |input| assign(input, columns)),
    }
}

fn collect_plan(plan: &Plan, into: &mut BTreeMap<String, DataType>) {
    match plan {
        Plan::Scan(scan) => {
            for predicate in &scan.predicates {
                collect_expr(predicate, into);
            }
        }
        Plan::Filter { input, predicate } => {
            collect_expr(predicate, into);
            collect_plan(input, into);
        }
        Plan::Project { input, exprs, .. } => {
            for e in exprs {
                collect_expr(&e.expr, into);
            }
            collect_plan(input, into);
        }
        Plan::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            for k in group_by {
                collect_expr(&k.expr, into);
            }
            for a in aggregates {
                for arg in &a.args {
                    collect_expr(arg, into);
                }
            }
            collect_plan(input, into);
        }
        Plan::Sort { input, keys, .. } | Plan::TopK { input, keys, .. } => {
            for k in keys {
                collect_expr(&k.expr, into);
            }
            collect_plan(input, into);
        }
        Plan::Window {
            input,
            partition_by,
            order_by,
            functions,
        } => {
            for expr in partition_by
                .iter()
                .chain(order_by.iter().map(|k| &k.expr))
                .chain(functions.iter().flat_map(|w| &w.args))
            {
                collect_expr(expr, into);
            }
            collect_plan(input, into);
        }
        Plan::Join {
            left,
            right,
            on,
            asof,
            ..
        } => {
            let left_keys = on.iter().map(|(l, _)| l);
            let right_keys = on.iter().map(|(_, r)| r);
            collect_side(left, left_keys.chain(asof.iter().map(|a| &a.left)), into);
            collect_side(right, right_keys.chain(asof.iter().map(|a| &a.right)), into);
        }
        other => {
            for input in other.inputs() {
                collect_plan(input, into);
            }
        }
    }
}

/// Collects what one side of a join reads, in its keys and below it,
/// under the side's name: the paths are that side's rows', and the
/// other side, which may be the same table, is not to read them too.
fn collect_side<'a>(
    side: &JoinSide,
    keys: impl Iterator<Item = &'a Expr>,
    into: &mut BTreeMap<String, DataType>,
) {
    let mut own = BTreeMap::new();
    for key in keys {
        collect_expr(key, &mut own);
    }
    collect_plan(&side.plan, &mut own);
    for (path, data_type) in own {
        let qualified = qualify(&side.name, &path);
        // What does not read as a path of the side goes to both, as
        // it is: the stored shape of a trace, which is no attribute.
        let name = if split_qualifier(&qualified).is_some() {
            qualified
        } else {
            path
        };
        into.entry(name).or_insert(data_type);
    }
}

fn collect_expr(expr: &Expr, into: &mut BTreeMap<String, DataType>) {
    match expr {
        Expr::Path { path, data_type } => {
            into.entry(path.clone())
                .or_insert_with(|| data_type.clone());
        }
        Expr::Binary { left, right, .. } => {
            collect_expr(left, into);
            collect_expr(right, into);
        }
        Expr::Not(inner) | Expr::Negate(inner) => collect_expr(inner, into),
        Expr::IsNull { expr, .. } | Expr::Like { expr, .. } | Expr::InList { expr, .. } => {
            collect_expr(expr, into);
        }
        Expr::Cast { expr, .. } => collect_expr(expr, into),
        Expr::Call { args, .. } => {
            for arg in args {
                collect_expr(arg, into);
            }
        }
        Expr::Column(_) | Expr::Literal(_) | Expr::Label { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use tessera_ir::{BinaryOp, JoinKind, NamedExpr, Scan};
    use tessera_types::{DatasetName, Kind, TenantId};

    use super::*;

    #[test]
    fn a_qualified_path_becomes_a_column_of_its_own_side() {
        let scan = |k| {
            Plan::Scan(Scan::all(
                TenantId::DEFAULT,
                DatasetName::new("prod").unwrap(),
                k,
            ))
        };
        let path = |p: &str| Expr::Path {
            path: p.into(),
            data_type: DataType::Utf8,
        };
        let plan = Plan::Join {
            left: JoinSide::new(scan(Kind::Log), "a"),
            right: JoinSide::new(scan(Kind::Span), "b"),
            on: vec![(path("attr.request"), path("attr.request"))],
            kind: JoinKind::Inner,
            asof: None,
        }
        .project(vec![NamedExpr::new(
            path("b.attr.db.statement"),
            "statement",
        )]);
        let Plan::Project { input, .. } = promote_paths(plan) else {
            panic!("project")
        };
        let Plan::Join { left, right, .. } = *input else {
            panic!("join")
        };
        let paths = |side: &JoinSide| match side.plan.as_ref() {
            Plan::Scan(scan) => scan
                .paths
                .iter()
                .map(|p| p.path.clone())
                .collect::<Vec<_>>(),
            other => panic!("scan: {other}"),
        };
        assert_eq!(paths(&left), ["attr.request"]);
        assert_eq!(paths(&right), ["attr.db.statement", "attr.request"]);
    }

    /// A path one side reads below the join, in a predicate pushed to
    /// its scan, is that side's alone: the other, though the same
    /// table, reads its own, as the type its own reader asks for.
    #[test]
    fn a_path_read_below_a_join_stays_on_its_side() {
        let logs = || {
            Scan::all(
                TenantId::DEFAULT,
                DatasetName::new("prod").unwrap(),
                Kind::Log,
            )
        };
        let path = |p: &str, data_type| Expr::Path {
            path: p.into(),
            data_type,
        };
        let mut filtered = logs();
        filtered
            .predicates
            .push(path("attr.n", DataType::Int64).binary(BinaryOp::Gt, Expr::literal(15_i64)));
        let plan = Plan::Join {
            left: JoinSide::new(Plan::Scan(logs()), "a"),
            right: JoinSide::new(Plan::Scan(filtered), "b"),
            on: vec![(Expr::column("body"), Expr::column("body"))],
            kind: JoinKind::Inner,
            asof: None,
        }
        .project(vec![NamedExpr::new(path("a.attr.n", DataType::Utf8), "n")]);
        let Plan::Project { input, .. } = promote_paths(plan) else {
            panic!("project")
        };
        let Plan::Join { left, right, .. } = *input else {
            panic!("join")
        };
        let paths = |side: &JoinSide| match side.plan.as_ref() {
            Plan::Scan(scan) => scan.paths.clone(),
            other => panic!("scan: {other}"),
        };
        let asked = |data_type| {
            vec![PathColumn {
                path: "attr.n".into(),
                data_type,
            }]
        };
        assert_eq!(paths(&left), asked(DataType::Utf8));
        assert_eq!(paths(&right), asked(DataType::Int64));
    }

    #[test]
    fn paths_read_above_a_scan_become_its_columns() {
        let host = Expr::Path {
            path: "attr.host".into(),
            data_type: DataType::Utf8,
        };
        let plan = Plan::Project {
            input: Box::new(Plan::Filter {
                input: Box::new(Plan::Scan(Scan::all(
                    TenantId::DEFAULT,
                    DatasetName::new("prod").unwrap(),
                    Kind::Log,
                ))),
                predicate: host.clone().binary(BinaryOp::Eq, Expr::literal("h1")),
            }),
            exprs: vec![NamedExpr::new(
                Expr::Path {
                    path: "attr.n".into(),
                    data_type: DataType::Int64,
                },
                "n",
            )],
        };
        let promoted = promote_paths(plan);
        let Plan::Project { input, .. } = &promoted else {
            panic!("project")
        };
        let Plan::Filter { input, .. } = input.as_ref() else {
            panic!("filter")
        };
        let Plan::Scan(scan) = input.as_ref() else {
            panic!("scan")
        };
        assert_eq!(
            scan.paths,
            [
                PathColumn {
                    path: "attr.host".into(),
                    data_type: DataType::Utf8
                },
                PathColumn {
                    path: "attr.n".into(),
                    data_type: DataType::Int64
                }
            ]
        );
        let schema = scan.schema().unwrap();
        assert!(schema.field_with_name("attr.host").is_ok());
        assert_eq!(
            schema.field_with_name("attr.n").unwrap().data_type(),
            &DataType::Int64
        );
    }

    /// QL-14: a trace group's span scan asks for the stored shape
    /// columns; its candidate scan does not.
    #[test]
    fn a_trace_group_reads_the_stored_shape_from_its_spans() {
        let scan = || {
            Plan::Scan(Scan::all(
                TenantId::DEFAULT,
                DatasetName::new("prod").unwrap(),
                Kind::Span,
            ))
        };
        let plan = promote_paths(Plan::TraceGroup {
            candidates: Some(Box::new(scan())),
            spans: Box::new(scan()),
            spanset: tessera_ir::Spanset::Filter(None),
        });
        let Plan::TraceGroup {
            candidates, spans, ..
        } = plan
        else {
            panic!("trace group")
        };
        let paths = |plan: &Plan| match plan {
            Plan::Scan(scan) => scan
                .paths
                .iter()
                .map(|p| p.path.clone())
                .collect::<Vec<_>>(),
            other => panic!("scan: {other}"),
        };
        assert!(paths(&candidates.unwrap()).is_empty());
        assert_eq!(paths(&spans).len(), 7);
        assert!(paths(&spans).contains(&"nest_left".to_owned()));
    }
}
