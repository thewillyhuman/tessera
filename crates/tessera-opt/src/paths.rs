// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Attribute paths as scan columns (AD-4): every path an expression of
//! the plan reads is asked of every scan, which outputs it as a column
//! named by the path, from a file's dedicated column where it has one
//! and from `_overflow` elsewhere. Above the scan the kernels find the
//! column by name before they open the variant column.

use std::collections::BTreeMap;

use arrow::datatypes::DataType;
use tessera_ir::{Expr, JoinSide, PathColumn, Plan, split_qualifier};

use crate::rewrite::rebuild_inputs;

/// Asks every scan of `plan` for the paths the plan reads.
#[must_use]
pub fn promote_paths(plan: Plan) -> Plan {
    let mut paths = BTreeMap::new();
    collect_plan(&plan, &mut paths);
    if paths.is_empty() {
        return plan;
    }
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
        Plan::Join {
            left, right, on, ..
        } => {
            for (l, r) in on {
                collect_expr(l, into);
                collect_expr(r, into);
            }
            collect_plan(&left.plan, into);
            collect_plan(&right.plan, into);
        }
        other => {
            for input in other.inputs() {
                collect_plan(input, into);
            }
        }
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
        assert_eq!(paths(&right), ["attr.request", "attr.db.statement"]);
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
}
