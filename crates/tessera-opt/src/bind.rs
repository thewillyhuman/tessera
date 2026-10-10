// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Binding: giving every attribute path the type its context asks for,
//! and checking that every expression types over its input.
//!
//! A front end does not know what an attribute holds; it lowers
//! `attr.n = 2` with a provisional text type. The binder reads the other
//! side of a comparison, a function's expectation or a cast, and types
//! the path accordingly: integer literals make it an integer, floats a
//! float, booleans a boolean, timestamps a timestamp, anything else text.

use arrow::datatypes::DataType;
use tessera_ir::{AggregateExpr, Expr, Literal, NamedExpr, Plan, PlanError, Registry, SortKey};

/// Types every path in `plan` from its context and checks the plan.
///
/// # Errors
///
/// If an expression does not type or a projection or union is malformed.
pub fn bind(plan: Plan, registry: &Registry) -> Result<Plan, PlanError> {
    let bound = bind_plan(plan);
    bound.schema(registry)?;
    Ok(bound)
}

fn bind_plan(plan: Plan) -> Plan {
    match plan {
        Plan::Scan(mut scan) => {
            scan.predicates = scan.predicates.into_iter().map(bind_expr).collect();
            Plan::Scan(scan)
        }
        Plan::Values { schema, rows } => Plan::Values { schema, rows },
        Plan::Filter { input, predicate } => Plan::Filter {
            input: Box::new(bind_plan(*input)),
            predicate: bind_expr(predicate),
        },
        Plan::Extract {
            input,
            source,
            stage,
        } => Plan::Extract {
            input: Box::new(bind_plan(*input)),
            source: bind_expr(source),
            stage,
        },
        Plan::Format {
            input,
            line,
            labels,
            drop,
            keep,
        } => Plan::Format {
            input: Box::new(bind_plan(*input)),
            line,
            labels,
            drop,
            keep,
        },
        Plan::Project { input, exprs } => Plan::Project {
            input: Box::new(bind_plan(*input)),
            exprs: exprs.into_iter().map(bind_named).collect(),
        },
        Plan::Aggregate {
            input,
            group_by,
            aggregates,
        } => Plan::Aggregate {
            input: Box::new(bind_plan(*input)),
            group_by: group_by.into_iter().map(bind_named).collect(),
            aggregates: aggregates.into_iter().map(bind_aggregate).collect(),
        },
        Plan::Sort { input, keys } => Plan::Sort {
            input: Box::new(bind_plan(*input)),
            keys: keys.into_iter().map(bind_key).collect(),
        },
        Plan::TopK { input, keys, k } => Plan::TopK {
            input: Box::new(bind_plan(*input)),
            keys: keys.into_iter().map(bind_key).collect(),
            k,
        },
        Plan::Limit {
            input,
            offset,
            count,
        } => Plan::Limit {
            input: Box::new(bind_plan(*input)),
            offset,
            count,
        },
        Plan::Union { inputs } => Plan::Union {
            inputs: inputs.into_iter().map(bind_plan).collect(),
        },
    }
}

/// Numeric aggregates read an attribute path as a number; the others
/// leave it as its context typed it.
fn bind_aggregate(aggregate: AggregateExpr) -> AggregateExpr {
    let numeric = matches!(
        aggregate.function.as_str(),
        "sum" | "sum_raw" | "avg" | "avg_raw"
    );
    AggregateExpr {
        args: aggregate
            .args
            .into_iter()
            .map(|arg| {
                let bound = bind_expr(arg);
                if numeric {
                    retype(bound, Some(DataType::Float64))
                } else {
                    bound
                }
            })
            .collect(),
        ..aggregate
    }
}

fn bind_named(named: NamedExpr) -> NamedExpr {
    NamedExpr {
        expr: bind_expr(named.expr),
        name: named.name,
    }
}

fn bind_key(key: SortKey) -> SortKey {
    SortKey {
        expr: bind_expr(key.expr),
        descending: key.descending,
    }
}

/// The type a path should take to meet `other`, if `other` fixes one.
fn type_from(other: &Expr) -> Option<DataType> {
    match other {
        Expr::Literal(literal) => literal.data_type(),
        _ => None,
    }
}

fn retype(expr: Expr, to: Option<DataType>) -> Expr {
    match (expr, to) {
        (Expr::Path { path, .. }, Some(data_type)) => Expr::Path { path, data_type },
        (Expr::Label { name, .. }, Some(data_type)) => Expr::Label { name, data_type },
        (expr, _) => expr,
    }
}

/// Binds the paths inside `expr`.
#[must_use]
pub fn bind_expr(expr: Expr) -> Expr {
    match expr {
        Expr::Binary { op, left, right } if op.is_comparison() => {
            let left = retype(bind_expr(*left), type_from(&right));
            let right = retype(bind_expr(*right), type_from(&left));
            Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            }
        }
        Expr::Binary { op, left, right } if op.is_logical() => Expr::Binary {
            op,
            left: Box::new(retype(bind_expr(*left), Some(DataType::Boolean))),
            right: Box::new(retype(bind_expr(*right), Some(DataType::Boolean))),
        },
        Expr::Binary { op, left, right } => {
            let left = retype(bind_expr(*left), Some(DataType::Float64));
            let right = retype(bind_expr(*right), Some(DataType::Float64));
            Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            }
        }
        Expr::Not(inner) => Expr::Not(Box::new(retype(bind_expr(*inner), Some(DataType::Boolean)))),
        Expr::Negate(inner) => {
            Expr::Negate(Box::new(retype(bind_expr(*inner), Some(DataType::Float64))))
        }
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(bind_expr(*expr)),
            negated,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => Expr::Like {
            expr: Box::new(retype(bind_expr(*expr), Some(DataType::Utf8))),
            pattern,
            negated,
            case_insensitive,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let item_type = list.iter().find_map(Literal::data_type);
            Expr::InList {
                expr: Box::new(retype(bind_expr(*expr), item_type)),
                list,
                negated,
            }
        }
        Expr::Call { name, args } => Expr::Call {
            name,
            args: args.into_iter().map(bind_expr).collect(),
        },
        Expr::Cast { expr, to } => {
            let inner = retype(bind_expr(*expr), Some(to.clone()));
            match inner {
                // A cast of a path or label is the value read as that type.
                Expr::Path { .. } | Expr::Label { .. } => inner,
                inner => Expr::Cast {
                    expr: Box::new(inner),
                    to,
                },
            }
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::TimeUnit;
    use tessera_ir::{BinaryOp, Scan};
    use tessera_types::{DatasetName, Kind, TenantId, Timestamp};

    use super::*;

    fn path(p: &str) -> Expr {
        Expr::Path {
            path: p.into(),
            data_type: DataType::Utf8,
        }
    }

    fn typed(expr: &Expr) -> &DataType {
        match expr {
            Expr::Path { data_type, .. } => data_type,
            other => panic!("not a path: {other}"),
        }
    }

    #[test]
    fn comparisons_type_paths_from_literals() {
        let bound = bind_expr(path("attr.n").binary(BinaryOp::Eq, Expr::literal(2)));
        let Expr::Binary { left, .. } = bound else {
            panic!()
        };
        assert_eq!(typed(&left), &DataType::Int64);
        let bound = bind_expr(Expr::literal(1.5).binary(BinaryOp::Lt, path("attr.x")));
        let Expr::Binary { right, .. } = bound else {
            panic!()
        };
        assert_eq!(typed(&right), &DataType::Float64);
        let bound =
            bind_expr(path("attr.t").binary(BinaryOp::Gt, Expr::literal(Timestamp::UNIX_EPOCH)));
        let Expr::Binary { left, .. } = bound else {
            panic!()
        };
        assert_eq!(
            typed(&left),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        let bound = bind_expr(path("attr.m").binary(BinaryOp::Eq, Expr::literal("GET")));
        let Expr::Binary { left, .. } = bound else {
            panic!()
        };
        assert_eq!(typed(&left), &DataType::Utf8);
    }

    #[test]
    fn logic_arithmetic_and_casts_fix_the_type() {
        let bound = bind_expr(Expr::Not(Box::new(path("attr.ok"))));
        let Expr::Not(inner) = bound else { panic!() };
        assert_eq!(typed(&inner), &DataType::Boolean);
        let bound = bind_expr(path("attr.a").binary(BinaryOp::Add, Expr::literal(1)));
        let Expr::Binary { left, .. } = bound else {
            panic!()
        };
        assert_eq!(typed(&left), &DataType::Float64);
        let bound = bind_expr(Expr::Cast {
            expr: Box::new(path("attr.n")),
            to: DataType::Int64,
        });
        assert_eq!(typed(&bound), &DataType::Int64);
        let bound = bind_expr(Expr::InList {
            expr: Box::new(path("attr.n")),
            list: vec![Literal::Null, Literal::Int(3)],
            negated: false,
        });
        let Expr::InList { expr, .. } = bound else {
            panic!()
        };
        assert_eq!(typed(&expr), &DataType::Int64);
    }

    #[test]
    fn numeric_aggregates_read_paths_as_numbers() {
        let plan = Plan::Aggregate {
            input: Box::new(Plan::Scan(Scan::all(
                TenantId::DEFAULT,
                DatasetName::new("prod").unwrap(),
                Kind::Log,
            ))),
            group_by: vec![],
            aggregates: vec![
                AggregateExpr {
                    function: "sum_raw".into(),
                    args: vec![path("attr.n")],
                    name: "total".into(),
                },
                AggregateExpr {
                    function: "max".into(),
                    args: vec![path("attr.host")],
                    name: "host".into(),
                },
            ],
        };
        let bound = bind(plan, &Registry::builtin()).unwrap();
        let Plan::Aggregate { aggregates, .. } = bound else {
            panic!()
        };
        assert_eq!(typed(&aggregates[0].args[0]), &DataType::Float64);
        assert_eq!(typed(&aggregates[1].args[0]), &DataType::Utf8);
    }

    #[test]
    fn binding_checks_the_whole_plan() {
        let scan = Scan::all(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Kind::Log,
        );
        let plan = Plan::Scan(scan.clone())
            .filter(path("attr.n").binary(BinaryOp::GtEq, Expr::literal(2)))
            .project(vec![NamedExpr::new(Expr::column("body"), "body")]);
        let bound = bind(plan, &Registry::builtin()).unwrap();
        let Plan::Project { input, .. } = bound else {
            panic!()
        };
        let Plan::Filter { predicate, .. } = *input else {
            panic!()
        };
        assert_eq!(predicate.to_string(), "(attr.n >= 2)");
        let bad = Plan::Scan(scan).project(vec![NamedExpr::new(Expr::column("nope"), "x")]);
        assert!(bind(bad, &Registry::builtin()).is_err());
    }
}
