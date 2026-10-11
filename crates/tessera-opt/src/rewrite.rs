// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Rule-based rewrites that are always beneficial (QE-5, QE-6).
//!
//! 1. Predicates move down to the scan: through filters, projections
//!    that pass the referenced columns through, sorts and unions.
//! 2. Comparisons of `timestamp` with constants tighten the scan's time
//!    range (time-range inference).
//! 3. The scan reads only the columns something above it needs
//!    (projection pushdown).
//! 4. A limit over a sort becomes a top-K, and a top-K or limit by time
//!    descending over a scan lets the scan read newest first and stop
//!    (limit and order pushdown with early termination).

use std::collections::BTreeSet;

use tessera_ir::trace::SPAN_COLUMNS;
use tessera_ir::{BinaryOp, Expr, Literal, Plan, Scan, SortKey, Spanset, VALUE as VALUE_COLUMN};
use tessera_types::event::{BODY, EXTRACTED, OVERFLOW, SAMPLE_RATE, TIMESTAMP, TRACE_ID};
use tessera_types::{TimeRange, Timestamp};

/// Applies every rewrite to `plan`.
#[must_use]
pub fn rewrite(plan: Plan) -> Plan {
    let pushed = push_predicates(plan, Vec::new());
    let limited = push_limits(pushed);
    push_projection(limited, None)
}

/// Moves `pending` conjuncts down as far as they go, collecting more on
/// the way.
fn push_predicates(plan: Plan, mut pending: Vec<Expr>) -> Plan {
    match plan {
        Plan::Filter { input, predicate } => {
            pending.extend(predicate.conjuncts());
            push_predicates(*input, pending)
        }
        Plan::Scan(mut scan) => {
            for predicate in pending {
                tighten_time_range(&mut scan, &predicate);
                scan.predicates.push(predicate);
            }
            Plan::Scan(scan)
        }
        Plan::Project { input, exprs } => {
            // A predicate may pass through if every column it reads is
            // output unchanged under its own name.
            let passthrough: BTreeSet<&str> = exprs
                .iter()
                .filter(|e| matches!(&e.expr, Expr::Column(c) if *c == e.name))
                .map(|e| e.name.as_str())
                .collect();
            let (below, above): (Vec<Expr>, Vec<Expr>) = pending.into_iter().partition(|p| {
                p.references()
                    .iter()
                    .all(|c| passthrough.contains(c.as_str()))
            });
            let input = push_predicates(*input, below);
            wrap_filter(input.project(exprs), above)
        }
        Plan::Sort { input, keys } => push_predicates(*input, pending).sort(keys),
        Plan::Extract {
            input,
            source,
            stage,
        } => {
            // A predicate that reads nothing a parser stage derives may run
            // before it; labels read `_extracted`, so they stay above.
            let (below, above): (Vec<Expr>, Vec<Expr>) = pending
                .into_iter()
                .partition(|p| !p.references().contains(EXTRACTED));
            let input = push_predicates(*input, below);
            wrap_filter(
                Plan::Extract {
                    input: Box::new(input),
                    source,
                    stage,
                },
                above,
            )
        }
        Plan::Union { inputs } => Plan::Union {
            inputs: inputs
                .into_iter()
                .map(|i| push_predicates(i, pending.clone()))
                .collect(),
        },
        other => {
            // Aggregates, limits, top-K and values stop predicates: the
            // rows above them are not the rows below.
            let rebuilt = rebuild_inputs(other, |input| push_predicates(input, Vec::new()));
            wrap_filter(rebuilt, pending)
        }
    }
}

fn wrap_filter(plan: Plan, predicates: Vec<Expr>) -> Plan {
    match Expr::all(predicates) {
        Some(predicate) => plan.filter(predicate),
        None => plan,
    }
}

/// Applies `f` to each input of `plan`, keeping the node itself.
pub(crate) fn rebuild_inputs(plan: Plan, mut f: impl FnMut(Plan) -> Plan) -> Plan {
    match plan {
        Plan::Scan(_) | Plan::Values { .. } | Plan::SystemTable { .. } => plan,
        Plan::RangeVector {
            input,
            function,
            value,
            range,
            step,
            start,
            end,
            grouping,
        } => Plan::RangeVector {
            input: Box::new(f(*input)),
            function,
            value,
            range,
            step,
            start,
            end,
            grouping,
        },
        Plan::Filter { input, predicate } => f(*input).filter(predicate),
        Plan::Project { input, exprs } => f(*input).project(exprs),
        Plan::Aggregate {
            input,
            group_by,
            aggregates,
        } => Plan::Aggregate {
            input: Box::new(f(*input)),
            group_by,
            aggregates,
        },
        Plan::Sort { input, keys } => f(*input).sort(keys),
        Plan::Limit {
            input,
            offset,
            count,
        } => f(*input).limit(offset, count),
        Plan::TopK { input, keys, k } => Plan::TopK {
            input: Box::new(f(*input)),
            keys,
            k,
        },
        Plan::Extract {
            input,
            source,
            stage,
        } => Plan::Extract {
            input: Box::new(f(*input)),
            source,
            stage,
        },
        Plan::Format {
            input,
            line,
            labels,
            drop,
            keep,
        } => Plan::Format {
            input: Box::new(f(*input)),
            line,
            labels,
            drop,
            keep,
        },
        Plan::Union { inputs } => Plan::Union {
            inputs: inputs.into_iter().map(f).collect(),
        },
        Plan::TraceGroup {
            candidates,
            spans,
            spanset,
        } => Plan::TraceGroup {
            candidates: candidates.map(|c| Box::new(f(*c))),
            spans: Box::new(f(*spans)),
            spanset,
        },
        Plan::PromQL {
            inputs,
            expr,
            start,
            end,
            step,
        } => Plan::PromQL {
            inputs: inputs.into_iter().map(f).collect(),
            expr,
            start,
            end,
            step,
        },
    }
}

/// Narrows `scan.time_range` when `predicate` compares `timestamp` with
/// a constant.
fn tighten_time_range(scan: &mut Scan, predicate: &Expr) {
    let Expr::Binary { op, left, right } = predicate else {
        return;
    };
    let (op, value) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(c), Expr::Literal(Literal::Timestamp(t))) if c == TIMESTAMP => (*op, *t),
        (Expr::Literal(Literal::Timestamp(t)), Expr::Column(c)) if c == TIMESTAMP => {
            (op.flipped(), *t)
        }
        _ => return,
    };
    let current = scan.time_range;
    let inferred = match op {
        BinaryOp::Eq => TimeRange::new(value, next(value)),
        BinaryOp::Lt => TimeRange::new(Timestamp::MIN, value),
        BinaryOp::LtEq => TimeRange::new(Timestamp::MIN, next(value)),
        BinaryOp::Gt => TimeRange::new(next(value), Timestamp::MAX),
        BinaryOp::GtEq => TimeRange::new(value, Timestamp::MAX),
        _ => return,
    };
    scan.time_range = current.intersect(inferred);
}

fn next(t: Timestamp) -> Timestamp {
    Timestamp::from_nanos(t.as_nanos().saturating_add(1))
}

/// Turns limits over sorts into top-K and lets a scan under a newest-first
/// top-K stop early. A limit moves below a projection first: both are
/// row-wise, and the sort it is looking for sits under the projection
/// when the query orders by an input column.
fn push_limits(plan: Plan) -> Plan {
    match plan {
        Plan::Limit {
            input,
            offset: 0,
            count: Some(count),
        } => match push_limits(*input) {
            Plan::Sort { input, keys } => newest_first(Plan::TopK {
                input,
                keys,
                k: count,
            }),
            Plan::Project { input, exprs } => {
                push_limits(input.limit(0, Some(count))).project(exprs)
            }
            other => newest_first(other.limit(0, Some(count))),
        },
        other => rebuild_inputs(other, push_limits),
    }
}

/// If `plan` is a top-K or limit by `timestamp DESC` directly over a
/// scan, the scan reads newest first and stops after `k` rows.
fn newest_first(plan: Plan) -> Plan {
    let by_time_desc = |keys: &[SortKey]| matches!(keys, [key] if key.descending && key.expr == Expr::column(TIMESTAMP));
    match plan {
        Plan::TopK { input, keys, k } if by_time_desc(&keys) => match *input {
            Plan::Scan(mut scan) => {
                scan.newest_first_limit = Some(k);
                Plan::TopK {
                    input: Box::new(Plan::Scan(scan)),
                    keys,
                    k,
                }
            }
            input => Plan::TopK {
                input: Box::new(input),
                keys,
                k,
            },
        },
        Plan::Limit {
            input,
            offset: 0,
            count: Some(count),
        } => match *input {
            // Rows without an order: newest first is as good as any and
            // lets the scan stop.
            Plan::Scan(mut scan) => {
                scan.newest_first_limit = Some(count);
                Plan::Scan(scan).limit(0, Some(count))
            }
            input => input.limit(0, Some(count)),
        },
        other => other,
    }
}

/// Narrows each scan to the columns needed above it. `needed` is `None`
/// when everything above wants every column.
/// `scan` narrowed to `needed` and the columns its own predicates read,
/// in schema order; untouched when everything above wants every column.
fn narrow_scan(mut scan: Scan, needed: Option<BTreeSet<String>>) -> Scan {
    let Some(needed) = needed else {
        return scan;
    };
    let mut needed = scan.predicates.iter().fold(needed, |mut set, expr| {
        set.extend(expr.references());
        set
    });
    if let Some(projection) = &scan.projection {
        needed.retain(|c| projection.contains(c));
    }
    let columns: Vec<String> = tessera_types::event::schema(scan.kind)
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .filter(|c| needed.contains(c))
        .collect();
    scan.projection = Some(columns);
    scan
}

fn push_projection(plan: Plan, needed: Option<BTreeSet<String>>) -> Plan {
    let add = |mut set: BTreeSet<String>, expr: &Expr| {
        set.extend(expr.references());
        set
    };
    match plan {
        Plan::Scan(scan) => Plan::Scan(narrow_scan(scan, needed)),
        Plan::Values { schema, rows } => Plan::Values { schema, rows },
        Plan::SystemTable { tenant, table } => Plan::SystemTable { tenant, table },
        Plan::Filter { input, predicate } => {
            let needed = needed.map(|n| add(n, &predicate));
            push_projection(*input, needed).filter(predicate)
        }
        Plan::Project { input, exprs } => {
            let needed = exprs
                .iter()
                .fold(BTreeSet::new(), |set, e| add(set, &e.expr));
            push_projection(*input, Some(needed)).project(exprs)
        }
        Plan::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let mut needed = group_by
                .iter()
                .fold(BTreeSet::new(), |set, e| add(set, &e.expr));
            for aggregate in &aggregates {
                needed = aggregate.args.iter().fold(needed, add);
            }
            // Weighted aggregates read the sample rate; harmless when the
            // input has none.
            needed.insert(SAMPLE_RATE.to_owned());
            Plan::Aggregate {
                input: Box::new(push_projection(*input, Some(needed))),
                group_by,
                aggregates,
            }
        }
        Plan::Sort { input, keys } => {
            let needed = needed.map(|n| keys.iter().fold(n, |set, k| add(set, &k.expr)));
            push_projection(*input, needed).sort(keys)
        }
        Plan::TopK { input, keys, k } => {
            let needed = needed.map(|n| keys.iter().fold(n, |set, k| add(set, &k.expr)));
            Plan::TopK {
                input: Box::new(push_projection(*input, needed)),
                keys,
                k,
            }
        }
        Plan::Limit {
            input,
            offset,
            count,
        } => push_projection(*input, needed).limit(offset, count),
        Plan::Union { inputs } => Plan::Union {
            inputs: inputs
                .into_iter()
                .map(|i| push_projection(i, needed.clone()))
                .collect(),
        },
        Plan::TraceGroup {
            candidates,
            spans,
            spanset,
        } => push_projection_trace(candidates, *spans, spanset),
        other => push_projection_text(other, needed),
    }
}

/// The trace group of [`push_projection`]: candidates contribute only
/// their trace id; the spans carry the span columns through and feed
/// the expressions.
fn push_projection_trace(candidates: Option<Box<Plan>>, spans: Plan, spanset: Spanset) -> Plan {
    let ids = BTreeSet::from([TRACE_ID.to_owned()]);
    let mut needed: BTreeSet<String> = SPAN_COLUMNS.map(str::to_owned).into();
    needed.insert(OVERFLOW.to_owned());
    needed.extend(spanset.references());
    Plan::TraceGroup {
        candidates: candidates.map(|c| Box::new(push_projection(*c, Some(ids)))),
        spans: Box::new(push_projection(spans, Some(needed))),
        spanset,
    }
}

/// The text and window nodes of [`push_projection`]: parser stages read
/// their source, templates and series keys read every label source.
fn push_projection_text(plan: Plan, needed: Option<BTreeSet<String>>) -> Plan {
    let add = |mut set: BTreeSet<String>, expr: &Expr| {
        set.extend(expr.references());
        set
    };
    match plan {
        Plan::Extract {
            input,
            source,
            stage,
        } => {
            let needed = needed.map(|n| add(n, &source));
            Plan::Extract {
                input: Box::new(push_projection(*input, needed)),
                source,
                stage,
            }
        }
        Plan::Format {
            input,
            line,
            labels,
            drop,
            keep,
        } => {
            // Templates may read any label: the line, the attributes, the
            // derived labels and the severity text all stay.
            let needed = needed.map(|mut n| {
                n.extend(
                    [
                        BODY,
                        OVERFLOW,
                        EXTRACTED,
                        tessera_types::event::SEVERITY_TEXT,
                    ]
                    .map(str::to_owned),
                );
                n
            });
            Plan::Format {
                input: Box::new(push_projection(*input, needed)),
                line,
                labels,
                drop,
                keep,
            }
        }
        other => push_projection_series(other),
    }
}

/// The series nodes of [`push_projection`]: a range vector reads every
/// label source and its unwrapped value; a `PromQL` node reads each
/// input's time, value and labels.
fn push_projection_series(plan: Plan) -> Plan {
    let add = |mut set: BTreeSet<String>, expr: &Expr| {
        set.extend(expr.references());
        set
    };
    match plan {
        Plan::RangeVector {
            input,
            function,
            value,
            range,
            step,
            start,
            end,
            grouping,
        } => {
            // Series keys come from every label source; lines are measured
            // by the byte functions; the unwrapped value reads what it reads.
            let mut needed: BTreeSet<String> = [
                TIMESTAMP,
                BODY,
                OVERFLOW,
                EXTRACTED,
                tessera_types::event::SEVERITY_TEXT,
            ]
            .map(str::to_owned)
            .into();
            if let Some(value) = &value {
                needed = add(needed, value);
            }
            Plan::RangeVector {
                input: Box::new(push_projection(*input, Some(needed))),
                function,
                value,
                range,
                step,
                start,
                end,
                grouping,
            }
        }
        Plan::PromQL {
            inputs,
            expr,
            start,
            end,
            step,
        } => {
            // The evaluator reads each input's time, value and labels.
            let needed: BTreeSet<String> = [TIMESTAMP, OVERFLOW, EXTRACTED, VALUE_COLUMN]
                .map(str::to_owned)
                .into();
            Plan::PromQL {
                inputs: inputs
                    .into_iter()
                    .map(|i| push_projection(i, Some(needed.clone())))
                    .collect(),
                expr,
                start,
                end,
                step,
            }
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use tessera_ir::{AggregateExpr, NamedExpr, Registry};
    use tessera_types::event::{BODY, SEVERITY_NUMBER};
    use tessera_types::{DatasetName, Kind, TenantId};

    use super::*;

    fn scan() -> Plan {
        Plan::Scan(Scan::all(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Kind::Log,
        ))
    }

    fn only_scan(plan: &Plan) -> &Scan {
        match plan {
            Plan::Scan(scan) => scan,
            other => only_scan(other.inputs()[0]),
        }
    }

    #[test]
    fn predicates_reach_the_scan_and_infer_the_time_range() {
        let from = Timestamp::from_nanos(100);
        let to = Timestamp::from_nanos(200);
        let plan = scan()
            .filter(
                Expr::column(TIMESTAMP)
                    .binary(BinaryOp::GtEq, Expr::literal(from))
                    .and(Expr::literal(to).binary(BinaryOp::Gt, Expr::column(TIMESTAMP))),
            )
            .project(vec![NamedExpr::new(Expr::column(BODY), BODY)])
            .filter(Expr::IsNull {
                expr: Box::new(Expr::column(BODY)),
                negated: true,
            });
        let rewritten = rewrite(plan);
        assert!(!matches!(rewritten, Plan::Filter { .. }));
        let scan = only_scan(&rewritten);
        assert_eq!(scan.predicates.len(), 3);
        assert_eq!(scan.time_range, TimeRange::new(from, to));
        // The scan reads what its own predicates need as well.
        assert_eq!(
            scan.projection.as_deref(),
            Some(&[TIMESTAMP.to_owned(), BODY.to_owned()][..])
        );
    }

    #[test]
    fn a_renaming_projection_keeps_the_predicate_above_it() {
        let plan = scan()
            .project(vec![NamedExpr::new(Expr::column(BODY), "text")])
            .filter(Expr::column("text").binary(BinaryOp::Eq, Expr::literal("x")));
        let rewritten = rewrite(plan);
        assert!(matches!(rewritten, Plan::Filter { .. }));
        assert!(only_scan(&rewritten).predicates.is_empty());
    }

    #[test]
    fn limits_over_sorts_become_top_k_and_reach_the_scan() {
        let plan = scan()
            .sort(vec![SortKey::descending(Expr::column(TIMESTAMP))])
            .limit(0, Some(5));
        let rewritten = rewrite(plan);
        assert!(matches!(&rewritten, Plan::TopK { k: 5, .. }), "{rewritten}");
        assert_eq!(only_scan(&rewritten).newest_first_limit, Some(5));

        let other_order = scan()
            .sort(vec![SortKey::ascending(Expr::column(SEVERITY_NUMBER))])
            .limit(0, Some(5));
        let rewritten = rewrite(other_order);
        assert!(matches!(&rewritten, Plan::TopK { k: 5, .. }));
        assert_eq!(only_scan(&rewritten).newest_first_limit, None);

        let with_offset = scan()
            .sort(vec![SortKey::descending(Expr::column(TIMESTAMP))])
            .limit(2, Some(5));
        assert!(matches!(rewrite(with_offset), Plan::Limit { .. }));

        let bare = scan().limit(0, Some(3));
        let rewritten = rewrite(bare);
        assert_eq!(only_scan(&rewritten).newest_first_limit, Some(3));

        let projected = scan()
            .sort(vec![SortKey::descending(Expr::column(TIMESTAMP))])
            .project(vec![NamedExpr::new(Expr::column(BODY), BODY)])
            .limit(0, Some(4));
        let rewritten = rewrite(projected);
        assert!(
            matches!(&rewritten, Plan::Project { input, .. } if matches!(**input, Plan::TopK { k: 4, .. })),
            "{rewritten}"
        );
        assert_eq!(only_scan(&rewritten).newest_first_limit, Some(4));
    }

    #[test]
    fn aggregates_block_predicates_and_need_the_sample_rate() {
        let plan = Plan::Aggregate {
            input: Box::new(scan()),
            group_by: vec![NamedExpr::new(Expr::column("severity_text"), "severity")],
            aggregates: vec![AggregateExpr {
                function: "count".into(),
                args: vec![],
                name: "n".into(),
            }],
        }
        .filter(Expr::column("n").binary(BinaryOp::Gt, Expr::literal(1.0)));
        let rewritten = rewrite(plan);
        assert!(matches!(rewritten, Plan::Filter { .. }));
        let scan = only_scan(&rewritten);
        assert!(scan.predicates.is_empty());
        assert_eq!(
            scan.projection.as_deref(),
            Some(&[SAMPLE_RATE.to_owned(), "severity_text".to_owned()][..])
        );
        rewritten.schema(&Registry::builtin()).unwrap();
    }

    #[test]
    fn a_full_projection_leaves_the_scan_wide() {
        let plan = scan().filter(Expr::column(BODY).binary(BinaryOp::Eq, Expr::literal("x")));
        let rewritten = rewrite(plan);
        assert_eq!(only_scan(&rewritten).projection, None);
    }

    #[test]
    fn trace_groups_narrow_their_scans() {
        use tessera_types::event::{DURATION_NS, NAME};

        let spans = Scan::all(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Kind::Span,
        );
        let condition = Expr::column(NAME).binary(BinaryOp::Eq, Expr::literal("a"));
        let mut candidates = spans.clone();
        candidates.predicates = vec![condition.clone()];
        let plan = rewrite(Plan::TraceGroup {
            candidates: Some(Box::new(Plan::Scan(candidates))),
            spans: Box::new(Plan::Scan(spans)),
            spanset: Spanset::AggregateFilter {
                input: Box::new(Spanset::Filter(Some(condition))),
                function: tessera_ir::SpanAggregate::Max,
                arg: Some(Expr::column(DURATION_NS)),
                op: BinaryOp::Gt,
                value: Literal::Int(1),
            },
        });
        let Plan::TraceGroup {
            candidates: Some(candidates),
            spans,
            ..
        } = plan
        else {
            panic!("not a trace group");
        };
        assert_eq!(
            only_scan(&candidates).projection,
            Some(vec![TRACE_ID.to_owned(), NAME.to_owned()]),
            "the id, plus what the predicate reads"
        );
        let projection = only_scan(&spans).projection.clone().unwrap();
        for column in SPAN_COLUMNS {
            assert!(projection.contains(&column.to_owned()), "{column}");
        }
        assert!(projection.contains(&OVERFLOW.to_owned()));
        assert!(!projection.contains(&SAMPLE_RATE.to_owned()));
    }

    #[test]
    fn predicates_on_derived_labels_stay_above_the_extraction() {
        use tessera_ir::ExtractStage;
        let plan = Plan::Extract {
            input: Box::new(scan()),
            source: Expr::column(BODY),
            stage: ExtractStage::Json,
        }
        .filter(
            Expr::Label {
                name: "status".into(),
                data_type: arrow::datatypes::DataType::Int64,
            }
            .binary(BinaryOp::GtEq, Expr::literal(500))
            .and(Expr::column(SEVERITY_NUMBER).binary(BinaryOp::GtEq, Expr::literal(9))),
        )
        .project(vec![NamedExpr::new(Expr::column(BODY), BODY)]);
        let rewritten = rewrite(plan);
        let Plan::Project { input, .. } = &rewritten else {
            panic!("{rewritten}")
        };
        let Plan::Filter { input, predicate } = input.as_ref() else {
            panic!("{rewritten}")
        };
        assert!(predicate.to_string().contains("label(status)"));
        assert!(matches!(input.as_ref(), Plan::Extract { .. }));
        let scan = only_scan(&rewritten);
        assert_eq!(
            scan.predicates.len(),
            1,
            "the severity test reached the scan"
        );
        let columns = scan.projection.clone().unwrap();
        assert!(columns.contains(&BODY.to_owned()));
        assert!(
            columns.contains(&OVERFLOW.to_owned()),
            "labels read the overflow: {columns:?}"
        );
    }
}
