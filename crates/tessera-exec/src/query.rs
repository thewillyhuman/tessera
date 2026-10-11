// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! One query, end to end: text in any language, through its front end,
//! the optimizer and the engine. `EXPLAIN` shows the optimized plan and
//! `EXPLAIN ANALYZE` runs it and reports what each scan did (QE-4, QE-8).

use std::fmt::Write;
use std::sync::Arc;

use tessera_ir::{BindContext, ControlStatement, Diagnostic, Expr, Frontend, Plan, Statement};
use tessera_opt::{OptError, optimize};
use tessera_types::DatasetName;

use crate::engine::{Analysis, Engine, EngineError, QueryLimits, QueryOutput};
use crate::workload;

/// Why a query failed, at the stage it failed.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// The text is not a valid query in its language.
    #[error("{0}")]
    Syntax(Diagnostic),
    /// The plan does not type or bind.
    #[error("{0}")]
    Plan(#[from] OptError),
    /// Running the plan failed.
    #[error("{0}")]
    Engine(#[from] EngineError),
    /// The caller may not read a dataset the query scans (SE-1).
    #[error("dataset {dataset} may not be read with these credentials")]
    Forbidden {
        /// The dataset.
        dataset: DatasetName,
    },
}

/// What a statement produced.
#[derive(Debug)]
pub enum Response {
    /// The rows of a query.
    Rows(QueryOutput),
    /// The plan of an explained query, with what running it did when
    /// analyzed.
    Explain {
        /// The optimized plan.
        plan: Plan,
        /// Present for `EXPLAIN ANALYZE`.
        analysis: Option<Analysis>,
    },
    /// A control statement, which the engine does not carry out: the
    /// caller that holds the control store does.
    Control(ControlStatement),
}

/// Which datasets a caller may read.
pub type Readable = Arc<dyn Fn(&DatasetName) -> bool + Send + Sync>;

/// The condition every row of a dataset must pass for the caller, if
/// the caller reads it under one (SE-1).
pub type RowFilter = Arc<dyn Fn(&DatasetName) -> Option<Expr> + Send + Sync>;

/// What a caller may change about how a query runs.
#[derive(Clone)]
pub struct RunOptions {
    /// Whether a matching aggregation may read a rollup instead of the
    /// raw rows (AD-13); on unless the caller turns it off.
    pub rollups: bool,
    /// The datasets the caller may read (SE-1); every one when absent.
    pub readable: Option<Readable>,
    /// The condition the caller reads each dataset under (SE-1); none
    /// when absent.
    pub row_filter: Option<RowFilter>,
    /// The limits the query runs under (QE-18); the node's defaults
    /// when absent.
    pub limits: Option<QueryLimits>,
}

impl Default for RunOptions {
    fn default() -> Self {
        RunOptions {
            rollups: true,
            readable: None,
            row_filter: None,
            limits: None,
        }
    }
}

/// Runs `text` under `ctx` with the default options.
///
/// # Errors
///
/// A syntax error with its position, a planning error, or an execution
/// failure.
pub async fn run(
    engine: &Engine,
    frontend: &dyn Frontend,
    text: &str,
    ctx: &BindContext,
) -> Result<Response, QueryError> {
    run_with(engine, frontend, text, ctx, RunOptions::default()).await
}

/// Runs `text` under `ctx` with `options`.
///
/// # Errors
///
/// A syntax error with its position, a planning error, or an execution
/// failure.
pub async fn run_with(
    engine: &Engine,
    frontend: &dyn Frontend,
    text: &str,
    ctx: &BindContext,
    options: RunOptions,
) -> Result<Response, QueryError> {
    let statement = frontend.lower(text, ctx).map_err(QueryError::Syntax)?;
    let limits = options.limits.unwrap_or(*engine.limits());
    match statement {
        Statement::Query(plan) => {
            let plan = plan_of(engine, plan, ctx, options).await?;
            let mut output = engine.run_with(&plan, &limits).await?;
            output.uses = workload::uses(&plan, &output.analysis);
            output.fingerprint = crate::cache::fingerprint(&plan);
            Ok(Response::Rows(output))
        }
        Statement::Control(statement) => Ok(Response::Control(statement)),
        Statement::Explain { plan, analyze } => {
            let plan = plan_of(engine, plan, ctx, options).await?;
            let analysis = if analyze {
                Some(engine.run_with(&plan, &limits).await?.analysis)
            } else {
                None
            };
            Ok(Response::Explain { plan, analysis })
        }
    }
}

/// `plan` optimized, and reading the tenant's rollups where it may
/// (AD-13): substitution runs on the optimized plan, whose scans carry
/// their time range, and its result is optimized again.
async fn plan_of(
    engine: &Engine,
    plan: Plan,
    ctx: &BindContext,
    options: RunOptions,
) -> Result<Plan, QueryError> {
    // The restriction goes in before optimizing, so that its conditions
    // are bound and pushed down like the query's own.
    let restricted = options
        .row_filter
        .as_ref()
        .is_some_and(|filter| scans(&plan).iter().any(|s| filter(&s.dataset).is_some()));
    let plan = match &options.row_filter {
        Some(filter) => restrict(plan, filter.as_ref()),
        None => plan,
    };
    let plan = optimize(plan, engine.registry())?;
    if let Some(readable) = &options.readable {
        if let Some(scan) = scans(&plan).into_iter().find(|s| !readable(&s.dataset)) {
            return Err(QueryError::Forbidden {
                dataset: scan.dataset.clone(),
            });
        }
    }
    // A rollup holds every row, so a restricted read keeps to the rows.
    let Some(source) = engine.rollups().filter(|_| options.rollups && !restricted) else {
        return Ok(plan);
    };
    let entries = source.entries(&ctx.tenant).await;
    if entries.is_empty() {
        return Ok(plan);
    }
    let substituted = tessera_opt::substitute(plan.clone(), &entries);
    if substituted == plan {
        return Ok(plan);
    }
    Ok(optimize(substituted, engine.registry())?)
}

/// `plan` with every scan of a dataset `filter` names reading its
/// rows through the condition, pushed into the scan with the rest of
/// its predicates (SE-1).
#[must_use]
pub fn restrict(plan: Plan, filter: &dyn Fn(&DatasetName) -> Option<Expr>) -> Plan {
    tessera_opt::map_scans(plan, &mut |mut scan| {
        if let Some(condition) = filter(&scan.dataset) {
            scan.predicates.push(condition);
        }
        scan
    })
}

/// Every scan in `plan`.
fn scans(plan: &Plan) -> Vec<&tessera_ir::Scan> {
    match plan {
        Plan::Scan(scan) => vec![scan],
        other => other.inputs().into_iter().flat_map(scans).collect(),
    }
}

/// The text of an `EXPLAIN`: the plan, then one block per scan when
/// analyzed, each stage with what it eliminated.
#[must_use]
pub fn explain_text(plan: &Plan, analysis: Option<&Analysis>) -> String {
    let mut out = plan.to_string();
    let Some(analysis) = analysis else {
        return out;
    };
    if analysis.result_cached {
        let _ = writeln!(out, "Result: from the cache, no scan run");
    }
    if analysis.spilled_bytes > 0 {
        let _ = writeln!(
            out,
            "Spilled: {} bytes to local disk",
            analysis.spilled_bytes
        );
    }
    for stage in &analysis.stages {
        let _ = writeln!(
            out,
            "Stage {}: partial on {} peers, {} run here after a failure",
            stage.kind, stage.remote, stage.failed
        );
    }
    for scan in &analysis.scans {
        let _ = writeln!(out, "Scan {}:", scan.source);
        let _ = writeln!(
            out,
            "  partitions: {} of {} in time range",
            scan.partitions_kept, scan.partitions
        );
        let _ = writeln!(
            out,
            "  files: {} total, {} after time range, {} after path presence, {} after statistics, {} read",
            scan.files.files,
            scan.files.after_time_range,
            scan.files.after_path_presence,
            scan.files.after_statistics,
            scan.files_read
        );
        let _ = writeln!(
            out,
            "  indexes: {} files skipped, {} row groups skipped",
            scan.files_skipped_by_index, scan.row_groups_skipped_by_index
        );
        let _ = writeln!(
            out,
            "  row groups: {} read of {}",
            scan.row_groups_read, scan.row_groups
        );
        let _ = writeln!(
            out,
            "  bytes: {} in {} requests, {} footers from cache",
            scan.bytes_read, scan.requests, scan.footers_cached
        );
        let _ = writeln!(
            out,
            "  rows: {} from files, {} from the tail ({} segments), {} scanned before predicates",
            scan.rows_from_files, scan.rows_from_tail, scan.segments, scan.rows_scanned
        );
        if scan.remote_tasks > 0 {
            let _ = writeln!(
                out,
                "  peers: {} files in {} leaf tasks, {} tasks read here after a failure",
                scan.files_remote, scan.remote_tasks, scan.remote_failed
            );
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use tessera_format::ParquetFormat;
    use tessera_ir::{Literal, Span};
    use tessera_object::{MemoryStore, Pools};
    use tessera_types::{DatasetName, Env, PoolId, TenantId, Timestamp};

    use super::*;
    use crate::engine::QueryLimits;
    use crate::scheduler::CpuPool;

    /// SE-1: a row filter joins the predicates of every scan of its
    /// dataset and leaves other datasets and other nodes alone.
    #[test]
    fn restrict_pushes_the_condition_into_the_scans() {
        use tessera_ir::{Expr, Scan};
        use tessera_types::Kind;

        let dataset = |name: &str| DatasetName::new(name).unwrap();
        let scan = |name: &str| Plan::Scan(Scan::all(TenantId::DEFAULT, dataset(name), Kind::Log));
        let plan = Plan::Union {
            inputs: vec![scan("prod").filter(Expr::literal(true)), scan("staging")],
        };
        let condition =
            Expr::column("severity_number").binary(tessera_ir::BinaryOp::Gt, Expr::literal(9));
        let restricted = restrict(plan, &|name: &DatasetName| {
            (name.as_str() == "prod").then(|| condition.clone())
        });
        let scans = scans(&restricted);
        assert_eq!(scans.len(), 2);
        let prod = scans.iter().find(|s| s.dataset.as_str() == "prod").unwrap();
        assert_eq!(prod.predicates, vec![condition]);
        let staging = scans
            .iter()
            .find(|s| s.dataset.as_str() == "staging")
            .unwrap();
        assert!(staging.predicates.is_empty());
    }

    /// A front end for one language: `one` selects the number one,
    /// `explain one` explains it, anything else is an error at byte 0.
    struct Tiny;

    impl Frontend for Tiny {
        fn lower(&self, text: &str, _: &BindContext) -> Result<Statement, Diagnostic> {
            let plan = Plan::Values {
                schema: Arc::new(Schema::new(vec![Field::new("one", DataType::Int64, true)])),
                rows: vec![vec![Literal::Int(1)]],
            };
            match text {
                "one" => Ok(Statement::Query(plan)),
                "explain one" => Ok(Statement::Explain {
                    plan,
                    analyze: false,
                }),
                "analyze one" => Ok(Statement::Explain {
                    plan,
                    analyze: true,
                }),
                _ => Err(Diagnostic::at(Span::new(0, 1), "not a tiny query")),
            }
        }
    }

    fn engine() -> Engine {
        let store = Arc::new(MemoryStore::new());
        Engine::new(
            Arc::new(Pools::new(PoolId::new("root").unwrap(), store)),
            Arc::new(ParquetFormat::default()),
            Env::simulated(1, Timestamp::UNIX_EPOCH),
            CpuPool::new(1),
            QueryLimits::default(),
        )
    }

    fn ctx() -> BindContext {
        BindContext::new(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Timestamp::UNIX_EPOCH,
        )
    }

    #[tokio::test]
    async fn runs_queries_and_explains_them() {
        let engine = engine();
        let Response::Rows(output) = run(&engine, &Tiny, "one", &ctx()).await.unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(output.num_rows(), 1);
        let Response::Explain { plan, analysis } =
            run(&engine, &Tiny, "explain one", &ctx()).await.unwrap()
        else {
            panic!("expected explain");
        };
        assert!(analysis.is_none());
        assert_eq!(explain_text(&plan, None), "Values: 1 rows\n");
        let Response::Explain { analysis, .. } =
            run(&engine, &Tiny, "analyze one", &ctx()).await.unwrap()
        else {
            panic!("expected explain");
        };
        assert!(analysis.is_some());
    }

    #[tokio::test]
    async fn syntax_errors_keep_their_position() {
        let error = run(&engine(), &Tiny, "two", &ctx()).await.unwrap_err();
        let QueryError::Syntax(diagnostic) = error else {
            panic!("expected a syntax error");
        };
        assert_eq!(diagnostic.span, Some(Span::new(0, 1)));
    }
}
