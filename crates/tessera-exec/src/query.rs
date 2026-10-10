// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! One query, end to end: text in any language, through its front end,
//! the optimizer and the engine. `EXPLAIN` shows the optimized plan and
//! `EXPLAIN ANALYZE` runs it and reports what each scan did (QE-4, QE-8).

use std::fmt::Write;

use tessera_ir::{BindContext, Diagnostic, Frontend, Plan, Statement};
use tessera_opt::{OptError, optimize};

use crate::engine::{Analysis, Engine, EngineError, QueryOutput};

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
}

/// Runs `text` under `ctx`.
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
    let statement = frontend.lower(text, ctx).map_err(QueryError::Syntax)?;
    match statement {
        Statement::Query(plan) => {
            let plan = optimize(plan, engine.registry())?;
            Ok(Response::Rows(engine.run(&plan).await?))
        }
        Statement::Explain { plan, analyze } => {
            let plan = optimize(plan, engine.registry())?;
            let analysis = if analyze {
                Some(engine.run(&plan).await?.analysis)
            } else {
                None
            };
            Ok(Response::Explain { plan, analysis })
        }
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
            "  bytes: {} in {} requests",
            scan.bytes_read, scan.requests
        );
        let _ = writeln!(
            out,
            "  rows: {} from files, {} from the tail ({} segments)",
            scan.rows_from_files, scan.rows_from_tail, scan.segments
        );
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
            Arc::new(ParquetFormat),
            Env::simulated(1, Timestamp::UNIX_EPOCH),
            CpuPool::new(1),
            QueryLimits::default(),
        )
    }

    fn ctx() -> BindContext {
        BindContext {
            tenant: TenantId::DEFAULT,
            dataset: DatasetName::new("prod").unwrap(),
            now: Timestamp::UNIX_EPOCH,
        }
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
