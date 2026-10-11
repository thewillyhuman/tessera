// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Periodic evaluation (CQ-8): a rule is re-executed at its interval
//! through the engine, in its own language, and each row or sample of
//! the result is one observed alert instance. This is the mode of every
//! rule that is not in the incremental subset of CQ-7.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use arrow::array::{
    Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow::datatypes::DataType;
use tessera_exec::{Engine, EngineError, QueryOutput, ScanUse};
use tessera_ir::{BindContext, Frontend, Statement};
use tessera_logql::LogqlFrontend;
use tessera_opt::{OptError, optimize};
use tessera_promql::PromqlFrontend;
use tessera_sql::SqlFrontend;
use tessera_types::event::{EXTRACTED, VALUE};
use tessera_types::{DatasetName, InvalidId, TenantId, Timestamp};

use crate::lifecycle::Observation;
use crate::rule::{AlertRule, Language};

/// Why a rule could not be evaluated.
#[derive(Debug, thiserror::Error)]
pub enum EvalError {
    /// The rule's expression is not a query the evaluator can run.
    #[error("rule {rule}: {message}")]
    Expression {
        /// The rule.
        rule: String,
        /// What is wrong.
        message: String,
    },
    /// The rule names a dataset that is not a valid name.
    #[error("rule {rule}: {source}")]
    Dataset {
        /// The rule.
        rule: String,
        /// The failure.
        #[source]
        source: InvalidId,
    },
    /// A rollup could not be maintained.
    #[error("rollup {rollup}: {message}")]
    Rollup {
        /// The rollup.
        rollup: String,
        /// What failed.
        message: String,
    },
    /// The plan does not type or bind.
    #[error("rule {rule}: {source}")]
    Plan {
        /// The rule.
        rule: String,
        /// The failure.
        #[source]
        source: OptError,
    },
    /// Running the plan failed.
    #[error("rule {rule}: {source}")]
    Engine {
        /// The rule.
        rule: String,
        /// The failure.
        #[source]
        source: EngineError,
    },
    /// The rule group's state log failed.
    #[error("rule state: {0}")]
    State(String),
    /// Alert events could not be written.
    #[error("alert events: {0}")]
    Sink(String),
}

/// The label Prometheus gives the rule's name on every instance.
pub const ALERTNAME: &str = "alertname";
/// The label the rule's severity goes under.
pub const SEVERITY: &str = "severity";

/// Runs `rule` at `now` and returns what it observed: one entry per
/// result row or sample.
///
/// What running a rule through the engine asked of its scans (AD-1):
/// the workload log's view of the rule, for the evaluator to record.
#[derive(Clone, Debug, PartialEq)]
pub struct RuleRun {
    /// The rule.
    pub rule: String,
    /// The rule's language.
    pub language: Language,
    /// What each scan read and was asked.
    pub uses: Vec<ScanUse>,
    /// The plan's fingerprint (CO-3).
    pub fingerprint: u64,
    /// How long the run took.
    pub latency: Duration,
}

/// # Errors
///
/// If the expression does not lower in its language, is not a query, or
/// cannot be planned or run.
pub async fn observe(
    engine: &Engine,
    tenant: &TenantId,
    rule: &AlertRule,
    now: Timestamp,
) -> Result<(Vec<Observation>, RuleRun), EvalError> {
    let expression = |message: String| EvalError::Expression {
        rule: rule.name.clone(),
        message,
    };
    let dataset = DatasetName::new(&rule.dataset).map_err(|source| EvalError::Dataset {
        rule: rule.name.clone(),
        source,
    })?;
    let ctx = BindContext::new(tenant.clone(), dataset, now);
    let instant = |start: Timestamp| (start, now, Duration::ZERO);
    let plan = match rule.language {
        Language::Sql => match SqlFrontend
            .lower(&rule.expr, &ctx)
            .map_err(|d| expression(d.describe(&rule.expr)))?
        {
            Statement::Query(plan) => plan,
            Statement::Explain { .. } | Statement::Control(_) => {
                return Err(expression(
                    "an alert rule is a query, not EXPLAIN or a control statement".into(),
                ));
            }
        },
        Language::Promql => {
            let (start, end, step) = instant(now);
            let lowered = PromqlFrontend {
                params: tessera_promql::RangeParams { start, end, step },
            }
            .lower_query(&rule.expr, &ctx)
            .map_err(|d| expression(d.describe(&rule.expr)))?;
            match lowered.kind {
                tessera_promql::ResultKind::Vector | tessera_promql::ResultKind::Scalar => {
                    lowered.plan
                }
                tessera_promql::ResultKind::Matrix | tessera_promql::ResultKind::String => {
                    return Err(expression(
                        "a PromQL alert rule must produce an instant vector or a scalar".into(),
                    ));
                }
            }
        }
        Language::Logql => {
            let (start, end, step) = instant(now);
            let lowered = LogqlFrontend {
                params: tessera_logql::RangeParams { start, end, step },
            }
            .lower_query(&rule.expr, &ctx)
            .map_err(|d| expression(d.describe(&rule.expr)))?;
            match lowered.kind {
                tessera_logql::ResultKind::Vector | tessera_logql::ResultKind::Scalar => {
                    lowered.plan
                }
                tessera_logql::ResultKind::Streams | tessera_logql::ResultKind::Matrix => {
                    return Err(expression(
                        "a LogQL alert rule must be a metric query at one instant".into(),
                    ));
                }
            }
        }
    };
    let plan = optimize(plan, engine.registry()).map_err(|source| EvalError::Plan {
        rule: rule.name.clone(),
        source,
    })?;
    let started = Instant::now();
    let output = engine
        .run(&plan)
        .await
        .map_err(|source| EvalError::Engine {
            rule: rule.name.clone(),
            source,
        })?;
    let run = RuleRun {
        rule: rule.name.clone(),
        language: rule.language,
        uses: tessera_exec::workload::uses(&plan, &output.analysis),
        fingerprint: tessera_exec::cache::fingerprint(&plan),
        latency: started.elapsed(),
    };
    let observations = match rule.language {
        Language::Sql => observe_rows(&output),
        Language::Promql | Language::Logql => observe_samples(&output.batches),
    };
    Ok((observations, run))
}

/// The labels of an instance: the rule's name and severity, the rule's
/// labels, then the observed ones, which win.
#[must_use]
pub fn instance_labels(
    rule: &AlertRule,
    observed: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(ALERTNAME.to_owned(), rule.name.clone());
    labels.insert(SEVERITY.to_owned(), rule.severity.clone());
    labels.extend(rule.labels.clone());
    labels.extend(observed.clone());
    labels
}

/// SQL rows: every text column is a label, the last numeric column the
/// value, one when there is none. Counts and sums far below 2^53 lose
/// nothing in the conversion.
#[allow(clippy::cast_precision_loss)]
fn observe_rows(output: &QueryOutput) -> Vec<Observation> {
    let mut observations = Vec::new();
    for batch in &output.batches {
        for row in 0..batch.num_rows() {
            let mut labels = BTreeMap::new();
            let mut value = 1.0;
            for (index, field) in batch.schema().fields().iter().enumerate() {
                let column = batch.column(index);
                if column.is_null(row) {
                    continue;
                }
                match field.data_type() {
                    DataType::Utf8 => {
                        if let Some(text) = column.as_any().downcast_ref::<StringArray>() {
                            labels.insert(field.name().clone(), text.value(row).to_owned());
                        }
                    }
                    DataType::Float64 => {
                        if let Some(v) = column.as_any().downcast_ref::<Float64Array>() {
                            value = v.value(row);
                        }
                    }
                    DataType::Int64 => {
                        if let Some(v) = column.as_any().downcast_ref::<Int64Array>() {
                            value = v.value(row) as f64;
                        }
                    }
                    DataType::Int32 => {
                        if let Some(v) = column.as_any().downcast_ref::<Int32Array>() {
                            value = f64::from(v.value(row));
                        }
                    }
                    DataType::UInt64 => {
                        if let Some(v) = column.as_any().downcast_ref::<UInt64Array>() {
                            value = v.value(row) as f64;
                        }
                    }
                    _ => {}
                }
            }
            observations.push(Observation { labels, value });
        }
    }
    observations
}

/// Range-vector rows: the labels JSON and the value of each sample.
fn observe_samples(batches: &[RecordBatch]) -> Vec<Observation> {
    let mut observations = Vec::new();
    for batch in batches {
        let Some(values) = batch
            .column_by_name(VALUE)
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        else {
            continue;
        };
        let labels = batch
            .column_by_name(EXTRACTED)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        for row in 0..batch.num_rows() {
            let labels: BTreeMap<String, String> = labels
                .filter(|l| !l.is_null(row))
                .and_then(|l| serde_json::from_str(l.value(row)).ok())
                .unwrap_or_default();
            observations.push(Observation {
                labels,
                value: if values.is_null(row) {
                    f64::NAN
                } else {
                    values.value(row)
                },
            });
        }
    }
    observations
}
