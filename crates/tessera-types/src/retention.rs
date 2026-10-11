// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Retention policies (section 13.3): what to do with data as it ages,
//! by kind, severity, value or pattern. A policy is a row of
//! `system.retention_policies`, written by `CREATE RETENTION POLICY`,
//! and is applied at compaction (CO-9); nothing is reduced by value
//! without an enabled policy (CO-10).

use serde::{Deserialize, Serialize};

/// One retention policy of a dataset.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// The policy's name, unique in the tenant.
    pub name: String,
    /// The dataset it applies to.
    pub dataset: String,
    /// The clauses, first match wins; a clause without a condition is
    /// the `ELSE`.
    pub clauses: Vec<RetentionClause>,
    /// Whether the policy is applied.
    #[serde(default = "enabled")]
    pub enabled: bool,
}

fn enabled() -> bool {
    true
}

/// `WHEN condition THEN actions`, or `ELSE actions` without a condition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RetentionClause {
    /// The condition, as written in SQL, over `kind`, the row's
    /// columns and `value_score`; none for `ELSE`.
    pub condition: Option<String>,
    /// What to do, each at its own age.
    pub actions: Vec<RetentionAction>,
}

/// One thing a clause does to rows that match it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
pub enum RetentionAction {
    /// Keep a fraction of the rows after an age, with `sample_rate` set
    /// so weighted aggregates stay unbiased (CO-6, CO-7).
    Sample {
        /// The fraction kept, in `(0, 1]`.
        rate: f64,
        /// The age in seconds from which it applies.
        after_seconds: u64,
        /// Whether traces with an error span are kept whole (CO-7).
        #[serde(default)]
        except_errors: bool,
    },
    /// Remove the rows after an age; a whole partition at once when the
    /// policy says nothing finer (MF-6).
    Drop {
        /// The age in seconds from which it applies.
        after_seconds: u64,
    },
    /// Keep one row per series and interval after an age, with count,
    /// sum, min, max and last (CO-8).
    Downsample {
        /// The interval in seconds.
        interval_seconds: u64,
        /// The age in seconds from which it applies.
        after_seconds: u64,
    },
}

impl RetentionAction {
    /// The age the action applies from, in seconds.
    #[must_use]
    pub fn after_seconds(&self) -> u64 {
        match self {
            RetentionAction::Sample { after_seconds, .. }
            | RetentionAction::Drop { after_seconds }
            | RetentionAction::Downsample { after_seconds, .. } => *after_seconds,
        }
    }
}

impl std::fmt::Display for RetentionAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetentionAction::Sample {
                rate,
                after_seconds,
                except_errors,
            } => {
                write!(f, "SAMPLE {rate} AFTER {after_seconds}s")?;
                if *except_errors {
                    write!(f, " EXCEPT ERRORS")?;
                }
                Ok(())
            }
            RetentionAction::Drop { after_seconds } => write!(f, "DROP AFTER {after_seconds}s"),
            RetentionAction::Downsample {
                interval_seconds,
                after_seconds,
            } => write!(
                f,
                "DOWNSAMPLE TO {interval_seconds}s AFTER {after_seconds}s"
            ),
        }
    }
}
