// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Budgets (section 13.4): how many bytes a tenant, or one of its teams,
//! may ingest or scan per UTC day, and what happens to what arrives
//! beyond that: alert only (the default), sample it at ingest, or
//! reject it. Budgets are tenant settings; the ledger that counts
//! against them lives with admission control.

use serde::{Deserialize, Serialize};

/// What happens to what arrives beyond a budget.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BudgetAction {
    /// Admit it, and record once a day that the budget was reached.
    #[default]
    Alert,
    /// Keep `rate` of it, with `sample_rate` set on the kept rows
    /// (CO-6); only ingest can sample.
    Sample {
        /// The share kept, above 0 and below 1.
        rate: f64,
    },
    /// Refuse it until the day ends.
    Reject,
}

/// A budget: bytes per UTC day and the action beyond them.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Budget {
    /// The bytes allowed per UTC day.
    pub bytes_per_day: u64,
    /// What happens beyond them.
    #[serde(default)]
    pub action: BudgetAction,
}

/// Sound because a validated budget never carries a `NaN` rate, and
/// budgets are validated before they are stored or compared.
impl Eq for Budget {}

/// Why a budget is not valid.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidBudget {
    /// A budget of nothing would refuse or sample everything.
    #[error("a budget must allow at least one byte per day")]
    Empty,
    /// The sampling rate keeps nothing or everything.
    #[error("a sampling rate must be above 0 and below 1")]
    Rate,
    /// Queries cannot be sampled.
    #[error("a query budget cannot sample; use alert or reject")]
    QuerySample,
}

impl Budget {
    /// Checks the budget for ingest.
    ///
    /// # Errors
    ///
    /// If it allows nothing or samples at a rate outside (0, 1).
    pub fn validate_for_ingest(&self) -> Result<(), InvalidBudget> {
        if self.bytes_per_day == 0 {
            return Err(InvalidBudget::Empty);
        }
        match self.action {
            BudgetAction::Sample { rate } if !(rate > 0.0 && rate < 1.0) => {
                Err(InvalidBudget::Rate)
            }
            _ => Ok(()),
        }
    }

    /// Checks the budget for queries, which cannot sample.
    ///
    /// # Errors
    ///
    /// If it allows nothing or its action is to sample.
    pub fn validate_for_queries(&self) -> Result<(), InvalidBudget> {
        if self.bytes_per_day == 0 {
            return Err(InvalidBudget::Empty);
        }
        match self.action {
            BudgetAction::Sample { .. } => Err(InvalidBudget::QuerySample),
            BudgetAction::Alert | BudgetAction::Reject => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_action_defaults_to_alert_and_sampling_carries_its_rate() {
        let budget: Budget = serde_json::from_str(r#"{"bytes_per_day": 10}"#).unwrap();
        assert_eq!(budget.action, BudgetAction::Alert);
        let budget: Budget =
            serde_json::from_str(r#"{"bytes_per_day": 10, "action": {"sample": {"rate": 0.1}}}"#)
                .unwrap();
        assert_eq!(budget.action, BudgetAction::Sample { rate: 0.1 });
        let budget: Budget =
            serde_json::from_str(r#"{"bytes_per_day": 10, "action": "reject"}"#).unwrap();
        assert_eq!(budget.action, BudgetAction::Reject);
    }

    #[test]
    fn validation_refuses_empty_budgets_bad_rates_and_sampled_queries() {
        let empty = Budget {
            bytes_per_day: 0,
            action: BudgetAction::Alert,
        };
        assert_eq!(empty.validate_for_ingest(), Err(InvalidBudget::Empty));
        assert_eq!(empty.validate_for_queries(), Err(InvalidBudget::Empty));
        let sampled = |rate| Budget {
            bytes_per_day: 1,
            action: BudgetAction::Sample { rate },
        };
        assert_eq!(sampled(1.0).validate_for_ingest(), Err(InvalidBudget::Rate));
        assert_eq!(sampled(0.0).validate_for_ingest(), Err(InvalidBudget::Rate));
        assert_eq!(sampled(0.5).validate_for_ingest(), Ok(()));
        assert_eq!(
            sampled(0.5).validate_for_queries(),
            Err(InvalidBudget::QuerySample)
        );
        let reject = Budget {
            bytes_per_day: 1,
            action: BudgetAction::Reject,
        };
        assert_eq!(reject.validate_for_queries(), Ok(()));
    }
}
