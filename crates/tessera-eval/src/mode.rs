// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Which evaluation mode a rule runs in (CQ-8): the incremental subset
//! of CQ-7 over writer-log segments, or periodic re-execution through
//! the engine. `EXPLAIN ALERT` states the mode and why.

use std::fmt;
use std::time::Duration;

use crate::rule::AlertRule;

/// How a rule is evaluated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Re-executed through the engine at its interval (CQ-8).
    Periodic {
        /// The interval.
        every: Duration,
        /// Why the rule is not evaluated incrementally.
        reason: String,
    },
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Mode::Periodic { every, reason } => write!(
                f,
                "periodic: re-executed through the engine every {}s; {reason}",
                every.as_secs()
            ),
        }
    }
}

/// The mode `rule` runs in.
#[must_use]
pub fn mode_of(rule: &AlertRule) -> Mode {
    Mode::Periodic {
        every: rule.every(),
        reason: "incremental evaluation over segments is not available yet".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rule::Language;

    #[test]
    fn every_rule_is_periodic_for_now() {
        let rule = AlertRule::new("r", Language::Sql, "SELECT 1");
        let mode = mode_of(&rule);
        assert!(matches!(mode, Mode::Periodic { .. }));
        assert!(
            mode.to_string()
                .starts_with("periodic: re-executed through the engine every 60s")
        );
    }
}
