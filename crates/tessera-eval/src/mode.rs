// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Which evaluation mode a rule runs in (CQ-8): the incremental subset
//! of CQ-7 over writer-log segments, or periodic re-execution through
//! the engine. `EXPLAIN ALERT` states the mode and why.

use std::fmt;
use std::time::Duration;

use tessera_types::TenantId;

use crate::incremental::{Firing, shape_of};
use crate::rule::AlertRule;

/// How a rule is evaluated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Maintained from writer-log segments as they arrive (CQ-1).
    Incremental {
        /// The window length.
        window: Duration,
        /// How the condition fires.
        firing: Firing,
    },
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
            Mode::Incremental { window, firing } => write!(
                f,
                "incremental: maintained from writer-log segments over {}s windows, firing {}",
                window.as_secs(),
                match firing {
                    Firing::Eager => "as soon as the condition holds (it is monotone)",
                    Firing::Watermark => "when the watermark passes the window",
                }
            ),
            Mode::Periodic { every, reason } => write!(
                f,
                "periodic: re-executed through the engine every {}s; {reason}",
                every.as_secs()
            ),
        }
    }
}

/// The mode `rule` runs in for `tenant`.
#[must_use]
pub fn mode_of(rule: &AlertRule, tenant: &TenantId) -> Mode {
    match shape_of(rule, tenant) {
        Ok(shape) => Mode::Incremental {
            window: shape.window,
            firing: shape.firing,
        },
        Err(reason) => Mode::Periodic {
            every: rule.every(),
            reason,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rule::Language;

    #[test]
    fn rules_in_the_subset_are_incremental_and_the_rest_say_why() {
        let rule = AlertRule::new("r", Language::Sql, "SELECT 1");
        let mode = mode_of(&rule, &TenantId::DEFAULT);
        assert!(matches!(mode, Mode::Periodic { .. }));
        assert!(
            mode.to_string()
                .starts_with("periodic: re-executed through the engine every 60s")
        );
        let promql = AlertRule::new("p", Language::Promql, "up == 0");
        assert!(
            mode_of(&promql, &TenantId::DEFAULT)
                .to_string()
                .contains("engine")
        );
        let windowed = AlertRule::new(
            "w",
            Language::Sql,
            "SELECT attr.service AS service, count(*) AS n FROM logs \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute') HAVING count(*) > 5",
        );
        let mode = mode_of(&windowed, &TenantId::DEFAULT);
        assert_eq!(
            mode,
            Mode::Incremental {
                window: Duration::from_secs(60),
                firing: Firing::Eager
            }
        );
        let averaged = AlertRule::new(
            "a",
            Language::Sql,
            "SELECT avg(severity_number) AS s FROM logs \
             GROUP BY tumble(timestamp, INTERVAL '5m') HAVING avg(severity_number) > 10",
        );
        assert!(matches!(
            mode_of(&averaged, &TenantId::DEFAULT),
            Mode::Incremental {
                firing: Firing::Watermark,
                ..
            }
        ));
    }
}
