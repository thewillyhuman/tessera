// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The default alert rules for Tessera's own health (section 14.7),
//! over the node metrics in `_system.metrics`: intake and compaction
//! backlog, evaluator lag, query queueing and slot load. A node puts
//! them in the `_system` tenant's control store when it starts, in the
//! group `tessera`, leaving alone any rule of the same name an operator
//! has changed or removed since: a rule that exists is not touched.

use tessera_catalog::{ControlError, ControlStores, Table};
use tessera_eval::{AlertRule, Language};
use tessera_types::TenantId;

/// The rule group the default rules are in.
pub const GROUP: &str = "tessera";

/// One default rule: an SQL condition over the last two minutes of a
/// node metric, per node.
fn rule(name: &str, metric: &str, condition: &str, hold_seconds: u64, severity: &str) -> AlertRule {
    let mut rule = AlertRule::new(
        name,
        Language::Sql,
        format!(
            "SELECT attr.node AS node, max(value) AS value FROM metrics \
             WHERE name = '{metric}' AND timestamp > now() - INTERVAL '2 minutes' \
             GROUP BY attr.node HAVING max(value) {condition}"
        ),
    );
    rule.group = GROUP.into();
    rule.dataset = crate::observe::METRICS.into();
    rule.every_seconds = 60;
    rule.for_seconds = hold_seconds;
    rule.severity = severity.into();
    rule.annotations.insert(
        "summary".into(),
        format!("{metric} is {condition} on node {{{{node}}}}"),
    );
    rule
}

/// A default rule over how much a counter grew in the last two
/// minutes, per node.
fn growth_rule(name: &str, metric: &str, condition: &str, severity: &str) -> AlertRule {
    let mut rule = rule(name, metric, condition, 300, severity);
    rule.expr = format!(
        "SELECT attr.node AS node, max(value) - min(value) AS value FROM metrics \
         WHERE name = '{metric}' AND timestamp > now() - INTERVAL '2 minutes' \
         GROUP BY attr.node HAVING max(value) - min(value) {condition}"
    );
    rule.annotations.insert(
        "summary".into(),
        format!("{metric} grew {condition} in two minutes on node {{{{node}}}}"),
    );
    rule
}

/// The default rules.
#[must_use]
pub fn default_rules() -> Vec<AlertRule> {
    vec![
        rule(
            "tessera_write_ack",
            "tessera_write_ack_seconds",
            "> 5",
            300,
            "warning",
        ),
        growth_rule(
            "tessera_commit_conflicts",
            "tessera_commit_conflicts_total",
            "> 30",
            "warning",
        ),
        rule(
            "tessera_intake_backlog",
            "tessera_intake_backlog",
            "> 0",
            900,
            "warning",
        ),
        rule(
            "tessera_compaction_backlog",
            "tessera_compaction_backlog",
            "> 100",
            600,
            "warning",
        ),
        rule(
            "tessera_evaluator_lag",
            "tessera_evaluator_lag_seconds",
            "> 60",
            300,
            "critical",
        ),
        rule(
            "tessera_rules_degraded",
            "tessera_rules_degraded",
            "> 0",
            300,
            "warning",
        ),
        rule(
            "tessera_query_queue",
            "tessera_query_queue_seconds",
            "> 2",
            300,
            "warning",
        ),
        rule(
            "tessera_slot_load",
            "tessera_slots_owned",
            "> 256",
            600,
            "warning",
        ),
    ]
}

/// Puts every default rule that is missing in the `_system` tenant's
/// control store; returns how many were added.
///
/// # Errors
///
/// If the control store cannot be read or written.
pub async fn ensure_rules(controls: &ControlStores) -> Result<usize, ControlError> {
    let control = controls.open(&TenantId::SYSTEM);
    let rules = default_rules();
    let (added, _) = control
        .transact(|txn| {
            let mut added = 0;
            for rule in &rules {
                if txn.get(Table::AlertRules, &rule.name)?.is_some() {
                    continue;
                }
                let document = serde_json::to_value(rule)
                    .map_err(|e| ControlError::Rejected(e.to_string()))?;
                txn.put(Table::AlertRules, &rule.name, &document);
                added += 1;
            }
            Ok(added)
        })
        .await?;
    Ok(added)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tessera_object::MemoryStore;
    use tessera_types::{Env, Timestamp};

    use super::*;

    #[tokio::test]
    async fn the_default_rules_are_added_once_and_left_alone_after() {
        let store = Arc::new(MemoryStore::new());
        let env = Env::simulated(1, Timestamp::from_nanos(0));
        let controls = ControlStores::new(store, env);
        assert_eq!(ensure_rules(&controls).await.unwrap(), 8);
        let control = controls.open(&TenantId::SYSTEM);
        let mut changed: AlertRule = serde_json::from_value(
            control
                .get(Table::AlertRules, "tessera_evaluator_lag")
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(changed.group, GROUP);
        assert_eq!(changed.dataset, "metrics");
        assert!(
            changed.expr.contains("tessera_evaluator_lag_seconds"),
            "{}",
            changed.expr
        );
        changed.for_seconds = 1;
        control
            .put(
                Table::AlertRules,
                &changed.name,
                serde_json::to_value(&changed).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ensure_rules(&controls).await.unwrap(), 0);
        let kept: AlertRule = serde_json::from_value(
            control
                .get(Table::AlertRules, "tessera_evaluator_lag")
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(kept.for_seconds, 1, "the operator's change stays");
    }
}
