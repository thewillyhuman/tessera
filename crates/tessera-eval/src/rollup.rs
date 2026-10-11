// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Rollups (section 8.7): a continuous query whose output is written to
//! a derived dataset, so that queries with the same shape read a few
//! aggregated rows instead of the raw data (AD-13). A rollup is a row of
//! `system.rollups`, written by `CREATE ROLLUP` or by the advisor; its
//! query is a grouped aggregation over one tumbling window, without a
//! `HAVING`, whose aggregates can be merged again when a query asks for
//! a longer window.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tessera_ir::RollupDefinition;
use tessera_ir::rollup::{RollupAggregate, RollupEntry, RollupKey, stored_type};
use tessera_types::{DatasetName, TenantId, TimeRange};

use crate::incremental::{Shape, shape_of_sql};

/// Who defined a rollup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Origin {
    /// `CREATE ROLLUP`.
    User,
    /// The advisor, from a repeated query shape.
    Advisor,
}

/// One rollup. Rows of `system.rollups`, keyed by name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rollup {
    /// The rollup's name, unique in the tenant.
    pub name: String,
    /// The SQL query: a grouped aggregation over one tumbling window.
    pub expr: String,
    /// The dataset an unqualified table name in the query means.
    pub dataset: String,
    /// The derived dataset the output is written to.
    pub target: String,
    /// The window length, from the query.
    pub window_seconds: u64,
    /// Seconds between evaluations.
    pub every_seconds: u64,
    /// Who defined it.
    #[serde(default = "user")]
    pub origin: Origin,
    /// Whether it is maintained and read.
    #[serde(default = "enabled")]
    pub enabled: bool,
    /// How far back the first rounds reach, in seconds; the newest
    /// closed window alone when zero.
    #[serde(default)]
    pub backfill_seconds: u64,
}

fn user() -> Origin {
    Origin::User
}

fn enabled() -> bool {
    true
}

impl Rollup {
    /// The rollup `CREATE ROLLUP` defines, checked against what a rollup
    /// can be.
    ///
    /// # Errors
    ///
    /// If the query is not a grouped aggregation over one tumbling
    /// window of mergeable aggregates, has a `HAVING`, averages (store
    /// the sum and the count instead), or the target is not a dataset
    /// name.
    pub fn from_definition(
        definition: &RollupDefinition,
        tenant: &TenantId,
    ) -> Result<Self, String> {
        let target = match &definition.target {
            Some(target) => target.clone(),
            None => DatasetName::new(format!("rollup_{}", definition.name))
                .map_err(|e| format!("the default target is not a dataset name: {e}"))?,
        };
        let rollup = Rollup {
            name: definition.name.clone(),
            expr: definition.query.clone(),
            dataset: definition.dataset.to_string(),
            target: target.to_string(),
            window_seconds: 0,
            every_seconds: definition.every.map_or(0, |d| d.as_secs()),
            origin: Origin::User,
            enabled: true,
            backfill_seconds: definition.backfill.map_or(0, |d| d.as_secs()),
        };
        let shape = rollup.shape(tenant)?;
        if shape.hop.is_some() {
            return Err(
                "a rollup needs a tumbling window; hopping and sliding windows are for alert rules"
                    .into(),
            );
        }
        Ok(Rollup {
            window_seconds: shape.window.as_secs(),
            every_seconds: rollup.every_seconds.max(shape.window.as_secs()).max(1),
            ..rollup
        })
    }

    /// The shape of the rollup's query, checked as `from_definition`
    /// does.
    ///
    /// # Errors
    ///
    /// Why the query cannot be a rollup.
    pub fn shape(&self, tenant: &TenantId) -> Result<Shape, String> {
        let shape = shape_of_sql(&self.expr, &self.dataset, tenant, None)?;
        if shape.having.is_some() {
            return Err("a rollup keeps every group; drop the HAVING".into());
        }
        if let Some(average) = shape
            .aggregates
            .iter()
            .find(|a| a.function == "avg" || a.function == "avg_raw")
        {
            return Err(format!(
                "{} cannot be merged across windows; store sum and count instead",
                average.name
            ));
        }
        if shape.target_is(&self.target) {
            return Err("a rollup cannot read its own target".into());
        }
        Ok(shape)
    }

    /// The rollup as the planner reads it (AD-13), covering `covered`.
    ///
    /// # Errors
    ///
    /// Why the query cannot be a rollup, or the target is not a dataset
    /// name.
    pub fn entry(&self, tenant: &TenantId, covered: TimeRange) -> Result<RollupEntry, String> {
        let shape = self.shape(tenant)?;
        let target = DatasetName::new(&self.target).map_err(|e| e.to_string())?;
        Ok(RollupEntry {
            name: self.name.clone(),
            target,
            dataset: shape.dataset,
            kind: shape.kind,
            predicate: shape.predicate,
            keys: shape
                .keys
                .into_iter()
                .zip(&shape.key_types)
                .map(|((name, expr), data_type)| RollupKey {
                    name,
                    expr,
                    data_type: stored_type(data_type),
                })
                .collect(),
            window: shape.window,
            aggregates: shape
                .aggregates
                .into_iter()
                .zip(&shape.aggregate_types)
                .map(|(aggregate, data_type)| RollupAggregate {
                    aggregate,
                    data_type: stored_type(data_type),
                })
                .collect(),
            covered,
        })
    }

    /// The window length.
    #[must_use]
    pub fn window(&self) -> Duration {
        Duration::from_secs(self.window_seconds.max(1))
    }

    /// How often the rollup is evaluated; at least the window.
    #[must_use]
    pub fn every(&self) -> Duration {
        Duration::from_secs(self.every_seconds.max(self.window_seconds).max(1))
    }

    /// How far back the first rounds reach.
    #[must_use]
    pub fn backfill(&self) -> Duration {
        Duration::from_secs(self.backfill_seconds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition(query: &str) -> RollupDefinition {
        RollupDefinition {
            name: "errors_by_service".into(),
            every: None,
            target: None,
            query: query.into(),
            backfill: None,
            dataset: DatasetName::new("prod").unwrap(),
        }
    }

    #[test]
    fn a_rollup_is_a_windowed_aggregation_with_a_default_target() {
        let rollup = Rollup::from_definition(
            &definition(
                "SELECT attr.service AS service, count(*) AS n FROM logs \
                 WHERE severity_number >= 17 \
                 GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')",
            ),
            &TenantId::DEFAULT,
        )
        .unwrap();
        assert_eq!(rollup.target, "rollup_errors_by_service");
        assert_eq!(rollup.window_seconds, 60);
        assert_eq!(rollup.every_seconds, 60, "every defaults to the window");
        assert_eq!(rollup.origin, Origin::User);
        assert!(rollup.enabled);
        let shape = rollup.shape(&TenantId::DEFAULT).unwrap();
        assert_eq!(shape.keys.len(), 1);
        assert_eq!(shape.window, Duration::from_secs(60));
    }

    #[test]
    fn refuses_what_cannot_be_merged_again() {
        let errors = [
            (
                "SELECT count(*) AS n FROM logs",
                "no tumbling or hopping window",
            ),
            (
                "SELECT count(*) AS n FROM logs GROUP BY hop(timestamp, INTERVAL '2m', INTERVAL '1m')",
                "needs a tumbling window",
            ),
            (
                "SELECT count(*) AS n FROM logs GROUP BY tumble(timestamp, INTERVAL '1m') \
                 HAVING count(*) > 1",
                "HAVING",
            ),
            (
                "SELECT avg(severity_number) AS s FROM logs GROUP BY tumble(timestamp, INTERVAL '1m')",
                "sum and count",
            ),
        ];
        for (query, expected) in errors {
            let error =
                Rollup::from_definition(&definition(query), &TenantId::DEFAULT).unwrap_err();
            assert!(error.contains(expected), "{query}: {error}");
        }
    }

    #[test]
    fn the_planner_entry_carries_stored_types_and_coverage() {
        let rollup = Rollup::from_definition(
            &definition(
                "SELECT attr.service AS service, count(*) AS n, max(severity_number) AS worst \
                 FROM logs GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')",
            ),
            &TenantId::DEFAULT,
        )
        .unwrap();
        let covered = TimeRange::new(
            tessera_types::Timestamp::from_nanos(0),
            tessera_types::Timestamp::from_nanos(120_000_000_000),
        );
        let entry = rollup.entry(&TenantId::DEFAULT, covered).unwrap();
        assert_eq!(entry.target.as_str(), "rollup_errors_by_service");
        assert_eq!(entry.keys.len(), 1);
        assert_eq!(entry.keys[0].name, "service");
        assert_eq!(entry.keys[0].data_type, arrow::datatypes::DataType::Utf8);
        assert_eq!(entry.aggregates.len(), 2);
        assert_eq!(
            entry.aggregates[1].data_type,
            arrow::datatypes::DataType::Int64,
            "widened from the 32-bit severity"
        );
        assert_eq!(entry.covered, covered);
    }

    #[test]
    fn every_is_at_least_the_window() {
        let mut definition =
            definition("SELECT count(*) AS n FROM logs GROUP BY tumble(timestamp, INTERVAL '5m')");
        definition.every = Some(Duration::from_secs(30));
        let rollup = Rollup::from_definition(&definition, &TenantId::DEFAULT).unwrap();
        assert_eq!(rollup.every(), Duration::from_secs(300));
    }
}
