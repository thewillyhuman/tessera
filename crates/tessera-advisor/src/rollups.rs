// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Rollup proposals (section 8.7): a windowed aggregation shape that the
//! workload log shows repeated often enough, typically a dashboard
//! panel or an alert rule, becomes a rollup the planner can read
//! (AD-13). The shape is the query itself with its time range removed,
//! so the proposal is the rollup's definition. A shape that already
//! has a rollup, enabled or not, is not proposed again: disabling a
//! rollup is how an operator declines it. Everything here is pure; the
//! node reads the workload and stores what is proposed.

use tessera_types::stable_hash;

/// How many times a shape must repeat in the lookback to be proposed.
pub const MIN_REPEATS: u64 = 20;
/// The most advisor rollups one dataset gets.
pub const MAX_PER_DATASET: usize = 8;

/// One aggregation shape seen in the workload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepeatedShape {
    /// The source dataset.
    pub dataset: String,
    /// The shape, as the rollup query it would be.
    pub sql: String,
    /// How many queries had it in the lookback.
    pub repeats: u64,
}

/// A rollup to create.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Proposal {
    /// The rollup's name, derived from the shape so it is stable.
    pub name: String,
    /// The source dataset.
    pub dataset: String,
    /// The rollup query.
    pub sql: String,
}

/// The shapes worth a rollup, most repeated first, that `existing`
/// (dataset and query of every rollup there is) does not cover, within
/// the cap per dataset.
#[must_use]
pub fn propose(shapes: &[RepeatedShape], existing: &[(String, String)]) -> Vec<Proposal> {
    let mut candidates: Vec<&RepeatedShape> = shapes
        .iter()
        .filter(|s| s.repeats >= MIN_REPEATS)
        .filter(|s| {
            !existing
                .iter()
                .any(|(dataset, sql)| dataset == &s.dataset && sql == &s.sql)
        })
        .collect();
    candidates.sort_by(|a, b| b.repeats.cmp(&a.repeats).then_with(|| a.sql.cmp(&b.sql)));
    let mut proposals: Vec<Proposal> = Vec::new();
    for shape in candidates {
        let taken = existing.iter().filter(|(d, _)| d == &shape.dataset).count()
            + proposals
                .iter()
                .filter(|p| p.dataset == shape.dataset)
                .count();
        if taken >= MAX_PER_DATASET {
            continue;
        }
        proposals.push(Proposal {
            name: name_of(&shape.dataset, &shape.sql),
            dataset: shape.dataset.clone(),
            sql: shape.sql.clone(),
        });
    }
    proposals
}

/// `auto_` and eight hex digits of the shape's hash.
#[must_use]
pub fn name_of(dataset: &str, sql: &str) -> String {
    let hash = stable_hash(format!("{dataset}\n{sql}").as_bytes());
    format!("auto_{:08x}", hash & 0xffff_ffff)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(dataset: &str, sql: &str, repeats: u64) -> RepeatedShape {
        RepeatedShape {
            dataset: dataset.into(),
            sql: sql.into(),
            repeats,
        }
    }

    #[test]
    fn repeated_shapes_without_a_rollup_are_proposed_most_repeated_first() {
        let shapes = [
            shape("prod", "SELECT a", 25),
            shape("prod", "SELECT b", 100),
            shape("prod", "SELECT c", 5),
            shape("prod", "SELECT d", 40),
        ];
        let existing = [("prod".to_owned(), "SELECT d".to_owned())];
        let proposals = propose(&shapes, &existing);
        let sqls: Vec<&str> = proposals.iter().map(|p| p.sql.as_str()).collect();
        assert_eq!(sqls, ["SELECT b", "SELECT a"]);
        assert!(proposals[0].name.starts_with("auto_"));
        assert_eq!(proposals[0].name.len(), 13);
        assert_eq!(
            proposals[0].name,
            name_of("prod", "SELECT b"),
            "the name follows the shape"
        );
    }

    #[test]
    fn a_dataset_gets_at_most_the_cap() {
        let shapes: Vec<RepeatedShape> = (0..20)
            .map(|i| shape("prod", &format!("SELECT {i}"), 50 + i))
            .collect();
        let existing: Vec<(String, String)> = (0..3)
            .map(|i| ("prod".to_owned(), format!("SELECT existing {i}")))
            .collect();
        let proposals = propose(&shapes, &existing);
        assert_eq!(proposals.len(), MAX_PER_DATASET - 3);
        let other = [shape("staging", "SELECT x", 30)];
        assert_eq!(
            propose(&other, &existing).len(),
            1,
            "another dataset is not capped by prod"
        );
    }
}
