// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The advisor (spec section 8): from the workload log, weighted by age
//! (AD-2), and the files as they are, it chooses for each tenant,
//! dataset and kind a sort key of up to four columns before `timestamp`
//! (AD-5), the indexes the predicates call for (8.4), the attribute
//! paths worth a dedicated column (8.2) and the partition window
//! (AD-14). The cost of a layout is the decayed bytes the workload
//! would scan under it plus the cost of rewriting into it:
//!
//! ```text
//! cost(L) = Σ_q w_q · est_bytes_scanned(q, L) + λ · rewrite_bytes(current → L)
//! ```
//!
//! A new layout is adopted only when it beats the current one by a
//! margin on two consecutive runs (AD-7). Pins from the control store
//! override any choice (AD-12). Everything here is pure; the node reads
//! the inputs and writes the outcome.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tessera_manifest::{Advice, Alternative, LayoutRecord};
use tessera_types::event::TIMESTAMP;
use tessera_types::{Kind, Timestamp};

/// The half-life of a workload event's weight (AD-2).
pub const HALF_LIFE: Duration = Duration::from_secs(7 * 24 * 3600);
/// The margin a new layout must beat the current one by (AD-7).
pub const MARGIN: f64 = 0.2;
/// Consecutive runs that must agree before a layout is adopted (AD-7).
pub const RUNS_TO_ADOPT: u32 = 2;
/// Sort-key columns before `timestamp`.
pub const MAX_SORT_COLUMNS: usize = 4;
/// The least relative cost reduction that adds a sort column (AD-5).
pub const ADD_THRESHOLD: f64 = 0.05;
/// The weight of rewriting a byte against scanning one, λ.
pub const REWRITE_WEIGHT: f64 = 0.05;
/// The share of workload weight a path needs to earn a dedicated
/// column (the hot set of section 8.2).
pub const HOT_SHARE: f64 = 0.01;
/// Selectivity below which an equality column gets a Bloom filter.
pub const BLOOM_SELECTIVITY: f64 = 0.01;

/// One workload event, as the advisor reads it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Observation {
    /// When the query ran.
    pub at: Timestamp,
    /// Columns and paths it referenced.
    pub columns: BTreeSet<String>,
    /// Its predicates: column and operator.
    pub predicates: Vec<(String, String)>,
    /// The columns it grouped by.
    pub group_by: Vec<String>,
    /// Bytes the scan fetched.
    pub bytes_scanned: u64,
    /// Rows the scan read before predicates.
    pub rows_scanned: u64,
    /// Rows kept over rows scanned, when known.
    pub selectivity: Option<f64>,
    /// How the query was written, for the report.
    pub text: String,
}

/// A file as the cost model sees it.
#[derive(Clone, Debug, PartialEq)]
pub struct FileSummary {
    /// Its size.
    pub size_bytes: u64,
    /// Its sort key, `timestamp` last.
    pub sort_key: Vec<String>,
    /// Its layout epoch.
    pub epoch: u64,
    /// Its rows.
    pub rows: u64,
    /// Its partition's window start.
    pub window_start: Timestamp,
}

/// What a user pinned (AD-12); each part optional.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Pins {
    /// The sort key, `timestamp` implied last.
    #[serde(default)]
    pub sort_key: Option<Vec<String>>,
    /// Paths that always get a dedicated column.
    #[serde(default)]
    pub dedicated: Option<Vec<String>>,
    /// Columns that always get a Bloom filter.
    #[serde(default)]
    pub bloom: Option<Vec<String>>,
    /// The partition window, in seconds.
    #[serde(default)]
    pub window_seconds: Option<u64>,
}

/// Everything one run needs.
#[derive(Clone, Debug)]
pub struct Inputs {
    /// The kind of event.
    pub kind: Kind,
    /// The layout in force.
    pub current: LayoutRecord,
    /// The files of the kind, across partitions.
    pub files: Vec<FileSummary>,
    /// The workload, newest or oldest first, any order.
    pub observations: Vec<Observation>,
    /// The pins, if any.
    pub pins: Option<Pins>,
    /// The time of the run.
    pub now: Timestamp,
    /// The advisor's previous report, for the adoption streak.
    pub previous: Option<Advice>,
}

/// What a run concluded.
#[derive(Clone, Debug, PartialEq)]
pub struct Outcome {
    /// The report to record.
    pub advice: Advice,
    /// The layout to adopt now, when the streak and the margin allow;
    /// `None` keeps the current one.
    pub adopt: Option<LayoutRecord>,
    /// The partition window to set for new partitions.
    pub window: Duration,
}

/// The weight of an event `age` old (AD-2).
#[must_use]
pub fn decay(age: Duration) -> f64 {
    0.5_f64.powf(age.as_secs_f64() / HALF_LIFE.as_secs_f64())
}

/// One run of the advisor.
#[must_use]
pub fn advise(inputs: &Inputs) -> Outcome {
    let weighted = weigh(&inputs.observations, inputs.now);
    let pins = inputs.pins.clone().unwrap_or_default();
    let current_key = full_key(&inputs.current.sort_key);
    let current_cost = cost(&weighted, &inputs.files, &current_key, &current_key);
    let mut alternatives = vec![Alternative {
        sort_key: inputs.current.sort_key.clone(),
        cost: current_cost,
        current: true,
    }];
    let chosen = match &pins.sort_key {
        Some(pinned) => pinned.clone(),
        None => greedy(&weighted, &inputs.files, &current_key, &mut alternatives),
    };
    let chosen_cost = cost(&weighted, &inputs.files, &current_key, &full_key(&chosen));
    if !alternatives.iter().any(|a| a.sort_key == chosen) {
        alternatives.push(Alternative {
            sort_key: chosen.clone(),
            cost: chosen_cost,
            current: false,
        });
    }
    let hot = hot_paths(&weighted);
    let mut dedicated: BTreeSet<String> = hot.into_iter().collect();
    dedicated.extend(chosen.iter().filter(|c| is_path(c)).cloned());
    if let Some(pinned) = &pins.dedicated {
        dedicated.extend(pinned.iter().cloned());
    }
    let defaults = LayoutRecord::default_for(inputs.kind);
    let mut bloom: BTreeSet<String> = defaults.bloom.iter().cloned().collect();
    bloom.extend(bloom_candidates(&weighted));
    if let Some(pinned) = &pins.bloom {
        bloom.extend(pinned.iter().cloned());
    }
    let mut term: BTreeSet<String> = defaults.term.iter().cloned().collect();
    let mut trigram: BTreeSet<String> = defaults.trigram.iter().cloned().collect();
    for (column, op) in weighted
        .iter()
        .flat_map(|w| w.observation.predicates.iter())
    {
        match op.as_str() {
            "search" => {
                term.insert(column.clone());
            }
            "like" => {
                trigram.insert(column.clone());
            }
            _ => {}
        }
    }
    let proposed = LayoutRecord {
        epoch: inputs.current.epoch,
        sort_key: chosen.clone(),
        dedicated: dedicated.into_iter().collect(),
        bloom: bloom.into_iter().collect(),
        term: term.into_iter().collect(),
        trigram: trigram.into_iter().collect(),
    };
    let same_as_current = layout_matches(&proposed, &inputs.current);
    let streak = match &inputs.previous {
        Some(previous) if layout_matches(&previous.proposed, &proposed) => previous.streak + 1,
        _ => 1,
    };
    let beats = chosen_cost < current_cost * (1.0 - MARGIN) || pins.sort_key.is_some();
    let adopt = (!same_as_current && (beats || indexes_changed(&proposed, &inputs.current)))
        && (streak >= RUNS_TO_ADOPT || pins.sort_key.is_some());
    let mut drivers: Vec<(f64, String)> = weighted
        .iter()
        .map(|w| {
            (
                w.weight * bytes_of(&w.observation),
                w.observation.text.clone(),
            )
        })
        .collect();
    drivers.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    drivers.dedup_by(|a, b| a.1 == b.1);
    let advice = Advice {
        at: inputs.now,
        proposed: proposed.clone(),
        alternatives,
        streak,
        drivers: drivers.into_iter().take(5).map(|(_, text)| text).collect(),
    };
    let window = match pins.window_seconds {
        Some(seconds) => Duration::from_secs(seconds.max(60)),
        None => window_for(&inputs.files),
    };
    Outcome {
        advice,
        adopt: adopt.then_some(proposed),
        window,
    }
}

/// An observation with its decayed weight.
struct Weighted {
    observation: Observation,
    weight: f64,
}

fn weigh(observations: &[Observation], now: Timestamp) -> Vec<Weighted> {
    observations
        .iter()
        .map(|o| Weighted {
            observation: o.clone(),
            weight: decay(now.duration_since(o.at).unwrap_or(Duration::ZERO)),
        })
        .collect()
}

fn bytes_of(observation: &Observation) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let bytes = if observation.bytes_scanned > 0 {
        observation.bytes_scanned as f64
    } else {
        (observation.rows_scanned as f64) * 100.0
    };
    bytes.max(1.0)
}

fn full_key(sort_key: &[String]) -> Vec<String> {
    let mut key: Vec<String> = sort_key
        .iter()
        .filter(|c| c.as_str() != TIMESTAMP)
        .cloned()
        .collect();
    key.push(TIMESTAMP.to_owned());
    key
}

/// The per-column selectivity of a query: its observed selectivity
/// spread over its predicates, or a guess by operator.
fn column_selectivity(observation: &Observation, column: &str) -> Option<f64> {
    let predicates: Vec<&(String, String)> = observation
        .predicates
        .iter()
        .filter(|(_, op)| matches!(op.as_str(), "=" | "in" | "range"))
        .collect();
    let (_, op) = predicates.iter().find(|(c, _)| c == column)?;
    let guess: f64 = if op == "range" { 0.5 } else { 0.1 };
    match observation.selectivity {
        Some(s) if s > 0.0 && s < 1.0 => {
            #[allow(clippy::cast_precision_loss)]
            let spread = s.powf(1.0 / predicates.len().max(1) as f64);
            Some(spread)
        }
        _ => Some(guess),
    }
}

/// The bytes a query would scan under a sort key: its bytes times the
/// selectivity of every leading key column it filters on, a range
/// predicate ending the prefix.
fn estimated_bytes(observation: &Observation, sort_key: &[String]) -> f64 {
    let mut bytes = bytes_of(observation);
    for column in sort_key {
        if column == TIMESTAMP {
            break;
        }
        let Some(selectivity) = column_selectivity(observation, column) else {
            break;
        };
        bytes *= selectivity;
        let is_range = observation
            .predicates
            .iter()
            .any(|(c, op)| c == column && op == "range");
        if is_range {
            break;
        }
    }
    bytes
}

/// The cost of `candidate` for the workload, given the files are in
/// `current`.
fn cost(
    weighted: &[Weighted],
    files: &[FileSummary],
    current: &[String],
    candidate: &[String],
) -> f64 {
    let scan: f64 = weighted
        .iter()
        .map(|w| w.weight * estimated_bytes(&w.observation, candidate))
        .sum();
    let rewrite: f64 = if current == candidate {
        0.0
    } else {
        #[allow(clippy::cast_precision_loss)]
        let bytes: f64 = files
            .iter()
            .filter(|f| full_key(&f.sort_key) != candidate)
            .map(|f| f.size_bytes as f64)
            .sum();
        REWRITE_WEIGHT * bytes
    };
    scan + rewrite
}

/// Greedy selection (AD-5): add the column that most reduces cost,
/// lower-cardinality first among equals, until nothing improves cost
/// by the threshold or the key is full.
fn greedy(
    weighted: &[Weighted],
    files: &[FileSummary],
    current: &[String],
    alternatives: &mut Vec<Alternative>,
) -> Vec<String> {
    let mut candidates: BTreeMap<String, f64> = BTreeMap::new();
    for w in weighted {
        for (column, op) in &w.observation.predicates {
            if matches!(op.as_str(), "=" | "in" | "range") && column != TIMESTAMP {
                *candidates.entry(column.clone()).or_default() += w.weight;
            }
        }
    }
    let mut key: Vec<String> = Vec::new();
    // The bar to clear is the layout in force, which costs no rewrite.
    let mut best = cost(weighted, files, current, current);
    while key.len() < MAX_SORT_COLUMNS {
        let mut winner: Option<(String, f64, f64)> = None;
        for column in candidates.keys() {
            if key.contains(column) {
                continue;
            }
            let mut trial = key.clone();
            trial.push(column.clone());
            let trial_cost = cost(weighted, files, current, &full_key(&trial));
            // Lower cardinality earlier: among near-equal costs, prefer
            // the column whose predicates keep more rows.
            let cardinality = weighted
                .iter()
                .filter_map(|w| column_selectivity(&w.observation, column))
                .fold(0.0_f64, f64::max);
            let better = match &winner {
                None => true,
                Some((_, c, card)) => {
                    trial_cost < *c * 0.999
                        || ((trial_cost - c).abs() <= c * 0.001 && cardinality > *card)
                }
            };
            if better {
                winner = Some((column.clone(), trial_cost, cardinality));
            }
        }
        let Some((column, trial_cost, _)) = winner else {
            break;
        };
        if trial_cost >= best * (1.0 - ADD_THRESHOLD) {
            break;
        }
        key.push(column.clone());
        best = trial_cost;
        alternatives.push(Alternative {
            sort_key: key.clone(),
            cost: trial_cost,
            current: false,
        });
    }
    if key.is_empty() {
        alternatives
            .iter()
            .find(|a| a.current)
            .map(|a| a.sort_key.clone())
            .unwrap_or_default()
    } else {
        key
    }
}

fn is_path(column: &str) -> bool {
    column.starts_with("attr.") || column.starts_with("resource.") || column.starts_with("scope.")
}

/// Paths referenced by at least [`HOT_SHARE`] of the workload weight.
fn hot_paths(weighted: &[Weighted]) -> Vec<String> {
    let total: f64 = weighted.iter().map(|w| w.weight).sum();
    if total <= 0.0 {
        return Vec::new();
    }
    let mut by_path: BTreeMap<String, f64> = BTreeMap::new();
    for w in weighted {
        for column in w.observation.columns.iter().filter(|c| is_path(c)) {
            *by_path.entry(column.clone()).or_default() += w.weight;
        }
    }
    by_path
        .into_iter()
        .filter(|(_, weight)| weight / total >= HOT_SHARE)
        .map(|(path, _)| path)
        .collect()
}

/// Columns with equality predicates whose observed selectivity is low
/// enough for a Bloom filter to pay (8.4).
fn bloom_candidates(weighted: &[Weighted]) -> Vec<String> {
    let mut found = BTreeSet::new();
    for w in weighted {
        for (column, op) in &w.observation.predicates {
            let point = matches!(op.as_str(), "=" | "in");
            let selective = w
                .observation
                .selectivity
                .is_some_and(|s| s < BLOOM_SELECTIVITY);
            if point && selective && column != TIMESTAMP {
                found.insert(column.clone());
            }
        }
    }
    found.into_iter().collect()
}

fn layout_matches(a: &LayoutRecord, b: &LayoutRecord) -> bool {
    a.sort_key == b.sort_key
        && a.dedicated == b.dedicated
        && a.bloom == b.bloom
        && a.term == b.term
        && a.trigram == b.trigram
}

fn indexes_changed(a: &LayoutRecord, b: &LayoutRecord) -> bool {
    a.dedicated != b.dedicated || a.bloom != b.bloom || a.term != b.term || a.trigram != b.trigram
}

/// The partition window for the files as they are (AD-14): an hour when
/// a day holds many files, a week when it holds few small ones, a day
/// otherwise.
#[must_use]
pub fn window_for(files: &[FileSummary]) -> Duration {
    const DAY: Duration = Duration::from_secs(86_400);
    if files.is_empty() {
        return DAY;
    }
    let mut per_window: BTreeMap<Timestamp, (usize, u64)> = BTreeMap::new();
    for file in files {
        let entry = per_window.entry(file.window_start).or_default();
        entry.0 += 1;
        entry.1 += file.rows;
    }
    #[allow(clippy::cast_precision_loss)]
    let windows = per_window.len() as f64;
    #[allow(clippy::cast_precision_loss)]
    let files_per_window = files.len() as f64 / windows;
    #[allow(clippy::cast_precision_loss)]
    let rows_per_window = per_window.values().map(|(_, r)| *r as f64).sum::<f64>() / windows;
    if files_per_window > 64.0 || rows_per_window > 50_000_000.0 {
        Duration::from_secs(3_600)
    } else if rows_per_window < 100_000.0 && files_per_window < 4.0 {
        7 * DAY
    } else {
        DAY
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(days: f64) -> Timestamp {
        #[allow(clippy::cast_possible_truncation)]
        Timestamp::from_nanos((days * 86_400.0 * 1e9) as i64)
    }

    fn query(at_days: f64, column: &str, op: &str, selectivity: f64) -> Observation {
        Observation {
            at: at(at_days),
            columns: BTreeSet::from([column.to_owned(), "body".to_owned()]),
            predicates: vec![(column.to_owned(), op.to_owned())],
            group_by: Vec::new(),
            bytes_scanned: 1_000_000,
            rows_scanned: 10_000,
            selectivity: Some(selectivity),
            text: format!("SELECT ... WHERE {column} {op} ..."),
        }
    }

    fn files(sort_key: &[&str], count: usize) -> Vec<FileSummary> {
        (0..count)
            .map(|i| FileSummary {
                size_bytes: 1_000_000,
                sort_key: sort_key.iter().map(|s| (*s).to_owned()).collect(),
                epoch: 0,
                rows: 10_000,
                window_start: at(f64::from(u32::try_from(i).unwrap_or(0))),
            })
            .collect()
    }

    fn inputs(observations: Vec<Observation>, previous: Option<Advice>) -> Inputs {
        Inputs {
            kind: Kind::Log,
            current: LayoutRecord::default_for(Kind::Log),
            files: files(&["severity_number", "timestamp"], 10),
            observations,
            pins: None,
            previous,
            now: at(10.0),
        }
    }

    #[test]
    fn weights_halve_every_seven_days() {
        assert!((decay(Duration::ZERO) - 1.0).abs() < 1e-9);
        assert!((decay(HALF_LIFE) - 0.5).abs() < 1e-9);
        assert!((decay(2 * HALF_LIFE) - 0.25).abs() < 1e-9);
    }

    #[test]
    fn a_filtered_attribute_leads_the_sort_key_after_two_runs() {
        let workload: Vec<Observation> = (0..20)
            .map(|i| query(9.0 + f64::from(i) / 40.0, "attr.host", "=", 0.001))
            .collect();
        let first = advise(&inputs(workload.clone(), None));
        assert_eq!(first.advice.proposed.sort_key[0], "attr.host");
        assert!(
            first
                .advice
                .proposed
                .dedicated
                .contains(&"attr.host".to_owned())
        );
        assert!(
            first
                .advice
                .proposed
                .bloom
                .contains(&"attr.host".to_owned()),
            "selective equality"
        );
        assert_eq!(first.advice.streak, 1);
        assert!(first.adopt.is_none(), "one run is not enough");
        assert!(first.advice.alternatives.iter().any(|a| a.current));
        assert!(!first.advice.drivers.is_empty());

        let second = advise(&inputs(workload, Some(first.advice.clone())));
        assert_eq!(second.advice.streak, 2);
        let adopted = second.adopt.expect("adopted on the second agreeing run");
        assert_eq!(adopted.sort_key[0], "attr.host");
    }

    #[test]
    fn an_unfiltered_workload_keeps_the_default_and_pins_win() {
        let workload: Vec<Observation> = (0..5)
            .map(|i| Observation {
                predicates: Vec::new(),
                ..query(9.0 + f64::from(i) / 10.0, "body", "search", 0.5)
            })
            .collect();
        let outcome = advise(&inputs(workload.clone(), None));
        assert_eq!(
            outcome.advice.proposed.sort_key,
            LayoutRecord::default_for(Kind::Log).sort_key
        );
        assert!(outcome.adopt.is_none());

        let mut pinned = inputs(workload, None);
        pinned.pins = Some(Pins {
            sort_key: Some(vec!["attr.region".into()]),
            window_seconds: Some(3_600),
            ..Pins::default()
        });
        let outcome = advise(&pinned);
        let adopted = outcome.adopt.expect("a pin is adopted at once");
        assert_eq!(adopted.sort_key, ["attr.region"]);
        assert!(adopted.dedicated.contains(&"attr.region".to_owned()));
        assert_eq!(outcome.window, Duration::from_secs(3_600));
    }

    #[test]
    fn old_queries_weigh_less_than_new_ones() {
        // Thirty queries two months ago against four this morning: the
        // decayed thirty weigh less than the fresh four.
        let mut workload: Vec<Observation> = (0..30)
            .map(|i| query(f64::from(i) / 60.0, "attr.old", "=", 0.01))
            .collect();
        workload.extend((0..4).map(|i| query(59.9 + f64::from(i) / 100.0, "attr.new", "=", 0.01)));
        let mut inputs = inputs(workload, None);
        inputs.now = at(60.0);
        let outcome = advise(&inputs);
        assert_eq!(
            outcome.advice.proposed.sort_key[0], "attr.new",
            "{:?}",
            outcome.advice.alternatives
        );
    }

    #[test]
    fn the_window_follows_the_file_counts() {
        assert_eq!(window_for(&[]), Duration::from_secs(86_400));
        let many: Vec<FileSummary> = (0..200)
            .map(|i| FileSummary {
                size_bytes: 1,
                sort_key: vec![],
                epoch: 0,
                rows: 1_000,
                window_start: at(f64::from(i % 2)),
            })
            .collect();
        assert_eq!(window_for(&many), Duration::from_secs(3_600));
        let few = files(&["timestamp"], 3);
        assert_eq!(window_for(&few), Duration::from_secs(7 * 86_400));
    }
}
