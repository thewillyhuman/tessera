// SPDX-License-Identifier: Apache-2.0

//! Counting against budgets (section 13.4): a ledger of the bytes each
//! tenant and team has ingested, and each tenant's queries have
//! scanned, in the current UTC day. Before a write or a query the
//! ledger says whether the budget's action applies; after it, whether
//! this was the first time the budget was reached today, so that the
//! alert is recorded once.
//!
//! The ledger counts what this node admitted since it started, plus
//! what the other nodes published of their own counts, which the node
//! absorbs on the heartbeat cadence; a tenant served by several
//! ingesters has one budget, within that cadence. Seeding the ledger
//! from `system.usage` after every node restarted is a gap.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use tessera_types::{Budget, BudgetAction, TenantId, Timestamp};

const NANOS_PER_DAY: i64 = 86_400 * 1_000_000_000;

/// What a budget limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    /// Bytes received.
    Ingest,
    /// Bytes scanned.
    Query,
}

/// Whose usage is counted: a tenant, or one of its teams.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Account {
    /// The tenant.
    pub tenant: TenantId,
    /// What is counted.
    pub phase: Phase,
    /// The team, for a team budget.
    pub team: Option<String>,
}

/// What the ledger says about one request.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Charge {
    /// The budget's action, when the request goes beyond the budget.
    pub action: Option<BudgetAction>,
    /// Whether this request is the first today to reach the budget.
    pub crossed: bool,
    /// The bytes counted today, after this request.
    pub total: u64,
}

#[derive(Default)]
struct Entry {
    day: i64,
    bytes: u64,
    reached: bool,
}

/// What one node counted in one UTC day, as it publishes it for the
/// others (section 13.4).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    /// The UTC day, as days since the epoch.
    pub day: i64,
    /// The bytes per account.
    pub bytes: Vec<(Account, u64)>,
}

/// The bytes counted per account in the current day, on this node
/// and, as absorbed, on the others.
#[derive(Default)]
pub struct Ledger {
    entries: Mutex<HashMap<Account, Entry>>,
    /// What the other nodes counted today, summed per account.
    remote: Mutex<HashMap<Account, u64>>,
    /// What each other node last published, by the node's name, and
    /// for how many reads in a row it has been the same.
    published: Mutex<HashMap<String, Published>>,
}

/// What another node published, as last read.
struct Published {
    counts: Counts,
    /// Reads in a row that found these same counts.
    unchanged: u32,
    /// Whether the node is taken to have stopped: its counts are kept
    /// and no longer read.
    settled: bool,
}

/// How many reads in a row must find a node's counts the same, the
/// node gone from the membership view, for the counts to be taken as
/// final: a minute of heartbeats. A node that has only just started
/// is not in the view yet either, and its counts do change.
const SETTLED_AFTER: u32 = 12;

impl Ledger {
    /// An empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Ledger::default()
    }

    /// Counts `bytes` for `account` against `budget` at `now`. A request
    /// that takes the day's total beyond the budget gets the budget's
    /// action, and so does everything after it until the day ends: the
    /// ledger counts what arrives, admitted or not.
    pub fn charge(&self, account: &Account, budget: &Budget, bytes: u64, now: Timestamp) -> Charge {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = entries.entry(account.clone()).or_default();
        let day = now.as_nanos().div_euclid(NANOS_PER_DAY);
        if entry.day != day {
            *entry = Entry {
                day,
                ..Entry::default()
            };
        }
        let after = entry.bytes.saturating_add(bytes);
        let total = after.saturating_add(self.elsewhere(account));
        let beyond = total > budget.bytes_per_day;
        let crossed = beyond && !entry.reached;
        entry.reached |= beyond;
        entry.bytes = after;
        Charge {
            action: beyond.then_some(budget.action),
            crossed,
            total,
        }
    }

    /// What the other nodes counted today for `account`.
    fn elsewhere(&self, account: &Account) -> u64 {
        self.remote
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(account)
            .copied()
            .unwrap_or(0)
    }

    /// What this node counted today, for the other nodes.
    #[must_use]
    pub fn counts(&self, now: Timestamp) -> Counts {
        let day = now.as_nanos().div_euclid(NANOS_PER_DAY);
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut bytes: Vec<(Account, u64)> = entries
            .iter()
            .filter(|(_, entry)| entry.day == day && entry.bytes > 0)
            .map(|(account, entry)| (account.clone(), entry.bytes))
            .collect();
        bytes.sort_by(|a, b| format!("{:?}", a.0).cmp(&format!("{:?}", b.0)));
        Counts { day, bytes }
    }

    /// The counts a node that has stopped published for `day`, when
    /// they were found final ([`Ledger::published`]): a node's id is
    /// new on every start, so what a stopped node published stays for
    /// the day and does not change, and reading it again on every
    /// heartbeat costs the store a request for each node that ran
    /// today.
    #[must_use]
    pub fn settled(&self, node: &str, day: i64) -> Option<Counts> {
        let mut published = self
            .published
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        published.retain(|_, held| held.counts.day == day);
        published
            .get(node)
            .filter(|held| held.settled)
            .map(|held| held.counts.clone())
    }

    /// Notes what `node` was just read to have published and whether
    /// it is in the membership view. Counts found the same
    /// [`SETTLED_AFTER`] times in a row from a node that is not in the
    /// view are final.
    pub fn published(&self, node: &str, counts: &Counts, live: bool) {
        let mut published = self
            .published
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let held = published
            .entry(node.to_owned())
            .or_insert_with(|| Published {
                counts: counts.clone(),
                unchanged: 0,
                settled: false,
            });
        if held.counts == *counts {
            held.unchanged = held.unchanged.saturating_add(1);
        } else {
            held.counts = counts.clone();
            held.unchanged = 1;
        }
        held.settled = !live && held.unchanged >= SETTLED_AFTER;
    }

    /// Takes what the other nodes published as today's counts, replacing
    /// what was absorbed before; counts of another day are dropped.
    pub fn absorb(&self, others: &[Counts], now: Timestamp) {
        let day = now.as_nanos().div_euclid(NANOS_PER_DAY);
        let mut summed: HashMap<Account, u64> = HashMap::new();
        for counts in others.iter().filter(|c| c.day == day) {
            for (account, bytes) in &counts.bytes {
                let slot = summed.entry(account.clone()).or_default();
                *slot = slot.saturating_add(*bytes);
            }
        }
        *self
            .remote
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = summed;
    }

    /// Whether `account` has used up `budget` today, for the check before
    /// a query runs, when its cost is not yet known.
    pub fn exhausted(&self, account: &Account, budget: &Budget, now: Timestamp) -> bool {
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let day = now.as_nanos().div_euclid(NANOS_PER_DAY);
        let here = entries
            .get(account)
            .filter(|entry| entry.day == day)
            .map_or(0, |entry| entry.bytes);
        here.saturating_add(self.elsewhere(account)) >= budget.bytes_per_day
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account() -> Account {
        Account {
            tenant: TenantId::DEFAULT,
            phase: Phase::Ingest,
            team: None,
        }
    }

    fn budget(action: BudgetAction) -> Budget {
        Budget {
            bytes_per_day: 100,
            action,
        }
    }

    const NOON: Timestamp = Timestamp::from_nanos(NANOS_PER_DAY / 2);

    /// The counts of a node that stopped stay for the day and do not
    /// change: once they have been the same for a minute of reads and
    /// the node is gone from the view they are kept, not read again.
    /// A node in the view, or one whose counts still move, is read.
    #[test]
    fn the_counts_of_a_node_that_stopped_are_read_once_they_are_final() {
        let ledger = Ledger::new();
        let counts = |bytes: u64| Counts {
            day: 3,
            bytes: vec![(account(), bytes)],
        };
        for _ in 0..SETTLED_AFTER - 1 {
            ledger.published("gone", &counts(10), false);
        }
        assert_eq!(
            ledger.settled("gone", 3),
            None,
            "not the same for long enough"
        );
        ledger.published("gone", &counts(10), false);
        assert_eq!(ledger.settled("gone", 3), Some(counts(10)));

        for _ in 0..SETTLED_AFTER * 2 {
            ledger.published("live", &counts(10), true);
        }
        assert_eq!(
            ledger.settled("live", 3),
            None,
            "a node in the view goes on"
        );

        for round in 0..SETTLED_AFTER * 2 {
            ledger.published("busy", &counts(u64::from(round)), false);
        }
        assert_eq!(ledger.settled("busy", 3), None, "its counts still move");

        assert_eq!(ledger.settled("gone", 4), None, "another day");
        assert_eq!(ledger.settled("gone", 3), None, "and forgotten with it");
    }

    #[test]
    fn the_request_that_goes_beyond_the_budget_gets_the_action_once() {
        let ledger = Ledger::new();
        let budget = budget(BudgetAction::Alert);
        let first = ledger.charge(&account(), &budget, 60, NOON);
        assert_eq!(first.action, None);
        assert!(!first.crossed);
        let second = ledger.charge(&account(), &budget, 60, NOON);
        assert_eq!(second.action, Some(BudgetAction::Alert));
        assert!(second.crossed, "the first time beyond");
        assert_eq!(second.total, 120);
        let third = ledger.charge(&account(), &budget, 1, NOON);
        assert_eq!(third.action, Some(BudgetAction::Alert));
        assert!(!third.crossed, "already reached today");
    }

    #[test]
    fn once_reached_everything_after_gets_the_action() {
        let ledger = Ledger::new();
        let budget = budget(BudgetAction::Reject);
        ledger.charge(&account(), &budget, 90, NOON);
        let big = ledger.charge(&account(), &budget, 20, NOON);
        assert_eq!(big.action, Some(BudgetAction::Reject));
        assert_eq!(big.total, 110, "what arrived is counted");
        let small = ledger.charge(&account(), &budget, 5, NOON);
        assert_eq!(small.action, Some(BudgetAction::Reject));
        assert!(ledger.exhausted(&account(), &budget, NOON));
    }

    /// Section 13.4: what the other nodes counted today counts here,
    /// and what they counted yesterday does not.
    #[test]
    fn the_other_nodes_counts_are_absorbed_for_today_alone() {
        let ledger = Ledger::new();
        let budget = budget(BudgetAction::Reject);
        ledger.charge(&account(), &budget, 30, NOON);
        let published = ledger.counts(NOON);
        assert_eq!(published.bytes, vec![(account(), 30)]);
        let other = Ledger::new();
        other.absorb(std::slice::from_ref(&published), NOON);
        let charge = other.charge(&account(), &budget, 80, NOON);
        assert_eq!(charge.total, 110, "its own 80 and the 30 from elsewhere");
        assert_eq!(charge.action, Some(BudgetAction::Reject));
        assert!(charge.crossed);
        assert!(other.exhausted(&account(), &budget, NOON));
        let yesterday = Counts {
            day: published.day - 1,
            bytes: vec![(account(), 1_000)],
        };
        other.absorb(&[yesterday], NOON);
        assert!(
            !other.exhausted(&account(), &budget, NOON),
            "replaced, and not today's"
        );
    }

    #[test]
    fn a_new_day_starts_from_nothing() {
        let ledger = Ledger::new();
        let budget = budget(BudgetAction::Reject);
        ledger.charge(&account(), &budget, 100, NOON);
        assert!(ledger.exhausted(&account(), &budget, NOON));
        let tomorrow = Timestamp::from_nanos(NOON.as_nanos() + NANOS_PER_DAY);
        assert!(!ledger.exhausted(&account(), &budget, tomorrow));
        let charge = ledger.charge(&account(), &budget, 10, tomorrow);
        assert_eq!(charge.action, None);
        assert_eq!(charge.total, 10);
    }

    #[test]
    fn accounts_are_counted_apart() {
        let ledger = Ledger::new();
        let budget = budget(BudgetAction::Reject);
        let team = Account {
            team: Some("ads".into()),
            ..account()
        };
        ledger.charge(&account(), &budget, 100, NOON);
        assert!(ledger.exhausted(&account(), &budget, NOON));
        assert!(!ledger.exhausted(&team, &budget, NOON));
        let queries = Account {
            phase: Phase::Query,
            ..account()
        };
        assert!(!ledger.exhausted(&queries, &budget, NOON));
    }
}
