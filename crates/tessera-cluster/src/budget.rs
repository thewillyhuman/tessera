// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Counting against budgets (section 13.4): a ledger of the bytes each
//! tenant and team has ingested, and each tenant's queries have
//! scanned, in the current UTC day. Before a write or a query the
//! ledger says whether the budget's action applies; after it, whether
//! this was the first time the budget was reached today, so that the
//! alert is recorded once.
//!
//! The ledger counts what this node admitted since it started. Across a
//! cluster every ingester keeps its own, so a tenant can get up to as
//! many budgets as there are ingesters serving it; seeding the ledger
//! from `system.usage` is a gap.

use std::collections::HashMap;
use std::sync::Mutex;

use tessera_types::{Budget, BudgetAction, TenantId, Timestamp};

const NANOS_PER_DAY: i64 = 86_400 * 1_000_000_000;

/// What a budget limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Phase {
    /// Bytes received.
    Ingest,
    /// Bytes scanned.
    Query,
}

/// Whose usage is counted: a tenant, or one of its teams.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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

/// The bytes counted per account in the current day.
#[derive(Default)]
pub struct Ledger {
    entries: Mutex<HashMap<Account, Entry>>,
}

impl Ledger {
    /// An empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Ledger::default()
    }

    /// Counts `bytes` for `account` against `budget` at `now`. A request
    /// that would take the day's total beyond the budget gets the
    /// budget's action; one that is rejected is not counted.
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
        let beyond = after > budget.bytes_per_day;
        let crossed = beyond && !entry.reached;
        entry.reached |= beyond;
        if !(beyond && budget.action == BudgetAction::Reject) {
            entry.bytes = after;
        }
        Charge {
            action: beyond.then_some(budget.action),
            crossed,
            total: entry.bytes,
        }
    }

    /// Whether `account` has used up `budget` today, for the check before
    /// a query runs, when its cost is not yet known.
    pub fn exhausted(&self, account: &Account, budget: &Budget, now: Timestamp) -> bool {
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let day = now.as_nanos().div_euclid(NANOS_PER_DAY);
        entries
            .get(account)
            .is_some_and(|entry| entry.day == day && entry.bytes >= budget.bytes_per_day)
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
    fn rejected_bytes_are_not_counted() {
        let ledger = Ledger::new();
        let budget = budget(BudgetAction::Reject);
        ledger.charge(&account(), &budget, 90, NOON);
        let big = ledger.charge(&account(), &budget, 20, NOON);
        assert_eq!(big.action, Some(BudgetAction::Reject));
        assert_eq!(big.total, 90);
        let small = ledger.charge(&account(), &budget, 5, NOON);
        assert_eq!(small.action, None, "a smaller request still fits");
        assert_eq!(small.total, 95);
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
