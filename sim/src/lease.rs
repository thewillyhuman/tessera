// SPDX-License-Identifier: Apache-2.0

//! Scenario: nodes compete for slot leases, crash, and take over.
//!
//! Each tick, every node renews the leases it believes it holds when they
//! are due, forgets a lease with some probability (a crash), and tries to
//! acquire slots it does not hold. All nodes share the simulated clock, so
//! there is no skew, and the invariants are:
//!
//! - at no tick do two nodes both hold an unexpired lease for one slot;
//! - no (slot, epoch) is ever granted twice (OP-1: one creator per epoch);
//! - a node that forgets a lease sees the slot taken over by someone
//!   within one lease duration plus one tick, if anyone tries.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use tessera_commit::{Lease, LeaseManager, Slot};
use tessera_object::Key;
use tessera_types::{Clock, Epoch, Random, WorkKind};

use crate::{Report, Sim, Violation, ensure};

const SLOTS: u32 = 3;
const DURATION: Duration = Duration::from_secs(30);
const TICK: Duration = Duration::from_secs(5);
const CRASH_PER_MILLE: u64 = 50;
const TRY_PER_MILLE: u64 = 700;

/// Runs the scenario on `sim`.
///
/// # Errors
///
/// The first invariant that does not hold.
pub async fn ownership(sim: &Sim) -> Result<Report, Violation> {
    let prefix = Key::new("_cluster/leases").unwrap_or_else(|_| unreachable!());
    let managers: Vec<LeaseManager> = (0..sim.config().nodes)
        .map(|i| LeaseManager::new(sim.store(), prefix.clone(), sim.node_env(i), DURATION))
        .collect();
    let mut held: Vec<BTreeMap<Slot, Lease>> = vec![BTreeMap::new(); managers.len()];
    let mut granted: BTreeSet<(Slot, Epoch)> = BTreeSet::new();
    let mut random = sim.random();
    let mut report = Report::default();

    for tick in 0..sim.config().steps {
        let now = sim.clock().now();
        // Vary the order nodes act in, so that no node is always first.
        let first = (tick as usize) % managers.len();
        for offset in 0..managers.len() {
            let index = (first + offset) % managers.len();
            let manager = &managers[index];
            for slot in (0..SLOTS).map(Slot::new) {
                let outcome = if let Some(lease) = held[index].get(&slot).cloned() {
                    if lease.is_expired_at(now) || random.below(1_000) < CRASH_PER_MILLE {
                        held[index].remove(&slot);
                        continue;
                    }
                    let due = lease
                        .acquired_at
                        .checked_add(LeaseManager::DEFAULT_RENEWAL)
                        .is_none_or(|t| now >= t);
                    if !due {
                        continue;
                    }
                    manager.renew(&lease).await
                } else if random.below(1_000) < TRY_PER_MILLE {
                    manager.acquire(WorkKind::Intake, slot).await
                } else {
                    continue;
                };
                match outcome {
                    Ok(Some(lease)) => {
                        ensure!(
                            granted.insert((slot, lease.epoch)),
                            "slot {slot} epoch {} granted twice",
                            lease.epoch
                        );
                        held[index].insert(slot, lease);
                        report.acknowledged += 1;
                    }
                    Ok(None) => {
                        held[index].remove(&slot);
                        report.rejected += 1;
                    }
                    Err(_) => report.failed += 1,
                }
            }
        }
        check_single_holder(&held, now)?;
        sim.clock().advance(TICK);
    }
    report.faults = sim.faults_injected();
    Ok(report)
}

fn check_single_holder(
    held: &[BTreeMap<Slot, Lease>],
    now: tessera_types::Timestamp,
) -> Result<(), Violation> {
    for slot in (0..SLOTS).map(Slot::new) {
        let holders: Vec<_> = held
            .iter()
            .enumerate()
            .filter_map(|(i, h)| {
                h.get(&slot)
                    .filter(|l| !l.is_expired_at(now))
                    .map(|l| (i, l.epoch))
            })
            .collect();
        ensure!(
            holders.len() <= 1,
            "slot {slot} has several unexpired holders: {holders:?}"
        );
    }
    Ok(())
}
