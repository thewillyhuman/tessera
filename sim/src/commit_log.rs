// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Scenario: nodes contend on one commit log under faults.
//!
//! Each node repeatedly commits `node{i}/count = next`, where `next` is one
//! more than its last acknowledged value, retrying the same value after any
//! failure. The invariants checked afterwards:
//!
//! - versions are contiguous from one, and the log has exactly that many
//!   objects (CL-1: one object per version, no gaps);
//! - no two acknowledgements name the same version;
//! - every node's final count is its last acknowledged value, or one more
//!   if its last attempt failed (the write may have landed);
//! - every commit was made by a node of the simulation;
//! - every node, once faults stop, sees the same state as a fresh handle.

use std::collections::BTreeSet;
use std::sync::Arc;

use futures::TryStreamExt;
use tessera_commit::{Action, CommitLog, KvMutation, KvState, Snapshot};
use tessera_object::{FaultPolicy, Key};
use tessera_types::Version;

use crate::{Report, Sim, Violation, ensure};

struct NodeOutcome {
    key: String,
    acknowledged: u32,
    last_failed: bool,
    versions: Vec<Version>,
    failures: u64,
    violation: Option<String>,
}

/// Runs the scenario on `sim`.
///
/// # Errors
///
/// The first invariant that does not hold.
pub async fn contention(sim: &Sim) -> Result<Report, Violation> {
    let prefix = Key::new("sim/log").unwrap_or_else(|_| unreachable!());
    let logs: Vec<Arc<CommitLog<KvState>>> = (0..sim.config().nodes)
        .map(|i| Arc::new(CommitLog::new(sim.store(), prefix.clone(), sim.node_env(i))))
        .collect();
    let steps = sim.config().steps;
    let mut tasks = Vec::new();
    for (index, log) in logs.iter().enumerate() {
        let log = Arc::clone(log);
        tasks.push(tokio::spawn(async move { drive(&log, index, steps).await }));
    }
    let mut outcomes = Vec::new();
    for task in tasks {
        outcomes.push(
            task.await
                .map_err(|e| Violation(format!("node task panicked: {e}")))?,
        );
    }
    for outcome in &outcomes {
        if let Some(violation) = &outcome.violation {
            return Err(Violation(violation.clone()));
        }
    }

    sim.set_faults(FaultPolicy::NONE);
    let reference = CommitLog::<KvState>::new(sim.pristine(), prefix.clone(), sim.node_env(0));
    let truth = reference
        .snapshot()
        .await
        .map_err(|e| Violation(format!("reading the log back: {e}")))?;
    check_versions(sim, &reference, &truth).await?;
    check_acknowledgements(&outcomes, &truth)?;
    check_committers(sim, &reference).await?;
    for log in &logs {
        let seen = log
            .snapshot()
            .await
            .map_err(|e| Violation(format!("node refresh: {e}")))?;
        ensure!(
            *seen == *truth,
            "a node's view differs from the log after refresh"
        );
    }

    Ok(Report {
        acknowledged: outcomes.iter().map(|o| u64::from(o.acknowledged)).sum(),
        rejected: 0,
        failed: outcomes.iter().map(|o| o.failures).sum(),
        faults: sim.faults_injected(),
    })
}

async fn drive(log: &CommitLog<KvState>, index: usize, steps: u32) -> NodeOutcome {
    let key = format!("node{index}/count");
    let mut outcome = NodeOutcome {
        key: key.clone(),
        acknowledged: 0,
        last_failed: false,
        versions: Vec::new(),
        failures: 0,
        violation: None,
    };
    for _ in 0..steps {
        let next = outcome.acknowledged + 1;
        let put = Action::Kv(KvMutation::Put {
            key: key.clone(),
            value: next.to_string(),
        });
        if let Ok(snapshot) = log.update(|_| Ok::<_, ()>(vec![put.clone()])).await {
            if snapshot.state().get(&key) != Some(next.to_string().as_str()) {
                outcome.violation = Some(format!(
                    "{key}: acknowledged {next} but the snapshot disagrees"
                ));
                return outcome;
            }
            outcome.acknowledged = next;
            outcome.last_failed = false;
            outcome.versions.push(snapshot.version());
        } else {
            outcome.failures += 1;
            outcome.last_failed = true;
        }
        tokio::task::yield_now().await;
    }
    outcome
}

async fn check_versions(
    sim: &Sim,
    reference: &CommitLog<KvState>,
    truth: &Snapshot<KvState>,
) -> Result<(), Violation> {
    let commits: Vec<Version> = reference
        .tail(Version::ZERO)
        .map_ok(|c| c.version)
        .try_collect()
        .await
        .map_err(|e| Violation(format!("tailing the log: {e}")))?;
    let expected: Vec<Version> = (1..=truth.version().get()).map(Version::new).collect();
    ensure!(
        commits == expected,
        "versions are not contiguous: {commits:?}"
    );
    let prefix = Key::new("sim/log/_log").unwrap_or_else(|_| unreachable!());
    let listed: BTreeSet<Version> = sim
        .pristine()
        .list(&prefix)
        .map_ok(|m| m.key.name().parse::<Version>().unwrap_or(Version::ZERO))
        .try_collect()
        .await
        .map_err(|e| Violation(format!("listing the log: {e}")))?;
    ensure!(
        listed.len() == expected.len(),
        "the log has {} objects for {} versions",
        listed.len(),
        expected.len()
    );
    Ok(())
}

fn check_acknowledgements(
    outcomes: &[NodeOutcome],
    truth: &Snapshot<KvState>,
) -> Result<(), Violation> {
    let mut seen = BTreeSet::new();
    for outcome in outcomes {
        for version in &outcome.versions {
            ensure!(
                seen.insert(*version),
                "version {version} was acknowledged twice"
            );
        }
        let actual: u32 = truth
            .state()
            .get(&outcome.key)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let allowed = actual == outcome.acknowledged
            || (outcome.last_failed && actual == outcome.acknowledged + 1);
        ensure!(
            allowed,
            "{}: final value {actual}, acknowledged {}, last attempt failed: {}",
            outcome.key,
            outcome.acknowledged,
            outcome.last_failed
        );
    }
    Ok(())
}

async fn check_committers(sim: &Sim, reference: &CommitLog<KvState>) -> Result<(), Violation> {
    let nodes: BTreeSet<_> = sim.node_ids().collect();
    let mut tail = reference.tail(Version::ZERO);
    while let Some(commit) = tail
        .try_next()
        .await
        .map_err(|e| Violation(format!("tailing the log: {e}")))?
    {
        ensure!(
            nodes.contains(&commit.committer),
            "version {} was committed by an unknown node",
            commit.version
        );
        ensure!(
            !commit.actions.is_empty(),
            "version {} carries no actions",
            commit.version
        );
    }
    Ok(())
}
