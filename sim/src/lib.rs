// SPDX-License-Identifier: Apache-2.0

//! Deterministic simulation of Tessera's coordination primitives (RS-2,
//! section 18.5).
//!
//! A [`Sim`] is built from a seed: every node id, every random draw, every
//! injected fault and the clock follow from it, so a violation found at a
//! seed can be replayed. Nodes run as tasks on a single-threaded runtime
//! with paused time, against one in-memory object store wrapped in fault
//! injection. Each scenario drives the nodes through random operations
//! and then checks its safety invariants against the pristine store.

pub mod catalog;
pub mod commit_log;
pub mod lease;
pub mod write_path;

use std::future::Future;
use std::sync::Arc;

use tessera_object::{FaultPolicy, FaultyStore, MemoryStore, ObjectStore};
use tessera_types::{Clock, Env, ManualClock, NodeId, SeededRandom, SharedRandom, Timestamp};

/// How a simulation is set up.
#[derive(Clone, Copy, Debug)]
pub struct SimConfig {
    /// Determines everything else.
    pub seed: u64,
    /// How many nodes take part.
    pub nodes: u32,
    /// How many operations each node attempts.
    pub steps: u32,
    /// How often the object store fails.
    pub faults: FaultPolicy,
}

impl SimConfig {
    /// A moderate configuration for a given seed: four nodes, thirty steps,
    /// one store call in ten failing before and one in ten after.
    #[must_use]
    pub fn moderate(seed: u64) -> Self {
        SimConfig {
            seed,
            nodes: 4,
            steps: 30,
            faults: FaultPolicy {
                fail_before: 0.1,
                fail_after: 0.1,
            },
        }
    }
}

/// One simulated deployment.
pub struct Sim {
    config: SimConfig,
    pristine: MemoryStore,
    faulty: Arc<FaultyStore>,
    clock: Arc<ManualClock>,
    random: SharedRandom,
}

impl Sim {
    /// A deployment set up from `config`.
    #[must_use]
    pub fn new(config: SimConfig) -> Self {
        let pristine = MemoryStore::new();
        let random = SharedRandom::new(SeededRandom::new(config.seed));
        let faulty = FaultyStore::new(Arc::new(pristine.clone()), random.clone(), config.faults);
        let clock = Arc::new(ManualClock::new(Timestamp::from_nanos(
            1_700_000_000_000_000_000,
        )));
        Sim {
            config,
            pristine,
            faulty: Arc::new(faulty),
            clock,
            random,
        }
    }

    /// The configuration.
    #[must_use]
    pub fn config(&self) -> &SimConfig {
        &self.config
    }

    /// The environment of node `index`: a fixed id, the shared clock and the
    /// shared random stream.
    #[must_use]
    pub fn node_env(&self, index: u32) -> Env {
        let node = NodeId::from_u128(u128::from(index) + 1);
        Env::new(
            node,
            Arc::clone(&self.clock) as Arc<dyn Clock>,
            self.random.clone(),
        )
    }

    /// The ids of all nodes.
    pub fn node_ids(&self) -> impl Iterator<Item = NodeId> {
        (0..self.config.nodes).map(|i| NodeId::from_u128(u128::from(i) + 1))
    }

    /// The store nodes use: faults injected.
    #[must_use]
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.faulty) as Arc<dyn ObjectStore>
    }

    /// The store without faults, for checking invariants.
    #[must_use]
    pub fn pristine(&self) -> Arc<dyn ObjectStore> {
        Arc::new(self.pristine.clone())
    }

    /// Changes the fault policy for all later store calls.
    pub fn set_faults(&self, faults: FaultPolicy) {
        self.faulty.set_policy(faults);
    }

    /// How many faults have been injected.
    #[must_use]
    pub fn faults_injected(&self) -> u64 {
        self.faulty.injected()
    }

    /// The simulated clock.
    #[must_use]
    pub fn clock(&self) -> &ManualClock {
        &self.clock
    }

    /// The shared random stream, for scenario decisions.
    #[must_use]
    pub fn random(&self) -> SharedRandom {
        self.random.clone()
    }

    /// Every object in the pristine store, for comparing runs.
    #[must_use]
    pub fn dump(&self) -> Vec<(String, Vec<u8>)> {
        let store = self.pristine.clone();
        run(async move {
            use futures::TryStreamExt;
            let mut objects = Vec::new();
            let roots = ["_cluster", "sim", "_wal", "default"];
            for root in roots {
                let prefix = tessera_object::Key::new(root).unwrap_or_else(|_| unreachable!());
                let listed: Vec<_> = store.list(&prefix).try_collect().await.unwrap_or_default();
                for meta in listed {
                    if let Ok(body) = store.get(&meta.key).await {
                        objects.push((meta.key.to_string(), body.to_vec()));
                    }
                }
            }
            objects
        })
    }
}

/// A safety invariant that did not hold.
#[derive(Debug, thiserror::Error)]
#[error("safety violation: {0}")]
pub struct Violation(pub String);

/// Fails with a violation unless `condition` holds.
#[macro_export]
macro_rules! ensure {
    ($condition:expr, $($message:tt)+) => {
        if !$condition {
            return Err($crate::Violation(format!($($message)+)));
        }
    };
}

/// What a scenario did, for reporting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Report {
    /// Operations that were acknowledged.
    pub acknowledged: u64,
    /// Operations the component rejected on purpose (for example, a
    /// duplicate name).
    pub rejected: u64,
    /// Operations that failed because of the store.
    pub failed: u64,
    /// Faults the store injected.
    pub faults: u64,
}

/// Runs `future` to completion on a single-threaded runtime whose timers
/// advance only when every task is idle, so sleeps cost no wall time and
/// scheduling is reproducible.
///
/// # Panics
///
/// If the runtime cannot be built, which only happens when the process is
/// out of resources.
pub fn run<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("a current-thread runtime always builds")
        .block_on(future)
}

/// Runs every scenario for one configuration.
///
/// # Errors
///
/// The first violation found, naming the scenario.
pub fn run_all(
    config: SimConfig,
) -> Result<Vec<(&'static str, Report)>, (&'static str, Violation)> {
    let mut reports = Vec::new();
    let commit_log =
        run(commit_log::contention(&Sim::new(config))).map_err(|v| ("commit-log", v))?;
    reports.push(("commit-log", commit_log));
    let lease = run(lease::ownership(&Sim::new(config))).map_err(|v| ("lease", v))?;
    reports.push(("lease", lease));
    let catalog = run(catalog::tenants(&Sim::new(config))).map_err(|v| ("catalog", v))?;
    reports.push(("catalog", catalog));
    let write_path =
        run(write_path::write_path(&Sim::new(config))).map_err(|v| ("write-path", v))?;
    reports.push(("write-path", write_path));
    Ok(reports)
}
