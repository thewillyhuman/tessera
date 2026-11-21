// SPDX-License-Identifier: Apache-2.0

//! Runs every scenario over many seeds. `TESSERA_SIM_SEEDS` raises the
//! count for a long run.

use tessera_object::FaultPolicy;
use tessera_sim::{Sim, SimConfig, commit_log, run, run_all};

fn seeds() -> u64 {
    std::env::var("TESSERA_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(48)
}

#[test]
fn no_safety_violation_without_faults() {
    for seed in 1..=8 {
        let config = SimConfig {
            faults: FaultPolicy::NONE,
            ..SimConfig::moderate(seed)
        };
        run_all(config).unwrap_or_else(|(scenario, v)| panic!("seed {seed} {scenario}: {v}"));
    }
}

#[test]
fn no_safety_violation_under_moderate_faults() {
    for seed in 1..=seeds() {
        run_all(SimConfig::moderate(seed))
            .unwrap_or_else(|(scenario, v)| panic!("seed {seed} {scenario}: {v}"));
    }
}

#[test]
fn no_safety_violation_under_heavy_faults() {
    let faults = FaultPolicy {
        fail_before: 0.3,
        fail_after: 0.3,
    };
    for seed in 1..=seeds() / 2 {
        let config = SimConfig {
            faults,
            nodes: 3,
            steps: 20,
            seed,
        };
        run_all(config).unwrap_or_else(|(scenario, v)| panic!("seed {seed} {scenario}: {v}"));
    }
}

#[test]
fn faults_are_actually_injected() {
    let report = run(commit_log::contention(&Sim::new(SimConfig::moderate(3)))).unwrap();
    assert!(report.faults > 0, "{report:?}");
    assert!(report.acknowledged > 0, "{report:?}");
}

#[test]
fn a_run_is_reproducible_from_its_seed() {
    let dumps: Vec<_> = (0..2)
        .map(|_| {
            let sim = Sim::new(SimConfig::moderate(11));
            run(commit_log::contention(&sim)).unwrap();
            sim.dump()
        })
        .collect();
    assert_eq!(dumps[0], dumps[1]);
}
