// SPDX-License-Identifier: Apache-2.0

//! `tessera-sim`: runs the simulation scenarios over a range of seeds and
//! reports the first safety violation, with the seed that reproduces it.

use clap::Parser;
use tessera_object::FaultPolicy;
use tessera_sim::{SimConfig, run_all};

/// Runs Tessera's deterministic simulation over many seeds.
#[derive(Parser, Debug)]
#[command(name = "tessera-sim", version)]
struct Args {
    /// First seed to run.
    #[arg(long, default_value_t = 1)]
    start_seed: u64,
    /// How many seeds to run.
    #[arg(long, default_value_t = 100)]
    seeds: u64,
    /// Nodes per simulation.
    #[arg(long, default_value_t = 4)]
    nodes: u32,
    /// Operations per node.
    #[arg(long, default_value_t = 30)]
    steps: u32,
    /// Probability that a store call fails before taking effect.
    #[arg(long, default_value_t = 0.1)]
    fail_before: f64,
    /// Probability that a store write takes effect but reports failure.
    #[arg(long, default_value_t = 0.1)]
    fail_after: f64,
}

fn main() {
    let args = Args::parse();
    let faults = FaultPolicy {
        fail_before: args.fail_before,
        fail_after: args.fail_after,
    };
    let mut totals = std::collections::BTreeMap::new();
    for seed in args.start_seed..args.start_seed + args.seeds {
        let config = SimConfig {
            seed,
            nodes: args.nodes,
            steps: args.steps,
            faults,
        };
        match run_all(config) {
            Ok(reports) => {
                for (scenario, report) in reports {
                    let entry = totals.entry(scenario).or_insert((0u64, 0u64, 0u64, 0u64));
                    entry.0 += report.acknowledged;
                    entry.1 += report.rejected;
                    entry.2 += report.failed;
                    entry.3 += report.faults;
                }
            }
            Err((scenario, violation)) => {
                eprintln!("seed {seed}, scenario {scenario}: {violation}");
                eprintln!(
                    "reproduce with: tessera-sim --start-seed {seed} --seeds 1 --nodes {} --steps {} --fail-before {} --fail-after {}",
                    args.nodes, args.steps, args.fail_before, args.fail_after
                );
                std::process::exit(1);
            }
        }
    }
    println!(
        "{} seeds from {}: no safety violation",
        args.seeds, args.start_seed
    );
    println!(
        "{:<12} {:>12} {:>12} {:>12} {:>12}",
        "scenario", "acknowledged", "rejected", "failed", "faults"
    );
    for (scenario, (acknowledged, rejected, failed, faults)) in totals {
        println!("{scenario:<12} {acknowledged:>12} {rejected:>12} {failed:>12} {faults:>12}");
    }
}
