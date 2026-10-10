// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Scenario: nodes change tenants in the catalog concurrently under faults.
//!
//! Each node performs random tenant operations over a small pool of names.
//! The invariants checked afterwards on a fresh, fault-free handle:
//!
//! - every tenant name is unique;
//! - the built-in tenants exist and are active (CT-4);
//! - every acknowledged create is still present (nothing purges);
//! - every node, after a refresh, sees the same state as the fresh handle.

use std::collections::BTreeSet;
use std::sync::Arc;

use tessera_catalog::{
    Catalog, CatalogError, PoolConfig, TenantName, TenantSettings, TenantStatus,
};
use tessera_object::{FaultPolicy, PoolState};
use tessera_types::{PoolId, Random, TenantId};

use crate::{Report, Sim, Violation, ensure};

const NAMES: [&str; 6] = ["acme", "globex", "initech", "umbrella", "hooli", "stark"];

/// Runs the scenario on `sim`.
///
/// # Errors
///
/// The first invariant that does not hold.
pub async fn tenants(sim: &Sim) -> Result<Report, Violation> {
    let mut catalogs = Vec::new();
    for i in 0..sim.config().nodes {
        catalogs.push(Arc::new(open_with_retries(sim, i).await?));
    }
    let steps = sim.config().steps;
    let mut tasks = Vec::new();
    for catalog in &catalogs {
        let catalog = Arc::clone(catalog);
        let mut random = sim.random();
        tasks.push(tokio::spawn(async move {
            let mut created = BTreeSet::new();
            let mut report = Report::default();
            for _ in 0..steps {
                let name = pick_name(&mut random);
                let other = pick_name(&mut random);
                let result = match random.below(7) {
                    0 | 1 => catalog
                        .create_tenant(name, TenantSettings::default())
                        .await
                        .inspect(|t| {
                            created.insert(t.id.clone());
                        }),
                    2 => catalog.rename_tenant(&name, other).await,
                    3 => catalog.suspend_tenant(&name).await,
                    4 => catalog.resume_tenant(&name).await,
                    5 => catalog.drop_tenant(&name).await,
                    _ => catalog.restore_tenant(&name).await,
                };
                match result {
                    Ok(_) => report.acknowledged += 1,
                    Err(CatalogError::Log(_) | CatalogError::Contention { .. }) => {
                        report.failed += 1;
                    }
                    Err(_) => report.rejected += 1,
                }
                tokio::task::yield_now().await;
            }
            (created, report)
        }));
    }
    let mut created: BTreeSet<TenantId> = BTreeSet::new();
    let mut report = Report::default();
    for task in tasks {
        let (ids, node_report) = task
            .await
            .map_err(|e| Violation(format!("node task panicked: {e}")))?;
        created.extend(ids);
        report.acknowledged += node_report.acknowledged;
        report.rejected += node_report.rejected;
        report.failed += node_report.failed;
    }

    sim.set_faults(FaultPolicy::NONE);
    let truth = Catalog::open(sim.pristine(), root_pool(), sim.node_env(0))
        .await
        .map_err(|e| Violation(format!("opening the catalog fault-free: {e}")))?;
    let snapshot = truth
        .snapshot()
        .await
        .map_err(|e| Violation(format!("reading the catalog: {e}")))?;
    let state = snapshot.state();
    let mut names = BTreeSet::new();
    for tenant in state.tenants() {
        ensure!(
            names.insert(tenant.name.clone()),
            "tenant name {} is used twice",
            tenant.name
        );
    }
    for builtin in [TenantId::DEFAULT, TenantId::SYSTEM] {
        let tenant = state.tenant(&builtin);
        ensure!(
            tenant.is_some_and(|t| t.status == TenantStatus::Active),
            "built-in tenant {builtin} is missing or inactive"
        );
    }
    for id in &created {
        ensure!(
            state.tenant(id).is_some(),
            "acknowledged tenant {id} is missing"
        );
    }
    for catalog in &catalogs {
        let seen = catalog
            .snapshot()
            .await
            .map_err(|e| Violation(format!("node refresh: {e}")))?;
        ensure!(
            *seen == *snapshot,
            "a node's catalog view differs from the log after refresh"
        );
    }
    report.faults = sim.faults_injected();
    Ok(report)
}

/// One of the scenario's names, chosen at random.
fn pick_name(random: &mut impl Random) -> TenantName {
    let index = usize::try_from(random.below(NAMES.len() as u64)).unwrap_or(0);
    TenantName::new(NAMES[index]).unwrap_or_else(|_| unreachable!("scenario names are valid"))
}

fn root_pool() -> PoolConfig {
    PoolConfig {
        id: PoolId::new("root").unwrap_or_else(|_| unreachable!()),
        url: "memory://sim".into(),
        weight: 1,
        state: PoolState::Active,
    }
}

/// Opening goes through the faulty store too; it is retried like a node
/// restarting until its root pool answers.
async fn open_with_retries(sim: &Sim, index: u32) -> Result<Catalog, Violation> {
    let mut last = None;
    for _ in 0..64 {
        match Catalog::open(sim.store(), root_pool(), sim.node_env(index)).await {
            Ok(catalog) => return Ok(catalog),
            Err(error) => last = Some(error),
        }
    }
    Err(Violation(format!(
        "node {index} could not open the catalog in 64 attempts: {last:?}"
    )))
}
