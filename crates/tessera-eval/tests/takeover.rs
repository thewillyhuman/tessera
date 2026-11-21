// SPDX-License-Identifier: Apache-2.0

//! Takeover produces no duplicate transitions (M7): a new owner resumes
//! from the state log, and two owners that evaluate from the same
//! snapshot record each transition once, because the second commit
//! fails by CL-1 and the decision runs again on the newer state.

use std::sync::Arc;

use tessera_eval::{AlertRule, Delivery, Language, MemoryNotifier, MemorySink, RuleGroup, Status};
use tessera_exec::{CpuPool, Engine, QueryLimits};
use tessera_format::ParquetFormat;
use tessera_object::{MemoryStore, Pools};
use tessera_types::{Env, PoolId, TenantId, Timestamp};

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_nanos(seconds * 1_000_000_000)
}

fn engine(store: &Arc<MemoryStore>, env: &Env) -> Engine {
    let pools = Pools::new(PoolId::new("root").unwrap(), store.clone());
    Engine::new(
        Arc::new(pools),
        Arc::new(ParquetFormat::default()),
        env.clone(),
        CpuPool::new(2),
        QueryLimits::default(),
    )
}

/// A group named `g` with one always-firing rule, as one owner sees it.
fn owner(store: &Arc<MemoryStore>, env: &Env) -> RuleGroup {
    RuleGroup::open(
        store.clone(),
        TenantId::DEFAULT,
        "g",
        vec![AlertRule::new("always", Language::Promql, "vector(1)")],
        env.clone(),
    )
}

#[tokio::test]
async fn a_new_owner_resumes_without_repeating_a_transition() {
    let store = Arc::new(MemoryStore::new());
    let env = Env::simulated(1, at(1_000));
    let first = owner(&store, &env);
    let fired = first
        .evaluate(&engine(&store, &env), at(1_000))
        .await
        .unwrap();
    assert_eq!(fired.transitions.len(), 1);
    drop(first);

    // The second owner knows nothing but the state log.
    let second = owner(&store, &Env::simulated(2, at(1_000)));
    let resumed = second
        .evaluate(&engine(&store, &env), at(1_010))
        .await
        .unwrap();
    assert!(resumed.transitions.is_empty(), "already firing");
    let instances = second.instances().await.unwrap();
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].status, Status::Firing);
    assert_eq!(instances[0].transitions, 1);

    let sink = MemorySink::default();
    let notifier = MemoryNotifier::default();
    let delivery = Delivery {
        sink: &sink,
        notifier: &notifier,
        routes: &[],
        silences: &[],
        acknowledgments: &[],
        inhibitions: &[],
        firing: &[],
    };
    let delivered = second.deliver(&delivery, at(1_011)).await.unwrap();
    assert_eq!(
        delivered.events, 1,
        "the first owner's transition, delivered once"
    );
    assert!(second.outbox().await.unwrap().is_empty());
}

#[tokio::test]
async fn two_owners_from_one_snapshot_record_each_transition_once() {
    let store = Arc::new(MemoryStore::new());
    let env = Env::simulated(1, at(1_000));
    let a = owner(&store, &env);
    let b = owner(&store, &Env::simulated(2, at(1_000)));
    // Both read the empty state first, as two nodes that both believe
    // they own the slot would (AR-6).
    assert!(a.instances().await.unwrap().is_empty());
    assert!(b.instances().await.unwrap().is_empty());

    let from_a = a.evaluate(&engine(&store, &env), at(1_000)).await.unwrap();
    let from_b = b.evaluate(&engine(&store, &env), at(1_000)).await.unwrap();
    assert_eq!(from_a.transitions.len(), 1);
    assert!(
        from_b.transitions.is_empty(),
        "b's commit lost the race and its decision ran again on a's state"
    );
    let outbox = a.outbox().await.unwrap();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].number, 1);
    let ids: std::collections::BTreeSet<[u8; 16]> = outbox
        .iter()
        .map(|t| t.event_id(TenantId::DEFAULT.as_str()))
        .collect();
    assert_eq!(ids.len(), 1);
}
