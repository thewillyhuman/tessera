// SPDX-License-Identifier: Apache-2.0

//! The tenants a node has begun to write to (CT-6).
//!
//! A tenant nothing is written to has nothing for the background work
//! to do, and going over every tenant to find that out is a request
//! per tenant however idle the cluster. The nodes that compact leave
//! such a tenant alone, and are told when that changes: a node that
//! registers a writer with a dataset, which it does before the first
//! row it stores there, names the tenant in its heartbeats for a
//! while, and the nodes that read them go over the tenant at once.
//!
//! It is a hint like the rest of membership (P2): a node that dies
//! before its next heartbeat, or one that misses a minute of them,
//! leaves the tenant to the round that goes over every tenant, an
//! hour later at most. Its rows are readable all the while.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use tessera_types::{TenantId, Timestamp};

/// How long a node names a tenant it began to write to: a dozen
/// heartbeats at the default interval, so that a node that misses a
/// few still reads one.
pub const TOLD_FOR: Duration = Duration::from_secs(60);

/// The tenants this node began to write to, and when.
#[derive(Debug, Default)]
pub struct Woken {
    since: Mutex<BTreeMap<TenantId, Timestamp>>,
}

impl Woken {
    /// Notes that this node began to write to `tenant` at `now`.
    pub fn note(&self, tenant: &TenantId, now: Timestamp) {
        self.since().insert(tenant.clone(), now);
    }

    /// The tenants noted within [`TOLD_FOR`] of `now`, in order; the
    /// ones noted before are forgotten.
    #[must_use]
    pub fn lately(&self, now: Timestamp) -> Vec<TenantId> {
        let mut since = self.since();
        since.retain(|_, at| now.duration_since(*at).is_none_or(|age| age < TOLD_FOR));
        since.keys().cloned().collect()
    }

    fn since(&self) -> std::sync::MutexGuard<'_, BTreeMap<TenantId, Timestamp>> {
        self.since
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tenant_is_named_for_a_minute_after_it_was_begun() {
        let at = |seconds: i64| Timestamp::from_nanos(seconds * 1_000_000_000);
        let (acme, globex) = (
            TenantId::new("acme").unwrap(),
            TenantId::new("globex").unwrap(),
        );
        let woken = Woken::default();
        assert!(woken.lately(at(0)).is_empty());
        woken.note(&globex, at(0));
        woken.note(&acme, at(30));
        assert_eq!(woken.lately(at(59)), [acme.clone(), globex.clone()]);
        assert_eq!(woken.lately(at(60)), std::slice::from_ref(&acme));
        // Begun again, it is named again.
        woken.note(&globex, at(61));
        assert_eq!(woken.lately(at(89)), [acme, globex.clone()]);
        assert_eq!(woken.lately(at(90)), [globex]);
        assert!(woken.lately(at(121)).is_empty());
    }
}
