// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Admission control (QE-18, IN-11): per-tenant limits on concurrent
//! queries, on how long a query may wait for a turn, and on the bytes
//! of writes in flight, so that one tenant's load is rejected with a
//! retryable answer before it reaches the node's memory and never
//! causes rejections for another tenant.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tessera_types::TenantId;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The limits every tenant gets.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Queries a tenant may run at once.
    pub concurrent_queries: usize,
    /// How long a query may wait for a turn before it is rejected.
    pub queue: Duration,
    /// Bytes of a tenant's writes that may be in flight at once.
    pub ingest_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            concurrent_queries: 8,
            queue: Duration::from_secs(5),
            ingest_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Why a request was not admitted; the client may retry after the
/// delay.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Rejected {
    /// The tenant's queries are all busy and the queue wait ran out.
    #[error("too many concurrent queries for the tenant; retry in {}s", retry_after.as_secs().max(1))]
    QueriesBusy {
        /// How long to wait before retrying.
        retry_after: Duration,
    },
    /// The tenant's writes in flight would exceed the byte limit.
    #[error("too many bytes of writes in flight for the tenant; retry in {}s", retry_after.as_secs().max(1))]
    IngestFull {
        /// How long to wait before retrying.
        retry_after: Duration,
    },
    /// One request is larger than the whole budget.
    #[error("a write of {bytes} bytes exceeds the limit of {limit} bytes")]
    TooLarge {
        /// The request's size.
        bytes: usize,
        /// The limit.
        limit: usize,
    },
}

/// A tenant's budgets.
struct Budgets {
    queries: Arc<Semaphore>,
    ingest: Arc<Semaphore>,
}

/// Admits queries and writes within the limits.
pub struct Admission {
    limits: Limits,
    tenants: Mutex<HashMap<TenantId, Arc<Budgets>>>,
}

/// A query's turn; dropping it frees the slot.
pub struct QueryPermit {
    _permit: OwnedSemaphorePermit,
}

/// A write's bytes in flight; dropping it frees them.
pub struct IngestPermit {
    _permit: OwnedSemaphorePermit,
}

impl Admission {
    /// Admission under `limits`.
    #[must_use]
    pub fn new(limits: Limits) -> Self {
        Admission {
            limits,
            tenants: Mutex::new(HashMap::new()),
        }
    }

    /// The limits.
    #[must_use]
    pub fn limits(&self) -> Limits {
        self.limits
    }

    fn budgets(&self, tenant: &TenantId) -> Arc<Budgets> {
        let mut tenants = self
            .tenants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(tenants.entry(tenant.clone()).or_insert_with(|| {
            Arc::new(Budgets {
                queries: Arc::new(Semaphore::new(self.limits.concurrent_queries.max(1))),
                ingest: Arc::new(Semaphore::new(self.limits.ingest_bytes.max(1))),
            })
        }))
    }

    /// Waits up to the queue limit for a query turn of `tenant`.
    ///
    /// # Errors
    ///
    /// `QueriesBusy` when the wait ran out.
    pub async fn admit_query(&self, tenant: &TenantId) -> Result<QueryPermit, Rejected> {
        let budgets = self.budgets(tenant);
        let acquire = Arc::clone(&budgets.queries).acquire_owned();
        match tokio::time::timeout(self.limits.queue, acquire).await {
            Ok(Ok(permit)) => Ok(QueryPermit { _permit: permit }),
            _ => Err(Rejected::QueriesBusy {
                retry_after: Duration::from_secs(1),
            }),
        }
    }

    /// Reserves `bytes` of `tenant`'s write budget, without waiting:
    /// backpressure must reach the client before memory does (IN-11).
    ///
    /// # Errors
    ///
    /// `TooLarge` for a write beyond the whole budget, `IngestFull` when
    /// the budget is spent.
    pub fn admit_ingest(&self, tenant: &TenantId, bytes: usize) -> Result<IngestPermit, Rejected> {
        if bytes > self.limits.ingest_bytes {
            return Err(Rejected::TooLarge {
                bytes,
                limit: self.limits.ingest_bytes,
            });
        }
        let budgets = self.budgets(tenant);
        let wanted = u32::try_from(bytes.max(1)).unwrap_or(u32::MAX);
        Arc::clone(&budgets.ingest)
            .try_acquire_many_owned(wanted)
            .map(|permit| IngestPermit { _permit: permit })
            .map_err(|_| Rejected::IngestFull {
                retry_after: Duration::from_secs(1),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(name: &str) -> TenantId {
        TenantId::new(name).unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn queries_queue_then_reject_per_tenant() {
        let admission = Admission::new(Limits {
            concurrent_queries: 1,
            queue: Duration::from_millis(100),
            ingest_bytes: 10,
        });
        let first = admission.admit_query(&tenant("a")).await.unwrap();
        let other = admission.admit_query(&tenant("b")).await;
        assert!(other.is_ok(), "another tenant is not affected");
        let waiting = admission.admit_query(&tenant("a")).await;
        assert_eq!(
            waiting.unwrap_err(),
            Rejected::QueriesBusy {
                retry_after: Duration::from_secs(1)
            }
        );
        drop(first);
        assert!(admission.admit_query(&tenant("a")).await.is_ok());
    }

    #[test]
    fn writes_are_bounded_by_bytes_in_flight() {
        let admission = Admission::new(Limits {
            concurrent_queries: 1,
            queue: Duration::from_millis(1),
            ingest_bytes: 10,
        });
        let held = admission.admit_ingest(&tenant("a"), 6).unwrap();
        assert!(matches!(
            admission.admit_ingest(&tenant("a"), 5),
            Err(Rejected::IngestFull { .. })
        ));
        assert!(admission.admit_ingest(&tenant("b"), 10).is_ok(), "per tenant");
        assert!(matches!(
            admission.admit_ingest(&tenant("a"), 11),
            Err(Rejected::TooLarge { bytes: 11, limit: 10 })
        ));
        drop(held);
        assert!(admission.admit_ingest(&tenant("a"), 10).is_ok());
        assert!(admission.limits().ingest_bytes == 10);
    }
}
