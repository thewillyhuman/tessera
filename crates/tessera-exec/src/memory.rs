// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Memory accounting for blocking operators (QE-11, RS-3).
//!
//! A pool has a limit; operators reserve what they buffer and release it
//! when done. A reservation that would exceed the limit fails with
//! [`OutOfMemory`], which the query reports as its own failure (QE-18).
//! Spilling to local disk is the follow-up that turns the failure into
//! slower progress.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A reservation would exceed the pool's limit.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
#[error("query memory limit of {limit} bytes exceeded: {used} in use, {requested} requested")]
pub struct OutOfMemory {
    /// The pool's limit.
    pub limit: usize,
    /// Bytes in use before the request.
    pub used: usize,
    /// The failed request.
    pub requested: usize,
}

/// A bounded pool of memory shared by the operators of one query, or of
/// one tenant.
#[derive(Debug)]
pub struct MemoryPool {
    limit: usize,
    used: AtomicUsize,
}

impl MemoryPool {
    /// A pool of `limit` bytes.
    #[must_use]
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(MemoryPool {
            limit,
            used: AtomicUsize::new(0),
        })
    }

    /// A pool that never refuses, for tests and trusted callers.
    #[must_use]
    pub fn unbounded() -> Arc<Self> {
        Self::new(usize::MAX)
    }

    /// The pool's limit.
    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Bytes currently reserved.
    #[must_use]
    pub fn used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }

    /// Reserves `bytes`.
    ///
    /// # Errors
    ///
    /// `OutOfMemory` if the pool cannot grant them; nothing is reserved.
    pub fn reserve(&self, bytes: usize) -> Result<(), OutOfMemory> {
        let mut used = self.used.load(Ordering::SeqCst);
        loop {
            let next = used.saturating_add(bytes);
            if next > self.limit {
                return Err(OutOfMemory {
                    limit: self.limit,
                    used,
                    requested: bytes,
                });
            }
            match self
                .used
                .compare_exchange(used, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return Ok(()),
                Err(current) => used = current,
            }
        }
    }

    /// Returns `bytes` reserved earlier.
    pub fn release(&self, bytes: usize) {
        // A release never exceeds what was reserved by the same operator;
        // saturate so an accounting slip cannot wrap around.
        let _ = self
            .used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                Some(used.saturating_sub(bytes))
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_are_bounded_by_the_limit() {
        let pool = MemoryPool::new(100);
        pool.reserve(60).unwrap();
        assert_eq!(pool.used(), 60);
        let error = pool.reserve(50).unwrap_err();
        assert_eq!(
            error,
            OutOfMemory {
                limit: 100,
                used: 60,
                requested: 50
            }
        );
        assert_eq!(pool.used(), 60);
        pool.release(60);
        pool.reserve(100).unwrap();
        pool.release(1_000);
        assert_eq!(pool.used(), 0);
    }
}
