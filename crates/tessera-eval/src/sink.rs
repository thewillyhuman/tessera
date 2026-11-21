// SPDX-License-Identifier: Apache-2.0

//! Where alert events go (CQ-14): each transition is written as an
//! `alert` event of the rule's dataset with a deterministic event id,
//! so that a replay after a takeover is removed by deduplication.

use std::sync::Mutex;

use futures::future::BoxFuture;
use tessera_types::EventBatch;

/// Writes alert events; the node gives the ingest path.
pub trait AlertSink: Send + Sync {
    /// Writes `batch`.
    fn write(&self, batch: EventBatch) -> BoxFuture<'_, Result<(), String>>;
}

/// A sink that keeps what it was given, for tests.
#[derive(Debug, Default)]
pub struct MemorySink {
    batches: Mutex<Vec<EventBatch>>,
}

impl MemorySink {
    /// Everything written so far.
    #[must_use]
    pub fn batches(&self) -> Vec<EventBatch> {
        self.batches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl AlertSink for MemorySink {
    fn write(&self, batch: EventBatch) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.batches
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(batch);
            Ok(())
        })
    }
}
