// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Tessera's network front ends (spec section 16): the HTTP
//! administration API, OTLP ingest over HTTP, SQL over HTTP and over
//! the `PostgreSQL` wire protocol, the Loki API with `LogQL`, the
//! Tempo API with `TraceQL` and the Prometheus API with remote write.
//! The other query languages and compatibility APIs arrive with their
//! milestones.
//!
//! This crate is transport only: it decodes requests, calls the catalog,
//! the ingest service and the query engine, and encodes responses. No
//! rule about tenants or data lives here.

pub mod admin;
pub mod encode;
mod error;
pub mod internal;
pub mod loki;
pub mod otlp;
mod params;
pub mod pg;
pub mod prometheus;
pub mod query;
pub mod tempo;

use std::sync::Arc;

use axum::Router;
use tessera_catalog::Catalog;
use tessera_exec::Engine;
use tessera_format::FileFormat;
use tessera_ingest::IngestHandle;
use tessera_object::Pools;
use tessera_types::Env;

pub use error::ApiError;

/// What the handlers share.
#[derive(Clone)]
pub struct AppState {
    /// The cluster catalog.
    pub catalog: Arc<Catalog>,
    /// The ingest service of this node.
    pub ingest: IngestHandle,
    /// The storage pools.
    pub pools: Arc<Pools>,
    /// The data file format.
    pub format: Arc<dyn FileFormat>,
    /// The query engine.
    pub engine: Arc<Engine>,
    /// The node's environment.
    pub env: Env,
}

/// Every HTTP route of a node.
pub fn router(state: AppState) -> Router {
    admin::router(state.clone())
        .merge(internal::router(state.clone()))
        .merge(otlp::router(state.clone()))
        .merge(loki::router(state.clone()))
        .merge(tempo::router(state.clone()))
        .merge(prometheus::router(state.clone()))
        .merge(query::router(state))
}

impl AppState {
    /// A single-node state over an in-memory store with an ingester that
    /// flushes almost at once: for tests, demos and differential harnesses.
    ///
    /// # Panics
    ///
    /// If the in-memory catalog cannot be opened, which cannot happen
    /// without a store failure.
    pub async fn in_memory() -> AppState {
        use std::time::Duration;

        use tessera_catalog::PoolConfig;
        use tessera_exec::{CpuPool, QueryLimits};
        use tessera_format::ParquetFormat;
        use tessera_ingest::{IngestConfig, Ingester};
        use tessera_object::{MemoryStore, PoolState};
        use tessera_types::PoolId;

        let store = MemoryStore::new();
        let env = Env::system();
        let pool = PoolId::new("root").unwrap_or_else(|_| unreachable!("a valid id"));
        let root = PoolConfig {
            id: pool.clone(),
            url: "memory://test".into(),
            weight: 1,
            state: PoolState::Active,
        };
        let catalog = Catalog::open(Arc::new(store.clone()), root, env.clone())
            .await
            .expect("an in-memory catalog opens");
        let config = IngestConfig {
            linger: Duration::from_millis(1),
            ..IngestConfig::default()
        };
        let ingester = Ingester::start(
            Arc::new(store.clone()),
            env.clone(),
            config,
            pool.clone(),
            Arc::new(ParquetFormat),
        );
        let ingest = tessera_ingest::spawn(ingester, Duration::from_millis(1));
        let pools = Arc::new(Pools::new(pool, Arc::new(store)));
        let engine = Engine::new(
            Arc::clone(&pools),
            Arc::new(ParquetFormat),
            env.clone(),
            CpuPool::new(2),
            QueryLimits::default(),
        );
        AppState {
            catalog: Arc::new(catalog),
            ingest,
            pools,
            format: Arc::new(ParquetFormat),
            engine: Arc::new(engine),
            env,
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::AppState;

    /// A single-node state over an in-memory store with a fast ingester.
    pub async fn state() -> AppState {
        AppState::in_memory().await
    }
}
