// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Tessera's network front ends (spec section 16): the HTTP
//! administration API, OTLP ingest over HTTP and a plain read of a
//! dataset. The query languages and the compatibility APIs arrive with
//! their milestones.
//!
//! This crate is transport only: it decodes requests, calls the catalog,
//! the ingest service and the consistent view, and encodes responses. No
//! rule about tenants or data lives here.

pub mod admin;
mod error;
pub mod otlp;
pub mod read;

use std::sync::Arc;

use axum::Router;
use tessera_catalog::Catalog;
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
    /// The node's environment.
    pub env: Env,
}

/// Every HTTP route of a node.
pub fn router(state: AppState) -> Router {
    admin::router(state.clone())
        .merge(otlp::router(state.clone()))
        .merge(read::router(state))
}

#[cfg(test)]
pub(crate) mod testing {
    use std::sync::Arc;
    use std::time::Duration;

    use tessera_catalog::{Catalog, PoolConfig};
    use tessera_format::ParquetFormat;
    use tessera_ingest::{IngestConfig, Ingester};
    use tessera_object::{MemoryStore, PoolState, Pools};
    use tessera_types::{Env, PoolId};

    use super::AppState;

    /// A single-node state over an in-memory store with a fast ingester.
    pub async fn state() -> AppState {
        let store = MemoryStore::new();
        let env = Env::system();
        let pool = PoolId::new("root").unwrap();
        let root = PoolConfig {
            id: pool.clone(),
            url: "memory://test".into(),
            weight: 1,
            state: PoolState::Active,
        };
        let catalog = Catalog::open(Arc::new(store.clone()), root, env.clone())
            .await
            .unwrap();
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
        AppState {
            catalog: Arc::new(catalog),
            ingest,
            pools: Arc::new(Pools::new(pool, Arc::new(store))),
            format: Arc::new(ParquetFormat),
            env,
        }
    }
}
