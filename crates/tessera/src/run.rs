// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `tessera run`: opens the root pool, the catalog and the HTTP API, keeps
//! the catalog fresh and checkpointed, and stops cleanly on Ctrl-C.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tessera_catalog::{Catalog, PoolConfig};
use tessera_format::{FileFormat, ParquetFormat};
use tessera_ingest::{IngestConfig, Ingester};
use tessera_object::{LocalStore, ObjectStore, PoolState, Pools};
use tessera_server::AppState;
use tessera_types::{Env, PoolId};

/// How often a node re-reads the catalog, so that a change made elsewhere
/// takes effect within the bound of CT-3.
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);
/// How often the catalog is checkpointed, if it changed.
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(60);
/// How often the ingester does its time-based work.
const INGEST_TICK: Duration = Duration::from_millis(50);

/// What `run` needs.
#[derive(Debug)]
pub struct Config {
    /// The directory that acts as the root pool.
    pub data_dir: PathBuf,
    /// Where the HTTP API listens.
    pub listen: SocketAddr,
}

/// Why the node could not start or stopped.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// The data directory cannot be used.
    #[error("cannot open data directory {path}: {source}")]
    DataDir {
        /// The directory.
        path: PathBuf,
        /// The failure.
        #[source]
        source: std::io::Error,
    },
    /// The catalog cannot be opened.
    #[error("cannot open the catalog: {0}")]
    Catalog(#[from] tessera_catalog::CatalogError),
    /// The listener cannot be bound or served.
    #[error("cannot serve the HTTP API on {address}: {source}")]
    Serve {
        /// The address.
        address: SocketAddr,
        /// The failure.
        #[source]
        source: std::io::Error,
    },
}

/// Runs the node until Ctrl-C.
///
/// # Errors
///
/// If the data directory, the catalog or the listener cannot be opened.
pub async fn run(config: Config) -> Result<(), RunError> {
    let env = Env::system();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalStore::open(&config.data_dir).await.map_err(
        |source| RunError::DataDir {
            path: config.data_dir.clone(),
            source,
        },
    )?);
    let pool = PoolId::new("root").unwrap_or_else(|_| unreachable!("a literal valid pool id"));
    let root_pool = PoolConfig {
        id: pool.clone(),
        url: format!(
            "file://{}",
            config
                .data_dir
                .canonicalize()
                .unwrap_or(config.data_dir.clone())
                .display()
        ),
        weight: 1,
        state: PoolState::Active,
    };
    let catalog = Arc::new(Catalog::open(Arc::clone(&store), root_pool, env.clone()).await?);
    tracing::info!(
        node = %env.node,
        data_dir = %config.data_dir.display(),
        version = %catalog.cached().version(),
        "catalog open"
    );
    let keeper = tokio::spawn(keep_fresh(Arc::clone(&catalog)));

    let format: Arc<dyn FileFormat> = Arc::new(ParquetFormat);
    let pools = Arc::new(Pools::new(pool.clone(), Arc::clone(&store)));
    let ingester = Ingester::start(
        Arc::clone(&store),
        env.clone(),
        IngestConfig::default(),
        pool,
        Arc::clone(&format),
    );
    tracing::info!(writer = %ingester.writer_id(), "ingest started");
    let ingest = tessera_ingest::spawn(ingester, INGEST_TICK);

    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(|source| RunError::Serve {
            address: config.listen,
            source,
        })?;
    tracing::info!(address = %config.listen, "http api listening");
    let app = tessera_server::router(AppState {
        catalog: Arc::clone(&catalog),
        ingest: ingest.clone(),
        pools,
        format,
        env,
    });
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|source| RunError::Serve {
            address: config.listen,
            source,
        })?;

    keeper.abort();
    if let Err(error) = ingest.shutdown().await {
        tracing::warn!(error = %error, "ingest shutdown left work for recovery");
    }
    tracing::info!("stopped");
    Ok(())
}

/// Re-reads the catalog every second and checkpoints it every minute if
/// it changed. Failures are logged and retried on the next tick.
async fn keep_fresh(catalog: Arc<Catalog>) {
    let mut checkpointed = catalog.cached().version();
    let mut last_checkpoint = tokio::time::Instant::now();
    let mut ticks = tokio::time::interval(REFRESH_INTERVAL);
    loop {
        ticks.tick().await;
        let version = match catalog.snapshot().await {
            Ok(snapshot) => snapshot.version(),
            Err(error) => {
                tracing::warn!(error = %error, "catalog refresh failed");
                continue;
            }
        };
        if version > checkpointed && last_checkpoint.elapsed() >= CHECKPOINT_INTERVAL {
            match catalog.checkpoint().await {
                Ok(()) => {
                    tracing::debug!(%version, "catalog checkpointed");
                    checkpointed = version;
                }
                Err(error) => tracing::warn!(error = %error, "catalog checkpoint failed"),
            }
            last_checkpoint = tokio::time::Instant::now();
        }
    }
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(error = %error, "cannot listen for ctrl-c; running until killed");
        std::future::pending::<()>().await;
    }
    tracing::info!("shutdown requested");
}
