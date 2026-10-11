// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Encryption in transit (SE-3): the HTTP API and the `PostgreSQL`
//! protocol serve TLS from one certificate and key, and the peer client
//! checks the nodes it reaches against a CA of the operator's choosing.

use std::path::PathBuf;
use std::sync::Arc;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{self, ServerConfig};

/// Where the node's certificate, key and the cluster's CA are.
#[derive(Clone, Debug)]
pub struct Tls {
    /// The PEM certificate chain served, leaf first.
    pub cert: PathBuf,
    /// The PEM private key of the leaf certificate.
    pub key: PathBuf,
    /// A PEM CA certificate the peers' certificates are checked
    /// against; the system's roots otherwise.
    pub ca: Option<PathBuf>,
}

/// Why TLS could not be set up.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// A PEM file cannot be read or holds no usable object.
    #[error("cannot read {path}: {source}")]
    Pem {
        /// The file.
        path: PathBuf,
        /// The failure.
        source: rustls_pki_types::pem::Error,
    },
    /// The CA file cannot be read.
    #[error("cannot read {path}: {source}")]
    Ca {
        /// The file.
        path: PathBuf,
        /// The failure.
        source: std::io::Error,
    },
    /// The certificate and key do not make a server configuration.
    #[error("{path}: {source}")]
    Config {
        /// The certificate file.
        path: PathBuf,
        /// The failure.
        source: rustls::Error,
    },
}

/// The acceptor serving `tls`'s certificate.
///
/// # Errors
///
/// If a file cannot be read or the certificate and key do not match.
pub fn acceptor(tls: &Tls) -> Result<TlsAcceptor, TlsError> {
    let pem = |path: &PathBuf, source| TlsError::Pem {
        path: path.clone(),
        source,
    };
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&tls.cert)
        .map_err(|source| pem(&tls.cert, source))?
        .collect::<Result<_, _>>()
        .map_err(|source| pem(&tls.cert, source))?;
    let key = PrivateKeyDer::from_pem_file(&tls.key).map_err(|source| pem(&tls.key, source))?;
    let config_error = |source| TlsError::Config {
        path: tls.cert.clone(),
        source,
    };
    let config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(config_error)?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(config_error)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// The CA certificate as PEM bytes, when `tls` names one.
///
/// # Errors
///
/// If the file cannot be read.
pub fn ca_pem(tls: &Tls) -> Result<Option<Vec<u8>>, TlsError> {
    tls.ca
        .as_ref()
        .map(|path| {
            std::fs::read(path).map_err(|source| TlsError::Ca {
                path: path.clone(),
                source,
            })
        })
        .transpose()
}

/// Serves `app` over TLS on `listener` until `stopped` resolves; each
/// connection is handshaken and served on its own task.
pub(crate) async fn serve_https(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    app: axum::Router,
    mut stopped: oneshot::Receiver<()>,
) {
    let service = TowerToHyperService::new(app);
    loop {
        tokio::select! {
            _ = &mut stopped => break,
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        tracing::warn!(error = %error, "https accept failed");
                        continue;
                    }
                };
                let acceptor = acceptor.clone();
                let service = service.clone();
                tokio::spawn(async move {
                    let tls = match acceptor.accept(stream).await {
                        Ok(tls) => tls,
                        Err(error) => {
                            tracing::debug!(%peer, error = %error, "tls handshake failed");
                            return;
                        }
                    };
                    let served = Builder::new(TokioExecutor::new())
                        .serve_connection_with_upgrades(TokioIo::new(tls), service)
                        .await;
                    if let Err(error) = served {
                        tracing::debug!(%peer, error = %error, "https connection ended");
                    }
                });
            }
        }
    }
}
