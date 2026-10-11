// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Encryption in transit (SE-3): the HTTP API and the `PostgreSQL`
//! protocol serve TLS from one certificate and key, and the peer client
//! checks the nodes it reaches against a CA of the operator's choosing.
//! With a client CA, the HTTP listener also verifies the certificates
//! clients present and attaches each to the connection's requests, for
//! the server to authenticate (SE-1).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::http::Request;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tessera_server::mtls::ClientCertificate;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::server::danger::ClientCertVerifier;
use tokio_rustls::rustls::{self, RootCertStore, ServerConfig};
use tower::ServiceExt;

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
    /// A PEM CA certificate client certificates are checked against
    /// (SE-1). Set, a client presenting a certificate it signed is who
    /// the subject says; one presenting none still connects and needs
    /// a key or a token. Unset, no client certificate is asked for.
    pub client_ca: Option<PathBuf>,
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

/// The acceptor serving `tls`'s certificate, asking for client
/// certificates when `tls` names a client CA.
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
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(config_error)?;
    let builder = match &tls.client_ca {
        Some(path) => builder.with_client_cert_verifier(client_verifier(path, provider)?),
        None => builder.with_no_client_auth(),
    };
    let mut config = builder.with_single_cert(certs, key).map_err(config_error)?;
    // gRPC clients negotiate HTTP/2 through ALPN (IN-1).
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// The verifier accepting client certificates signed by the CA at
/// `path`, and clients presenting none (SE-1).
fn client_verifier(
    path: &Path,
    provider: Arc<CryptoProvider>,
) -> Result<Arc<dyn ClientCertVerifier>, TlsError> {
    let pem = |source| TlsError::Pem {
        path: path.to_path_buf(),
        source,
    };
    let config = |source| TlsError::Config {
        path: path.to_path_buf(),
        source,
    };
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(path).map_err(pem)? {
        roots.add(cert.map_err(pem)?).map_err(config)?;
    }
    WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
        .allow_unauthenticated()
        .build()
        .map_err(|e| config(rustls::Error::General(e.to_string())))
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
/// connection is handshaken and served on its own task, its requests
/// carrying the client certificate the handshake verified, if any.
pub(crate) async fn serve_https(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    app: axum::Router,
    mut stopped: oneshot::Receiver<()>,
) {
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
                let app = app.clone();
                tokio::spawn(async move {
                    let tls = match acceptor.accept(stream).await {
                        Ok(tls) => tls,
                        Err(error) => {
                            tracing::debug!(%peer, error = %error, "tls handshake failed");
                            return;
                        }
                    };
                    let certificate = tls
                        .get_ref()
                        .1
                        .peer_certificates()
                        .and_then(|chain| chain.first())
                        .map(|leaf| ClientCertificate::new(leaf.to_vec()));
                    let service = TowerToHyperService::new(app.map_request(attach(certificate)));
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

/// Attaches `certificate`, when the client presented one, to every
/// request of its connection, for the server to authenticate (SE-1).
fn attach<B>(
    certificate: Option<ClientCertificate>,
) -> impl FnMut(Request<B>) -> Request<B> + Clone {
    move |mut request| {
        if let Some(certificate) = &certificate {
            request.extensions_mut().insert(certificate.clone());
        }
        request
    }
}
