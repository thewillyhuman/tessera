// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Authentication by client certificate (SE-1). The listener checks a
//! certificate against the client CA the operator configured and hands
//! the leaf to each request of the connection as a
//! [`ClientCertificate`]; its subject says who the client is: the
//! organization (`O`) names the tenant, each organizational unit (`OU`)
//! a role, and the common name (`CN`) the principal. A request that
//! also carries an `Authorization` header is what the header says.

use std::sync::Arc;

use x509_certificate::X509Certificate;
use x509_certificate::rfc3280::AttributeTypeAndValue;

use crate::ApiError;
use crate::auth::Identity;

/// The DER leaf certificate a client presented and the listener
/// verified: a request extension.
#[derive(Clone, Debug)]
pub struct ClientCertificate(Arc<[u8]>);

impl ClientCertificate {
    /// The verified certificate `der` encodes.
    #[must_use]
    pub fn new(der: Vec<u8>) -> Self {
        ClientCertificate(Arc::from(der))
    }

    /// The DER bytes.
    #[must_use]
    pub fn der(&self) -> &[u8] {
        &self.0
    }

    /// The identity the subject asserts.
    ///
    /// # Errors
    ///
    /// 401 when the certificate does not parse or its subject has no
    /// organization or no common name.
    pub fn identity(&self) -> Result<Identity, ApiError> {
        let bad = |reason: String| ApiError::unauthorized(format!("client certificate: {reason}"));
        let certificate = X509Certificate::from_der(&self.0).map_err(|e| bad(e.to_string()))?;
        let subject = certificate.subject_name();
        let text = |attribute: &AttributeTypeAndValue| attribute.to_string().ok();
        let tenant = subject
            .iter_organization()
            .find_map(text)
            .ok_or_else(|| bad("the subject names no organization (O), the tenant".into()))?;
        let name = subject
            .iter_common_name()
            .find_map(text)
            .ok_or_else(|| bad("the subject has no common name (CN)".into()))?;
        let roles = subject
            .iter_organizational_unit()
            .filter_map(text)
            .collect();
        Ok(Identity {
            subject: name,
            tenant,
            roles,
        })
    }
}

#[cfg(test)]
mod tests {
    use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};

    use super::*;

    fn certificate(subject: DistinguishedName) -> ClientCertificate {
        let mut params = CertificateParams::default();
        params.distinguished_name = subject;
        let key = KeyPair::generate().unwrap();
        ClientCertificate::new(params.self_signed(&key).unwrap().der().to_vec())
    }

    #[test]
    fn the_subject_names_the_tenant_a_role_and_who() {
        let mut subject = DistinguishedName::new();
        subject.push(DnType::OrganizationName, "acme");
        subject.push(DnType::OrganizationalUnitName, "reader");
        subject.push(DnType::CommonName, "grafana");
        let identity = certificate(subject).identity().unwrap();
        assert_eq!(
            identity,
            Identity {
                subject: "grafana".into(),
                tenant: "acme".into(),
                roles: vec!["reader".into()],
            }
        );
    }

    #[test]
    fn a_subject_without_an_organization_is_refused() {
        let mut subject = DistinguishedName::new();
        subject.push(DnType::CommonName, "grafana");
        let error = certificate(subject).identity().unwrap_err();
        assert_eq!(error.status().as_u16(), 401);
        assert!(
            error.message().contains("organization"),
            "{}",
            error.message()
        );
    }

    #[test]
    fn bytes_that_are_no_certificate_are_refused() {
        let error = ClientCertificate::new(vec![0x30, 0x03, 0x02, 0x01])
            .identity()
            .unwrap_err();
        assert_eq!(error.status().as_u16(), 401);
    }
}
