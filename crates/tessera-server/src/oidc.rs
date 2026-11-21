// SPDX-License-Identifier: Apache-2.0

//! Authentication by `OpenID` Connect (SE-1): a bearer token that is not
//! an API key is a JWT, checked against the issuer's signing keys,
//! audience and expiry, and mapped by a configured claim to the tenant
//! it acts for and by another to the roles it acts with. The issuer's
//! keys are read from its discovery document and kept for a while; an
//! unknown key id reads them again, so a rotated key works at once.

use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::ApiError;
use crate::auth::Identity;

/// How long the issuer's keys are kept before they are read again.
const KEYS_TTL: Duration = Duration::from_secs(600);

/// How tokens are checked and read.
#[derive(Clone, Debug)]
pub struct Oidc {
    /// The issuer, as its tokens name it in `iss` and as the discovery
    /// document is reached under: `{issuer}/.well-known/openid-configuration`.
    pub issuer: String,
    /// The audience a token must carry.
    pub audience: String,
    /// The claim naming the tenant the token acts for.
    pub tenant_claim: String,
    /// The claim listing the roles the token acts with.
    pub roles_claim: String,
}

/// Checks tokens against an issuer.
pub struct Verifier {
    config: Oidc,
    agent: ureq::Agent,
    keys: Mutex<Option<(Instant, JwkSet)>>,
}

#[derive(Deserialize)]
struct Discovery {
    jwks_uri: String,
}

impl Verifier {
    /// A verifier for `config`.
    #[must_use]
    pub fn new(config: Oidc) -> Self {
        Verifier {
            config,
            agent: ureq::Agent::config_builder()
                .timeout_global(Some(Duration::from_secs(10)))
                .build()
                .into(),
            keys: Mutex::new(None),
        }
    }

    /// The configuration.
    #[must_use]
    pub fn config(&self) -> &Oidc {
        &self.config
    }

    /// The identity `token` proves. Blocking: it may read the issuer's
    /// keys over the network.
    ///
    /// # Errors
    ///
    /// 401 for a token that does not verify or lacks the claims; 503 when
    /// the issuer's keys cannot be read.
    pub fn verify(&self, token: &str) -> Result<Identity, ApiError> {
        let bad = |reason: String| ApiError::unauthorized(format!("invalid token: {reason}"));
        let header = decode_header(token).map_err(|e| bad(e.to_string()))?;
        let kid = header.kid.ok_or_else(|| bad("no key id".into()))?;
        let jwk = match self.key(&kid, false)? {
            Some(jwk) => jwk,
            None => self
                .key(&kid, true)?
                .ok_or_else(|| bad(format!("unknown key {kid}")))?,
        };
        let decoding = DecodingKey::from_jwk(&jwk).map_err(|e| bad(e.to_string()))?;
        let algorithm = jwk
            .common
            .key_algorithm
            .and_then(|a| a.to_string().parse::<Algorithm>().ok())
            .unwrap_or(header.alg);
        let mut validation = Validation::new(algorithm);
        validation.set_issuer(&[&self.config.issuer]);
        validation.set_audience(&[&self.config.audience]);
        let claims = decode::<Map<String, Value>>(token, &decoding, &validation)
            .map_err(|e| bad(e.to_string()))?
            .claims;
        let text = |claim: &str| {
            claims
                .get(claim)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| bad(format!("no {claim} claim")))
        };
        let roles = match claims.get(&self.config.roles_claim) {
            Some(Value::Array(items)) => items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
            Some(Value::String(one)) => vec![one.clone()],
            _ => Vec::new(),
        };
        Ok(Identity {
            subject: text("sub")?,
            tenant: text(&self.config.tenant_claim)?,
            roles,
        })
    }

    /// The issuer's key `kid`, from the kept set or, with `refresh` or
    /// no set kept recently, from the issuer.
    fn key(&self, kid: &str, refresh: bool) -> Result<Option<jsonwebtoken::jwk::Jwk>, ApiError> {
        let mut kept = self.keys.lock().unwrap_or_else(PoisonError::into_inner);
        let stale = kept.as_ref().is_none_or(|(at, _)| at.elapsed() > KEYS_TTL);
        if refresh || stale {
            let set = self.fetch()?;
            *kept = Some((Instant::now(), set));
        }
        Ok(kept.as_ref().and_then(|(_, set)| set.find(kid)).cloned())
    }

    fn fetch(&self) -> Result<JwkSet, ApiError> {
        let unavailable = |e: String| ApiError::unavailable(format!("identity provider: {e}"));
        let discovery_url = format!(
            "{}/.well-known/openid-configuration",
            self.config.issuer.trim_end_matches('/')
        );
        let discovery: Discovery = serde_json::from_str(&self.text(&discovery_url)?)
            .map_err(|e| unavailable(e.to_string()))?;
        serde_json::from_str(&self.text(&discovery.jwks_uri)?)
            .map_err(|e| unavailable(e.to_string()))
    }

    /// The body of `url`, read whole.
    fn text(&self, url: &str) -> Result<String, ApiError> {
        let unavailable = |e: String| ApiError::unavailable(format!("identity provider: {e}"));
        self.agent
            .get(url)
            .call()
            .map_err(|e| unavailable(e.to_string()))?
            .body_mut()
            .read_to_string()
            .map_err(|e| unavailable(e.to_string()))
    }
}
