// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Principals and what they may do (CT-5, SE-1): the API keys of a
//! tenant, the roles that grant reading and writing its datasets and
//! administering it, and the principal a request resolves to. Keys are
//! rows of `system.api_keys` and roles rows of `system.roles`. A key is
//! issued once as `tsk_<tenant>_<id>_<secret>`, which names the tenant
//! whose store holds it; only the SHA-256 of the secret is stored, and
//! the comparison takes the same time whether or not it matches.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tessera_types::{Random, TenantId, Timestamp};

use crate::TenantName;

/// The first part of every key.
pub const PREFIX: &str = "tsk";

/// An API key as stored, keyed by `id`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKey {
    /// The key's id, part of its text.
    pub id: String,
    /// What the key is for, for people.
    pub name: String,
    /// The SHA-256 of the secret, in hex.
    pub secret_hash: String,
    /// The roles the key acts with, by name.
    #[serde(default)]
    pub roles: Vec<String>,
    /// When the key was issued.
    pub created_at: Timestamp,
    /// When the key stops working, if ever.
    #[serde(default)]
    pub expires_at: Option<Timestamp>,
}

impl ApiKey {
    /// Whether `secret` is this key's and the key is live at `now`.
    #[must_use]
    pub fn verifies(&self, secret: &str, now: Timestamp) -> bool {
        let live = self.expires_at.is_none_or(|at| now < at);
        let matches = hash(secret)
            .as_bytes()
            .ct_eq(self.secret_hash.as_bytes())
            .into();
        live && matches
    }
}

/// A role as stored, keyed by `name`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Role {
    /// The role's name, unique in the tenant.
    pub name: String,
    /// The datasets the role may read: names, or `*` for every one.
    #[serde(default)]
    pub read: Vec<String>,
    /// The datasets the role may write: names, or `*` for every one.
    #[serde(default)]
    pub write: Vec<String>,
    /// Whether the role administers the tenant: control statements,
    /// rules, keys and roles.
    #[serde(default)]
    pub admin: bool,
}

impl Role {
    /// The built-in role that may do everything in the tenant.
    pub const ADMIN: &'static str = "admin";
    /// The built-in role that may read every dataset.
    pub const READER: &'static str = "reader";
    /// The built-in role that may write every dataset.
    pub const WRITER: &'static str = "writer";

    /// The built-in roles every tenant has; a stored role of the same
    /// name replaces it.
    #[must_use]
    pub fn builtin(name: &str) -> Option<Role> {
        let every = vec!["*".to_owned()];
        match name {
            Role::ADMIN => Some(Role {
                name: name.into(),
                read: every.clone(),
                write: every,
                admin: true,
            }),
            Role::READER => Some(Role {
                name: name.into(),
                read: every,
                write: Vec::new(),
                admin: false,
            }),
            Role::WRITER => Some(Role {
                name: name.into(),
                read: Vec::new(),
                write: every,
                admin: false,
            }),
            _ => None,
        }
    }
}

/// A key as issued: the text shown once, and the record to store.
#[derive(Clone, Debug)]
pub struct IssuedKey {
    /// `tsk_<tenant>_<id>_<secret>`.
    pub text: String,
    /// What `system.api_keys` keeps.
    pub record: ApiKey,
}

/// Issues a key of `tenant` named `name` acting with `roles`.
pub fn issue(
    tenant: &TenantName,
    name: &str,
    roles: Vec<String>,
    expires_at: Option<Timestamp>,
    now: Timestamp,
    random: &mut impl Random,
) -> IssuedKey {
    let id = format!("{:016x}", random.next_u64());
    let secret = format!("{:032x}", random.next_u128());
    IssuedKey {
        text: format!("{PREFIX}_{tenant}_{id}_{secret}"),
        record: ApiKey {
            id,
            name: name.to_owned(),
            secret_hash: hash(&secret),
            roles,
            created_at: now,
            expires_at,
        },
    }
}

/// The parts of a key's text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyText {
    /// The tenant whose store holds the key.
    pub tenant: TenantName,
    /// The key's id.
    pub id: String,
    /// The secret.
    pub secret: String,
}

/// Reads a key's text, or nothing when it is not one.
#[must_use]
pub fn parse(text: &str) -> Option<KeyText> {
    let rest = text.strip_prefix(PREFIX)?.strip_prefix('_')?;
    let (id_and_secret_start, secret) = rest.rsplit_once('_')?;
    let (tenant, id) = id_and_secret_start.rsplit_once('_')?;
    if id.is_empty() || secret.is_empty() {
        return None;
    }
    Some(KeyText {
        tenant: TenantName::new(tenant).ok()?,
        id: id.to_owned(),
        secret: secret.to_owned(),
    })
}

/// The SHA-256 of `secret`, in hex.
#[must_use]
pub fn hash(secret: &str) -> String {
    use std::fmt::Write;

    let digest = Sha256::digest(secret.as_bytes());
    digest
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

/// Who a request acts as.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    /// The tenant the principal belongs to.
    pub tenant: TenantId,
    /// The key's name, or `anonymous`.
    pub name: String,
    /// The roles the principal acts with.
    pub roles: Vec<Role>,
    /// Whether the principal administers the cluster and may act for
    /// any tenant: the `_system` tenant's admins, and anonymous access
    /// where it is enabled.
    pub cluster_admin: bool,
}

impl Principal {
    /// The principal of a request without credentials, where allowed:
    /// an administrator of the whole cluster.
    #[must_use]
    pub fn anonymous() -> Principal {
        Principal {
            tenant: TenantId::SYSTEM,
            name: "anonymous".into(),
            roles: vec![Role::builtin(Role::ADMIN).unwrap_or_else(|| unreachable!("built in"))],
            cluster_admin: true,
        }
    }

    /// The principal of `key` in `tenant`, with its roles resolved among
    /// `defined` and the built-in ones; a role no one defines grants
    /// nothing.
    #[must_use]
    pub fn of(tenant: TenantId, key: &ApiKey, defined: &[Role]) -> Principal {
        Self::with_roles(tenant, key.name.clone(), &key.roles, defined)
    }

    /// The principal `name` in `tenant` acting with the roles named,
    /// resolved among `defined` and the built-in ones; a role no one
    /// defines grants nothing. How a token's roles claim becomes a
    /// principal (SE-1).
    #[must_use]
    pub fn with_roles(
        tenant: TenantId,
        name: String,
        role_names: &[String],
        defined: &[Role],
    ) -> Principal {
        let roles: Vec<Role> = role_names
            .iter()
            .filter_map(|name| {
                defined
                    .iter()
                    .find(|r| &r.name == name)
                    .cloned()
                    .or_else(|| Role::builtin(name))
            })
            .collect();
        let cluster_admin = tenant == TenantId::SYSTEM && roles.iter().any(|r| r.admin);
        Principal {
            tenant,
            name,
            roles,
            cluster_admin,
        }
    }

    /// Whether the principal may read `dataset` of its tenant.
    #[must_use]
    pub fn may_read(&self, dataset: &str) -> bool {
        self.roles.iter().any(|r| grants(&r.read, dataset))
    }

    /// Whether the principal may write `dataset` of its tenant.
    #[must_use]
    pub fn may_write(&self, dataset: &str) -> bool {
        self.roles.iter().any(|r| grants(&r.write, dataset))
    }

    /// Whether the principal administers its tenant.
    #[must_use]
    pub fn is_admin(&self) -> bool {
        self.roles.iter().any(|r| r.admin)
    }

    /// Whether the principal may act for `tenant`: its own, or any as a
    /// cluster administrator.
    #[must_use]
    pub fn may_act_for(&self, tenant: &TenantId) -> bool {
        self.cluster_admin || &self.tenant == tenant
    }
}

fn grants(datasets: &[String], dataset: &str) -> bool {
    datasets.iter().any(|d| d == "*" || d == dataset)
}

#[cfg(test)]
mod tests {
    use tessera_types::SeededRandom;

    use super::*;

    fn tenant() -> TenantName {
        TenantName::new("acme").unwrap()
    }

    #[test]
    fn an_issued_key_parses_and_verifies_until_it_expires() {
        let mut random = SeededRandom::new(7);
        let issued = issue(
            &tenant(),
            "grafana",
            vec![Role::READER.into()],
            Some(Timestamp::from_nanos(100)),
            Timestamp::from_nanos(1),
            &mut random,
        );
        let parsed = parse(&issued.text).unwrap();
        assert_eq!(parsed.tenant, tenant());
        assert_eq!(parsed.id, issued.record.id);
        assert!(
            issued
                .record
                .verifies(&parsed.secret, Timestamp::from_nanos(50))
        );
        assert!(
            !issued
                .record
                .verifies(&parsed.secret, Timestamp::from_nanos(100)),
            "expired"
        );
        assert!(!issued.record.verifies("wrong", Timestamp::from_nanos(50)));
        assert_ne!(
            issued.record.secret_hash, parsed.secret,
            "only the hash is kept"
        );
        assert_eq!(parse("tsk_acme_abc"), None);
        assert_eq!(parse("Bearer x"), None);
        assert_eq!(parse("tsk_Acme_1_2"), None, "not a tenant name");
    }

    #[test]
    fn principals_act_within_their_roles() {
        let key = ApiKey {
            id: "k".into(),
            name: "ingest".into(),
            secret_hash: hash("s"),
            roles: vec!["payments".into(), Role::WRITER.into(), "ghost".into()],
            created_at: Timestamp::from_nanos(0),
            expires_at: None,
        };
        let payments = Role {
            name: "payments".into(),
            read: vec!["prod".into()],
            write: Vec::new(),
            admin: false,
        };
        let principal = Principal::of(TenantId::DEFAULT, &key, &[payments]);
        assert!(principal.may_read("prod"));
        assert!(!principal.may_read("staging"));
        assert!(principal.may_write("staging"), "writer writes everywhere");
        assert!(!principal.is_admin());
        assert!(principal.may_act_for(&TenantId::DEFAULT));
        assert!(!principal.may_act_for(&TenantId::SYSTEM));
        assert_eq!(principal.roles.len(), 2, "an undefined role grants nothing");
    }

    #[test]
    fn system_admins_and_anonymous_access_administer_the_cluster() {
        let key = ApiKey {
            id: "k".into(),
            name: "ops".into(),
            secret_hash: hash("s"),
            roles: vec![Role::ADMIN.into()],
            created_at: Timestamp::from_nanos(0),
            expires_at: None,
        };
        let ops = Principal::of(TenantId::SYSTEM, &key, &[]);
        assert!(ops.cluster_admin);
        assert!(ops.may_act_for(&TenantId::DEFAULT));
        let tenant_admin = Principal::of(TenantId::DEFAULT, &key, &[]);
        assert!(tenant_admin.is_admin());
        assert!(!tenant_admin.cluster_admin);
        let anonymous = Principal::anonymous();
        assert!(anonymous.cluster_admin && anonymous.is_admin());
        assert!(anonymous.may_read("anything") && anonymous.may_write("anything"));
    }
}
