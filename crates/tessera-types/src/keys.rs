// SPDX-License-Identifier: Apache-2.0

//! Per-tenant encryption keys (SE-2, SE-3). A node is given a keyring
//! of named 256-bit keys when it starts; a tenant's `encryption_key`
//! setting names the one its data is written under. Whatever is sealed
//! carries the name of the key that sealed it, so a reader finds the
//! key by name, and a tenant moved to a new key keeps reading what the
//! old one sealed for as long as the old key stays in the keyring.
//! Sealing is AES-256-GCM under a fresh random nonce and associated
//! data that binds the bytes to their place.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};

use crate::TenantId;

/// Bytes in a key.
pub const KEY_LEN: usize = 32;
/// Bytes in a nonce, written before the sealed bytes.
const NONCE_LEN: usize = 12;
/// Bytes in the authentication tag, written after them.
const TAG_LEN: usize = 16;

/// A 256-bit key. Its bytes are never printed.
#[derive(Clone, PartialEq, Eq)]
pub struct TenantKey(Arc<[u8; KEY_LEN]>);

impl fmt::Debug for TenantKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TenantKey(..)")
    }
}

/// Why a key or a keyring could not be read.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidKey {
    /// The key is not 64 hexadecimal digits.
    #[error("a key is 64 hexadecimal digits, not {0}")]
    NotHex(String),
    /// A keyring line is not `name = hex`.
    #[error("line {line}: expected `name = <64 hex digits>`")]
    Line {
        /// The line number, from one.
        line: usize,
    },
}

/// Why sealed bytes could not be opened, or bytes could not be sealed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    /// The named key is not in this node's keyring.
    #[error("key {name} is not in this node's keyring")]
    Missing {
        /// The key's name.
        name: String,
    },
    /// The bytes were not sealed by this key under this associated
    /// data, or were altered.
    #[error("sealed bytes do not open with key {name}")]
    Sealed {
        /// The key's name.
        name: String,
    },
    /// This node does not know the tenant yet, so it cannot say
    /// whether the tenant's data is to be sealed.
    #[error("tenant {tenant} is not known to this node yet")]
    UnknownTenant {
        /// The tenant.
        tenant: TenantId,
    },
}

impl TenantKey {
    /// The key with these bytes.
    #[must_use]
    pub fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        TenantKey(Arc::new(bytes))
    }

    /// The key written as 64 hexadecimal digits.
    ///
    /// # Errors
    ///
    /// `NotHex` for anything else.
    pub fn from_hex(text: &str) -> Result<Self, InvalidKey> {
        let text = text.trim();
        let digits = text.as_bytes();
        if digits.len() != KEY_LEN * 2 || !digits.iter().all(u8::is_ascii_hexdigit) {
            return Err(InvalidKey::NotHex(text.to_owned()));
        }
        let mut bytes = [0u8; KEY_LEN];
        for (byte, pair) in bytes.iter_mut().zip(digits.chunks(2)) {
            let hex = std::str::from_utf8(pair).unwrap_or_else(|_| unreachable!("ascii"));
            *byte = u8::from_str_radix(hex, 16).unwrap_or_else(|_| unreachable!("hex digits"));
        }
        Ok(TenantKey::from_bytes(bytes))
    }

    /// The key's bytes, for a cipher that takes them whole.
    #[must_use]
    pub fn bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }

    fn cipher(&self) -> LessSafeKey {
        LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, self.0.as_ref())
                .unwrap_or_else(|_| unreachable!("a 32-byte key")),
        )
    }

    /// `plain` sealed under `aad`: the nonce, the ciphertext and the
    /// tag, which only this key opens, and only under the same `aad`.
    ///
    /// # Panics
    ///
    /// If the operating system's random source is unavailable, as
    /// [`crate::OsRandom`] does.
    #[must_use]
    pub fn seal(&self, aad: &[u8], plain: &[u8]) -> Vec<u8> {
        let mut nonce = [0u8; NONCE_LEN];
        getrandom::fill(&mut nonce).expect("operating system random source is unavailable");
        let mut sealed = Vec::with_capacity(NONCE_LEN + plain.len() + TAG_LEN);
        sealed.extend_from_slice(&nonce);
        sealed.extend_from_slice(plain);
        let mut body = sealed.split_off(NONCE_LEN);
        self.cipher()
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(aad),
                &mut body,
            )
            .unwrap_or_else(|_| unreachable!("sealing cannot fail for a sound key"));
        sealed.extend_from_slice(&body);
        sealed
    }

    /// The bytes `seal` produced, opened under the same `aad`; `None`
    /// when they were sealed by another key, under other data, or
    /// altered since.
    #[must_use]
    pub fn open(&self, aad: &[u8], sealed: &[u8]) -> Option<Vec<u8>> {
        if sealed.len() < NONCE_LEN + TAG_LEN {
            return None;
        }
        let (nonce, body) = sealed.split_at(NONCE_LEN);
        let nonce: [u8; NONCE_LEN] = nonce.try_into().ok()?;
        let mut body = body.to_vec();
        let plain = self
            .cipher()
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(aad),
                &mut body,
            )
            .ok()?;
        Some(plain.to_vec())
    }
}

/// Named keys, as a node is given them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Keyring {
    keys: BTreeMap<String, TenantKey>,
}

impl Keyring {
    /// The keyring a file holds: one `name = <64 hex digits>` per line,
    /// blank lines and `#` comments ignored. A later line for the same
    /// name wins.
    ///
    /// # Errors
    ///
    /// `Line` for a line that is not `name = value`; `NotHex` for a
    /// value that is not a key.
    pub fn parse(text: &str) -> Result<Self, InvalidKey> {
        let mut keys = BTreeMap::new();
        for (index, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (name, hex) = line
                .split_once('=')
                .map(|(name, hex)| (name.trim(), hex.trim()))
                .filter(|(name, _)| !name.is_empty())
                .ok_or(InvalidKey::Line { line: index + 1 })?;
            keys.insert(name.to_owned(), TenantKey::from_hex(hex)?);
        }
        Ok(Keyring { keys })
    }

    /// This keyring with `key` under `name`.
    #[must_use]
    pub fn with(mut self, name: impl Into<String>, key: TenantKey) -> Self {
        self.keys.insert(name.into(), key);
        self
    }

    /// The key named `name`.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&TenantKey> {
        self.keys.get(name)
    }

    /// The names held, in order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.keys.keys().map(String::as_str)
    }

    /// Whether no key is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// Which key, by name, a tenant's data is written under now: the
/// tenant's `encryption_key` setting (CT-7), answered by the catalog.
pub trait KeyPolicy: Send + Sync {
    /// The name of `tenant`'s key, or `None` for plain storage.
    fn key_name(&self, tenant: &TenantId) -> Option<String>;

    /// Whether the policy knows `tenant` at all. A node hears of a new
    /// tenant a moment after it is created; until then "no key" is not
    /// an answer it can give, and nothing of the tenant's may be
    /// written. A policy with no notion of tenants knows them all.
    fn knows(&self, _tenant: &TenantId) -> bool {
        true
    }
}

/// The policy under which no tenant has a key.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoKeys;

impl KeyPolicy for NoKeys {
    fn key_name(&self, _: &TenantId) -> Option<String> {
        None
    }
}

/// A key and its name: what a writer seals with, the name recorded
/// beside the sealed bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedKey {
    /// The name, as the keyring holds it.
    pub name: String,
    /// The key.
    pub key: TenantKey,
}

/// A node's keys: the keyring it was given and the policy naming the
/// tenants' keys. Writers ask [`Keys::for_tenant`]; readers open by the
/// recorded name.
pub struct Keys {
    ring: Keyring,
    policy: Arc<dyn KeyPolicy>,
}

impl fmt::Debug for Keys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Keys")
            .field("names", &self.ring.names().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl Default for Keys {
    fn default() -> Self {
        Keys::none()
    }
}

impl Keys {
    /// No keys: every tenant's data is stored plain.
    #[must_use]
    pub fn none() -> Self {
        Keys::new(Keyring::default(), Arc::new(NoKeys))
    }

    /// The keys in `ring`, named for tenants by `policy`.
    #[must_use]
    pub fn new(ring: Keyring, policy: Arc<dyn KeyPolicy>) -> Self {
        Keys { ring, policy }
    }

    /// The keyring.
    #[must_use]
    pub fn ring(&self) -> &Keyring {
        &self.ring
    }

    /// The key `tenant`'s data is written under now, or `None` for
    /// plain storage.
    ///
    /// # Errors
    ///
    /// `Missing` when the policy names a key the keyring lacks, and
    /// `UnknownTenant` when the policy does not know the tenant: the
    /// write must fail rather than store plain what was to be sealed.
    pub fn for_tenant(&self, tenant: &TenantId) -> Result<Option<NamedKey>, KeyError> {
        let Some(name) = self.policy.key_name(tenant) else {
            if self.policy.knows(tenant) {
                return Ok(None);
            }
            return Err(KeyError::UnknownTenant {
                tenant: tenant.clone(),
            });
        };
        let key = self
            .ring
            .get(&name)
            .cloned()
            .ok_or_else(|| KeyError::Missing { name: name.clone() })?;
        Ok(Some(NamedKey { name, key }))
    }

    /// `sealed`, as the key named `name` sealed it under `aad`.
    ///
    /// # Errors
    ///
    /// `Missing` without that key; `Sealed` when the bytes do not open.
    pub fn open(&self, name: &str, aad: &[u8], sealed: &[u8]) -> Result<Vec<u8>, KeyError> {
        let key = self.ring.get(name).ok_or_else(|| KeyError::Missing {
            name: name.to_owned(),
        })?;
        key.open(aad, sealed).ok_or_else(|| KeyError::Sealed {
            name: name.to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEX: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    struct Fixed(&'static str);

    impl KeyPolicy for Fixed {
        fn key_name(&self, _: &TenantId) -> Option<String> {
            Some(self.0.to_owned())
        }
    }

    #[test]
    fn a_keyring_is_named_hex_keys_one_per_line() {
        let ring = Keyring::parse(&format!(
            "# the keys\n\nk2024 = {HEX}\nk2025={}\n",
            HEX.to_uppercase()
        ))
        .unwrap();
        assert_eq!(ring.names().collect::<Vec<_>>(), ["k2024", "k2025"]);
        assert_eq!(ring.get("k2024"), ring.get("k2025"), "case does not matter");
    }

    #[test]
    fn refuses_lines_that_are_not_a_named_key() {
        assert_eq!(
            Keyring::parse("k2024\n").unwrap_err(),
            InvalidKey::Line { line: 1 }
        );
        assert_eq!(
            Keyring::parse("= abc\n").unwrap_err(),
            InvalidKey::Line { line: 1 }
        );
        assert!(matches!(
            Keyring::parse(&format!("k = {}", &HEX[..60])).unwrap_err(),
            InvalidKey::NotHex(_)
        ));
    }

    #[test]
    fn sealed_bytes_open_with_the_same_key_under_the_same_data() {
        let key = TenantKey::from_hex(HEX).unwrap();
        let sealed = key.seal(b"block 7", b"hello");
        assert_ne!(&sealed[NONCE_LEN..NONCE_LEN + 5], b"hello");
        assert_eq!(key.open(b"block 7", &sealed).unwrap(), b"hello");
        assert_ne!(
            key.seal(b"block 7", b"hello"),
            sealed,
            "a fresh nonce every time"
        );
    }

    #[test]
    fn sealed_bytes_do_not_open_under_other_data_another_key_or_altered() {
        let key = TenantKey::from_hex(HEX).unwrap();
        let other = TenantKey::from_bytes([7; KEY_LEN]);
        let sealed = key.seal(b"block 7", b"hello");
        assert_eq!(key.open(b"block 8", &sealed), None);
        assert_eq!(other.open(b"block 7", &sealed), None);
        let mut altered = sealed.clone();
        altered[NONCE_LEN] ^= 1;
        assert_eq!(key.open(b"block 7", &altered), None);
        assert_eq!(key.open(b"block 7", &sealed[..NONCE_LEN]), None);
    }

    #[test]
    fn keys_follow_the_policy_and_the_ring() {
        let ring = Keyring::default().with("k2025", TenantKey::from_hex(HEX).unwrap());
        let keys = Keys::new(ring.clone(), Arc::new(Fixed("k2025")));
        let named = keys.for_tenant(&TenantId::DEFAULT).unwrap().unwrap();
        assert_eq!(named.name, "k2025");
        let sealed = named.key.seal(b"aad", b"rows");
        assert_eq!(keys.open("k2025", b"aad", &sealed).unwrap(), b"rows");
        assert_eq!(
            keys.open("k2024", b"aad", &sealed).unwrap_err(),
            KeyError::Missing {
                name: "k2024".into()
            }
        );
        assert_eq!(
            keys.open("k2025", b"other", &sealed).unwrap_err(),
            KeyError::Sealed {
                name: "k2025".into()
            }
        );
        let missing = Keys::new(ring, Arc::new(Fixed("k2026")));
        assert_eq!(
            missing.for_tenant(&TenantId::DEFAULT).unwrap_err(),
            KeyError::Missing {
                name: "k2026".into()
            }
        );
        assert_eq!(Keys::none().for_tenant(&TenantId::DEFAULT).unwrap(), None);
    }

    /// A node learns of a tenant a moment after it is created. Until
    /// then it cannot say whether the tenant has a key, and "none" is
    /// the one answer that would store plain what was to be sealed.
    #[test]
    fn a_tenant_the_policy_does_not_know_is_not_taken_for_one_without_a_key() {
        struct Stranger;

        impl KeyPolicy for Stranger {
            fn key_name(&self, _: &TenantId) -> Option<String> {
                None
            }

            fn knows(&self, _: &TenantId) -> bool {
                false
            }
        }

        let keys = Keys::new(Keyring::default(), Arc::new(Stranger));
        assert_eq!(
            keys.for_tenant(&TenantId::DEFAULT).unwrap_err(),
            KeyError::UnknownTenant {
                tenant: TenantId::DEFAULT
            }
        );
    }
}
