// SPDX-License-Identifier: Apache-2.0

//! A hash function whose output is frozen for the life of the project.
//!
//! Slot assignment (AR-4), rendezvous hashing (IN-9, QE-15) and series ids
//! all need every node of every version to hash a value to the same
//! number. The standard library's hasher makes no such promise, so this
//! one is defined here and its test vectors must never change.

/// Hashes `bytes` to 64 bits with FNV-1a [1] followed by a mixing step so
/// that the low bits, which slot selection uses, depend on every input
/// byte. The result is the same on every platform and every release.
///
/// [1] <http://www.isthe.com/chongo/tech/comp/fnv/>
#[must_use]
pub fn stable_hash(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut z = hash;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// These vectors are frozen: a change here is a change of on-disk
    /// layout and placement for every deployment.
    #[test]
    fn stable_hash_vectors_never_change() {
        assert_eq!(stable_hash(b""), 0xf52a_15e9_a9b5_e89b);
        assert_eq!(stable_hash(b"a"), 0x02c0_bdbf_4814_20f8);
        assert_eq!(stable_hash(b"tessera"), 0x3c4b_28a1_6b8e_3b43);
    }

    #[test]
    fn low_bits_vary_between_similar_inputs() {
        let slots: std::collections::BTreeSet<u64> = (0..64u32)
            .map(|i| stable_hash(format!("writer-{i}").as_bytes()) & 7)
            .collect();
        assert_eq!(
            slots.len(),
            8,
            "every low-bit value should appear across 64 keys"
        );
    }
}
