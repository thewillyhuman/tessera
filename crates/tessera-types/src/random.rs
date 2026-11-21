// SPDX-License-Identifier: Apache-2.0

//! The injected random source.
//!
//! Identifiers, placement and jitter all draw from a [`Random`] (RS-2), so
//! that a simulation run is reproducible from its seed.

use std::sync::{Arc, Mutex, PoisonError};

/// A source of random bits.
///
/// Implementations need only [`next_u64`](Self::next_u64); the other
/// methods derive from it so that every implementation agrees on how a
/// bound or a buffer is produced from the raw stream.
pub trait Random: Send {
    /// The next 64 random bits.
    fn next_u64(&mut self) -> u64;

    /// The next 128 random bits, high word first.
    fn next_u128(&mut self) -> u128 {
        (u128::from(self.next_u64()) << 64) | u128::from(self.next_u64())
    }

    /// Fills `buffer` with random bytes.
    fn fill_bytes(&mut self, buffer: &mut [u8]) {
        for chunk in buffer.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }

    /// A value in `0..bound`, close to uniform (the bias is below one part in
    /// `2^64 / bound`, which no caller in this code base can observe).
    ///
    /// # Panics
    ///
    /// If `bound` is zero: there is no value to return, so this is a bug in
    /// the caller.
    fn below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "Random::below called with a zero bound");
        let product = u128::from(self.next_u64()) * u128::from(bound);
        // The high word of the product is below `bound` by construction.
        u64::try_from(product >> 64).expect("product >> 64 fits in u64")
    }
}

/// A deterministic generator (`SplitMix64` [1]) for tests and simulation.
///
/// Not suitable for anything that needs unpredictability.
///
/// [1] <https://prng.di.unimi.it/splitmix64.c>
#[derive(Clone, Debug)]
pub struct SeededRandom {
    state: u64,
}

impl SeededRandom {
    /// A generator whose whole stream is determined by `seed`.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        SeededRandom { state: seed }
    }
}

/// `seed` with a few random edits, as a fuzzer makes them (RS-5): bytes
/// flipped, inserted or removed, a slice repeated, the end cut, and
/// now and then plain random bytes instead. The tests of every parser
/// and decoder feed these to prove that no input panics.
#[must_use]
pub fn mutated(seed: &[u8], random: &mut impl Random) -> Vec<u8> {
    let mut out = seed.to_vec();
    let below = |random: &mut dyn Random, n: usize| {
        usize::try_from(random.next_u64() % u64::try_from(n.max(1)).unwrap_or(1)).unwrap_or(0)
    };
    if random.next_u64().is_multiple_of(8) {
        let len = below(random, 64);
        return (0..len)
            .map(|_| u8::try_from(random.next_u64() % 256).unwrap_or(0))
            .collect();
    }
    let edits = 1 + below(random, 4);
    for _ in 0..edits {
        let byte = u8::try_from(random.next_u64() % 256).unwrap_or(0);
        match random.next_u64() % 5 {
            0 if !out.is_empty() => {
                let at = below(random, out.len());
                out[at] ^= byte | 1;
            }
            1 => {
                let at = below(random, out.len() + 1);
                out.insert(at, byte);
            }
            2 if !out.is_empty() => {
                let at = below(random, out.len());
                out.remove(at);
            }
            3 if !out.is_empty() => {
                let start = below(random, out.len());
                let end = start + below(random, out.len() - start + 1);
                let slice = out[start..end].to_vec();
                let at = below(random, out.len() + 1);
                out.splice(at..at, slice);
            }
            _ => {
                let keep = below(random, out.len() + 1);
                out.truncate(keep);
            }
        }
    }
    out
}

impl Random for SeededRandom {
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// The operating system's random source.
#[derive(Clone, Copy, Debug, Default)]
pub struct OsRandom;

impl Random for OsRandom {
    fn next_u64(&mut self) -> u64 {
        let mut bytes = [0u8; 8];
        getrandom::fill(&mut bytes).expect("operating system random source is unavailable");
        u64::from_le_bytes(bytes)
    }
}

/// A cloneable handle to one random stream shared by several owners.
///
/// Clones draw from the same underlying generator, so a simulation with a
/// single seed stays reproducible however many components it hands the
/// source to.
#[derive(Clone)]
pub struct SharedRandom {
    inner: Arc<Mutex<Box<dyn Random>>>,
}

impl SharedRandom {
    /// Wraps `random` so that it can be shared.
    #[must_use]
    pub fn new(random: impl Random + 'static) -> Self {
        SharedRandom {
            inner: Arc::new(Mutex::new(Box::new(random))),
        }
    }

    /// A generator seeded from this stream that then draws on its own.
    ///
    /// Every draw through a `SharedRandom` takes its lock, and the
    /// production source behind it asks the operating system for each
    /// word; a caller that draws once per row, such as ingest making
    /// record ids, forks once per batch instead and the lock is taken
    /// once. The fork takes [`ForkedRandom::SEED_WORDS`] words from the
    /// stream, so a seeded stream stays reproducible.
    #[must_use]
    pub fn fork(&self) -> ForkedRandom {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let mut state = [0u64; ForkedRandom::SEED_WORDS];
        for word in &mut state {
            *word = inner.next_u64();
        }
        ForkedRandom::from_state(state)
    }
}

impl std::fmt::Debug for SharedRandom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SharedRandom")
    }
}

impl Random for SharedRandom {
    fn next_u64(&mut self) -> u64 {
        // A poisoned lock only means another holder panicked mid-draw; the
        // generator state is still a valid state.
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .next_u64()
    }
}

/// A generator forked from a [`SharedRandom`] for a batch of draws.
///
/// `xoshiro256++` [1]: 256 bits of state, so two forks of the operating
/// system's source never repeat each other's stream in practice, and a
/// permutation of its state, so the 128-bit ids one fork makes do not
/// repeat within it. Not suitable for anything that needs
/// unpredictability.
///
/// [1] <https://prng.di.unimi.it/xoshiro256plusplus.c>
#[derive(Clone, Debug)]
pub struct ForkedRandom {
    state: [u64; ForkedRandom::SEED_WORDS],
}

impl ForkedRandom {
    /// How many words of its source a fork consumes.
    pub const SEED_WORDS: usize = 4;

    /// A generator over `state`; an all-zero state, which would never
    /// leave zero, is replaced by the first words of [`SeededRandom`]'s
    /// stream, as the algorithm's authors seed it.
    fn from_state(mut state: [u64; ForkedRandom::SEED_WORDS]) -> Self {
        if state == [0; ForkedRandom::SEED_WORDS] {
            let mut seed = SeededRandom::new(0);
            for word in &mut state {
                *word = seed.next_u64();
            }
        }
        ForkedRandom { state }
    }
}

impl Random for ForkedRandom {
    fn next_u64(&mut self) -> u64 {
        let s = &mut self.state;
        let result = s[0].wrapping_add(s[3]).rotate_left(23).wrapping_add(s[0]);
        let t = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];
        s[2] ^= t;
        s[3] = s[3].rotate_left(45);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutations_change_the_seed_and_stay_deterministic() {
        let seed = b"SELECT count(*) FROM logs WHERE x = 1";
        let mut random = SeededRandom::new(9);
        let outputs: Vec<Vec<u8>> = (0..200).map(|_| mutated(seed, &mut random)).collect();
        assert!(outputs.iter().any(|o| o != seed), "something changes");
        assert!(outputs.iter().any(|o| o.len() < seed.len()));
        assert!(outputs.iter().any(|o| o.len() > seed.len()));
        let mut again = SeededRandom::new(9);
        let replay: Vec<Vec<u8>> = (0..200).map(|_| mutated(seed, &mut again)).collect();
        assert_eq!(outputs, replay, "a seed replays");
        assert!(mutated(b"", &mut random).len() <= 64, "an empty seed works");
    }

    #[test]
    fn seeded_random_is_reproducible() {
        let mut a = SeededRandom::new(42);
        let mut b = SeededRandom::new(42);
        let draws_a: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let draws_b: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        assert_eq!(draws_a, draws_b);
    }

    #[test]
    fn different_seeds_give_different_streams() {
        assert_ne!(
            SeededRandom::new(1).next_u64(),
            SeededRandom::new(2).next_u64()
        );
    }

    #[test]
    fn below_stays_within_the_bound() {
        let mut random = SeededRandom::new(7);
        assert!((0..1_000).all(|_| random.below(7) < 7));
        assert_eq!(random.below(1), 0);
    }

    #[test]
    fn fills_a_buffer_of_any_length() {
        let mut random = SeededRandom::new(3);
        let mut buffer = [0u8; 13];
        random.fill_bytes(&mut buffer);
        assert!(buffer.iter().any(|&byte| byte != 0));
    }

    #[test]
    fn os_random_does_not_repeat_itself() {
        let mut random = OsRandom;
        assert_ne!(random.next_u64(), random.next_u64());
    }

    #[test]
    fn forks_of_equal_seeds_replay_each_other() {
        let mut a = SharedRandom::new(SeededRandom::new(9)).fork();
        let mut b = SharedRandom::new(SeededRandom::new(9)).fork();
        let draws_a: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let draws_b: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        assert_eq!(draws_a, draws_b);
        assert_ne!(draws_a[0], draws_a[1], "a fork moves on");
    }

    #[test]
    fn a_fork_takes_its_seed_and_nothing_more_from_its_source() {
        let mut reference = SeededRandom::new(9);
        for _ in 0..ForkedRandom::SEED_WORDS {
            reference.next_u64();
        }
        let mut source = SharedRandom::new(SeededRandom::new(9));
        let mut fork = source.fork();
        fork.next_u64();
        fork.next_u64();
        assert_eq!(source.next_u64(), reference.next_u64());
    }

    #[test]
    fn successive_forks_differ() {
        let source = SharedRandom::new(SeededRandom::new(9));
        assert_ne!(source.fork().next_u64(), source.fork().next_u64());
        assert_ne!(
            SharedRandom::new(OsRandom).fork().next_u64(),
            SharedRandom::new(OsRandom).fork().next_u64()
        );
    }

    #[test]
    fn an_all_zero_seed_still_produces_bits() {
        let mut fork = ForkedRandom::from_state([0; ForkedRandom::SEED_WORDS]);
        assert_ne!(fork.next_u64(), fork.next_u64());
    }

    #[test]
    fn shared_random_clones_share_one_stream() {
        let mut reference = SeededRandom::new(9);
        let shared = SharedRandom::new(SeededRandom::new(9));
        let mut clone = shared.clone();
        let mut original = shared;
        assert_eq!(clone.next_u64(), reference.next_u64());
        assert_eq!(original.next_u64(), reference.next_u64());
    }
}
