// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The injected environment of a node: its identity, clock, random
//! source (RS-2) and tenant keys (SE-2), bundled so that components take
//! one value instead of four.

use std::sync::Arc;

use crate::{
    Clock, Keys, ManualClock, NodeId, OsRandom, SeededRandom, SharedRandom, SystemClock, Timestamp,
};

/// What a component needs from the node it runs on.
///
/// Cloning is cheap: the clock is shared and the random source is a
/// [`SharedRandom`], so clones draw from one stream.
#[derive(Clone)]
pub struct Env {
    /// The identity of this node process.
    pub node: NodeId,
    /// The clock every component reads time from.
    pub clock: Arc<dyn Clock>,
    /// The random source every component draws from.
    pub random: SharedRandom,
    /// The tenant keys data is sealed with and opened by (SE-2); none
    /// unless [`Env::with_keys`] gave some.
    pub keys: Arc<Keys>,
}

impl std::fmt::Debug for Env {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Env")
            .field("node", &self.node)
            .field("keys", &self.keys)
            .finish_non_exhaustive()
    }
}

impl Env {
    /// An environment from explicit parts, without tenant keys.
    #[must_use]
    pub fn new(node: NodeId, clock: Arc<dyn Clock>, random: SharedRandom) -> Self {
        Env {
            node,
            clock,
            random,
            keys: Arc::new(Keys::none()),
        }
    }

    /// This environment with `keys` as the tenant keys.
    #[must_use]
    pub fn with_keys(mut self, keys: Keys) -> Self {
        self.keys = Arc::new(keys);
        self
    }

    /// The production environment: a random node id, the system clock and
    /// the operating system's random source.
    #[must_use]
    pub fn system() -> Self {
        let mut random = OsRandom;
        Env::new(
            NodeId::random(&mut random),
            Arc::new(SystemClock),
            SharedRandom::new(random),
        )
    }

    /// A deterministic environment for tests and simulation: everything,
    /// including the node id, follows from `seed`; the clock starts at
    /// `start` and moves only through [`Env::manual_clock`].
    #[must_use]
    pub fn simulated(seed: u64, start: Timestamp) -> Self {
        let mut random = SeededRandom::new(seed);
        let node = NodeId::random(&mut random);
        Env::new(
            node,
            Arc::new(ManualClock::new(start)),
            SharedRandom::new(random),
        )
    }

    /// The same environment seen as another node, sharing the clock and
    /// random stream. Simulations use it to create their nodes.
    #[must_use]
    pub fn as_node(&self, node: NodeId) -> Self {
        Env {
            node,
            clock: Arc::clone(&self.clock),
            random: self.random.clone(),
            keys: Arc::clone(&self.keys),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ManualClock, Random, SeededRandom};

    #[test]
    fn simulated_environment_is_deterministic() {
        let mut a = Env::simulated(7, Timestamp::UNIX_EPOCH);
        let mut b = Env::simulated(7, Timestamp::UNIX_EPOCH);
        assert_eq!(a.node, b.node);
        assert_eq!(a.random.next_u64(), b.random.next_u64());
        assert_eq!(a.clock.now(), Timestamp::UNIX_EPOCH);
    }

    #[test]
    fn custom_environment_uses_the_given_parts() {
        let clock = Arc::new(ManualClock::new(Timestamp::from_nanos(5)));
        let env = Env::new(
            NodeId::from_u128(1),
            clock,
            SharedRandom::new(SeededRandom::new(1)),
        );
        assert_eq!(env.clock.now(), Timestamp::from_nanos(5));
        assert_eq!(env.node, NodeId::from_u128(1));
    }
}
