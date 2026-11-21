// SPDX-License-Identifier: Apache-2.0

//! Slots: hash buckets of single-owner work (spec section 5.3).
//!
//! Each kind of work has a slot count *N*, a power of two recorded in the
//! cluster catalog. An item belongs to slot `hash(key) mod N` (AR-4).
//! Doubling *N* moves no data: each old slot splits into two (AR-5), so a
//! node that owns old slot *s* still owns every item it had if it holds
//! new slots *s* and *s + N*.

use std::fmt;

use serde::{Deserialize, Serialize};
use tessera_types::stable_hash;

/// Why a number is not a valid slot count.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("slot count {0} is not a power of two between 1 and 2^31")]
pub struct InvalidSlotCount(u64);

/// The number of slots of one kind of work: a power of two.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct SlotCount(u32);

impl SlotCount {
    /// One slot: everything is owned by one node.
    pub const ONE: SlotCount = SlotCount(1);
    /// The largest count supported.
    pub const MAX: SlotCount = SlotCount(1 << 31);

    /// Validates `count` as a power of two in `1..=2^31`.
    ///
    /// # Errors
    ///
    /// If it is not.
    pub fn new(count: u32) -> Result<Self, InvalidSlotCount> {
        if count.is_power_of_two() && count <= Self::MAX.0 {
            Ok(SlotCount(count))
        } else {
            Err(InvalidSlotCount(u64::from(count)))
        }
    }

    /// The count as a number.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }

    /// Twice this count, or `None` at the maximum.
    #[must_use]
    pub fn doubled(self) -> Option<Self> {
        (self < Self::MAX).then_some(SlotCount(self.0 << 1))
    }

    /// The slot an item belongs to.
    #[must_use]
    pub fn slot_of(self, item: &[u8]) -> Slot {
        // The mask keeps the low bits, which never exceed `u32`.
        #[allow(clippy::cast_possible_truncation)]
        Slot((stable_hash(item) & u64::from(self.0 - 1)) as u32)
    }

    /// Every slot of this count, in order.
    pub fn slots(self) -> impl Iterator<Item = Slot> {
        (0..self.0).map(Slot)
    }
}

impl TryFrom<u32> for SlotCount {
    type Error = InvalidSlotCount;

    fn try_from(count: u32) -> Result<Self, InvalidSlotCount> {
        SlotCount::new(count)
    }
}

impl From<SlotCount> for u32 {
    fn from(count: SlotCount) -> u32 {
        count.0
    }
}

impl fmt::Display for SlotCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One slot of one kind of work. Formats as a decimal number for lease
/// keys.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Slot(u32);

impl Slot {
    /// Wraps a slot index. Callers get slots from [`SlotCount`]; this is for
    /// parsing them back.
    #[must_use]
    pub const fn new(index: u32) -> Self {
        Slot(index)
    }

    /// The slot index.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_powers_of_two_only() {
        assert!(SlotCount::new(1).is_ok());
        assert!(SlotCount::new(64).is_ok());
        assert!(SlotCount::new(0).is_err());
        assert!(SlotCount::new(3).is_err());
        assert!(SlotCount::new(1 << 31).is_ok());
    }

    #[test]
    fn slot_is_within_the_count() {
        let count = SlotCount::new(8).unwrap();
        assert!((0..1_000).all(|i| count.slot_of(format!("item-{i}").as_bytes()).get() < 8));
        assert_eq!(SlotCount::ONE.slot_of(b"anything"), Slot::new(0));
    }

    #[test]
    fn doubling_splits_each_slot_in_two() {
        let before = SlotCount::new(8).unwrap();
        let after = before.doubled().unwrap();
        assert_eq!(after.get(), 16);
        for i in 0..1_000 {
            let item = format!("item-{i}");
            let old = before.slot_of(item.as_bytes()).get();
            let new = after.slot_of(item.as_bytes()).get();
            assert!(new == old || new == old + 8, "{item}: {old} -> {new}");
        }
    }

    #[test]
    fn the_maximum_cannot_be_doubled() {
        assert_eq!(SlotCount::MAX.doubled(), None);
    }

    #[test]
    fn slots_are_spread_across_the_count() {
        let count = SlotCount::new(4).unwrap();
        let used: std::collections::BTreeSet<Slot> = (0..100)
            .map(|i| count.slot_of(format!("w{i}").as_bytes()))
            .collect();
        assert_eq!(used.len(), 4);
    }

    #[test]
    fn slot_count_serializes_as_a_number_and_rejects_invalid_ones() {
        assert_eq!(
            serde_json::to_string(&SlotCount::new(16).unwrap()).unwrap(),
            "16"
        );
        assert!(serde_json::from_str::<SlotCount>("16").is_ok());
        assert!(serde_json::from_str::<SlotCount>("6").is_err());
    }
}
