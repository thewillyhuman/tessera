// SPDX-License-Identifier: Apache-2.0

//! Positions in Tessera's ordered logs.
//!
//! Each position type formats as a fixed-width, zero-padded decimal so that
//! object keys list in order (spec section 6.2).

use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Defines a `u64` position type with a fixed-width decimal format.
macro_rules! position {
    ($(#[$doc:meta])* $name:ident, width = $width:literal) => {
        $(#[$doc])*
        #[derive(
            Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            /// The position before the first entry.
            pub const ZERO: $name = $name(0);

            /// Wraps a raw position.
            #[must_use]
            pub const fn new(value: u64) -> Self {
                $name(value)
            }

            /// The raw position.
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }

            /// The position after this one.
            ///
            /// # Panics
            ///
            /// If the position is `u64::MAX`, which no log reaches in practice.
            #[must_use]
            pub fn next(self) -> Self {
                $name(self.0.checked_add(1).expect("log position overflow"))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{:0width$}", self.0, width = $width)
            }
        }

        impl FromStr for $name {
            type Err = ParseIntError;

            fn from_str(value: &str) -> Result<Self, ParseIntError> {
                value.parse().map($name)
            }
        }
    };
}

position! {
    /// The version of a commit log: the number of commits it holds
    /// (section 6.5). Keys use twenty digits: `_log/{version:020}`.
    Version, width = 20
}

position! {
    /// The sequence number of an entry in a writer log (ST-5). Keys use
    /// twelve digits: `_wal/{writer_id}/{seq:012}`.
    Seq, width = 12
}

position! {
    /// The epoch of a slot lease (OP-1), one per acquisition or renewal.
    Epoch, width = 20
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_formats_with_twenty_digits() {
        assert_eq!(Version::new(42).to_string(), "00000000000000000042");
    }

    #[test]
    fn seq_formats_with_twelve_digits() {
        assert_eq!(Seq::new(7).to_string(), "000000000007");
    }

    #[test]
    fn epoch_formats_with_twenty_digits() {
        assert_eq!(Epoch::new(3).to_string(), "00000000000000000003");
    }

    #[test]
    fn parses_padded_and_unpadded_positions() {
        assert_eq!(
            "00000000000000000042".parse::<Version>().unwrap(),
            Version::new(42)
        );
        assert_eq!("42".parse::<Version>().unwrap(), Version::new(42));
        assert_eq!("000000000007".parse::<Seq>().unwrap(), Seq::new(7));
    }

    #[test]
    fn rejects_non_numeric_positions() {
        assert!("abc".parse::<Version>().is_err());
        assert!("".parse::<Seq>().is_err());
        assert!("-1".parse::<Epoch>().is_err());
    }

    #[test]
    fn next_increments_by_one() {
        assert_eq!(Version::ZERO.next(), Version::new(1));
        assert_eq!(Seq::new(9).next(), Seq::new(10));
    }

    #[test]
    fn positions_order_numerically() {
        assert!(Version::new(9) < Version::new(10));
    }

    #[test]
    fn positions_serialize_as_numbers() {
        assert_eq!(serde_json::to_string(&Version::new(5)).unwrap(), "5");
        assert_eq!(serde_json::from_str::<Seq>("12").unwrap(), Seq::new(12));
    }
}
