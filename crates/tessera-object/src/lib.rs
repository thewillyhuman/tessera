// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The only way any Tessera crate touches durable storage (spec section
//! 18.2): object keys, the `ObjectStore` trait, its implementations, storage
//! pools and fault injection for simulation.

pub mod key;

pub use key::{InvalidKey, Key};
