// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The only way any Tessera crate touches durable storage (spec section
//! 18.2): object keys, the `ObjectStore` trait, its implementations, storage
//! pools and fault injection for simulation.

#[cfg(test)]
pub mod conformance;
pub mod key;
pub mod memory;
pub mod store;

pub use key::{InvalidKey, Key};
pub use memory::MemoryStore;
pub use store::{BoxError, ObjectError, ObjectMeta, ObjectStore};
