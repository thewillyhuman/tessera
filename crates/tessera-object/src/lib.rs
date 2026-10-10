// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The only way any Tessera crate touches durable storage (spec section
//! 18.2): object keys, the `ObjectStore` trait, its implementations, storage
//! pools and fault injection for simulation.

#[cfg(test)]
pub mod conformance;
pub mod fault;
pub mod key;
pub mod local;
pub mod memory;
pub mod store;

pub use fault::{FaultPolicy, FaultyStore};
pub use key::{InvalidKey, Key};
pub use local::LocalStore;
pub use memory::MemoryStore;
pub use store::{BoxError, ObjectError, ObjectMeta, ObjectStore};
