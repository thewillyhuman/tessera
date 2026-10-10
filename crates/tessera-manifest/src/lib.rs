// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Partitions and their manifests (spec section 6.6), the writer registry,
//! intake (section 6.7) and the consistent view (section 6.8).
//!
//! A partition is one tenant, dataset, kind and event-time window. Its
//! manifest is a commit log holding the set of data files and, per writer,
//! the position up to which the writer's segments are represented by
//! those files.

pub mod intake;
pub mod manifest;
pub mod registry;

pub use manifest::{Manifest, ManifestError, ManifestState, list_partitions};
pub use registry::Registration;
