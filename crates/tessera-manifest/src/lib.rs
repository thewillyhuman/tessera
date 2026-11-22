// SPDX-License-Identifier: Apache-2.0

//! Partitions and their manifests (spec section 6.6), the writer registry,
//! intake (section 6.7) and the consistent view (section 6.8).
//!
//! A partition is one tenant, dataset, kind and event-time window. Its
//! manifest is a commit log holding the set of data files and, per writer,
//! the position up to which the writer's segments are represented by
//! those files.

pub mod checkpoint;
pub mod dataset;
pub mod intake;
pub mod manifest;
pub mod manifests;
pub mod registry;
pub mod view;

pub use dataset::{
    Advice, Alternative, DatasetLog, DatasetLogError, DatasetLogs, DatasetState, LayoutRecord,
    Tombstone,
};
pub use manifest::{
    Manifest, ManifestError, ManifestState, Partial, list_datasets, list_kinds, list_partitions,
    list_windows,
};
pub use manifests::Manifests;
pub use registry::Registration;
pub use view::{Sources, View, ViewSources, read_sources};
