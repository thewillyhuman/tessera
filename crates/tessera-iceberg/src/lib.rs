// SPDX-License-Identifier: Apache-2.0

//! Apache Iceberg metadata over a dataset's adopted Parquet files
//! (OP-9, G2), so that an external engine reads a Tessera table without
//! Tessera.
//!
//! Each tenant, dataset and kind is one Iceberg table whose location is
//! the dataset's prefix in the object store as the engine sees it. An
//! export is a full snapshot: the files every partition manifest holds,
//! listed in one manifest file, named by one manifest list, pointed to
//! by a table metadata file and the version hint. Dedicated columns are
//! typed columns; `_overflow` is the JSON string the files store. Only
//! adopted files are covered, never the tail.
//!
//! The format is Iceberg v3 [1], whose nanosecond timestamp matches the
//! files' event time. The metadata files are Avro object container
//! files written by [`avro`], with the Iceberg field ids an engine maps
//! fields by.
//!
//! [1] <https://iceberg.apache.org/spec/>

pub mod avro;
pub mod export;
pub mod manifest;
pub mod metadata;
pub mod schema;

pub use export::{ExportError, Exporter, Outcome};
