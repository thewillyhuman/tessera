// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Tessera's network front ends (spec section 16). Only the HTTP
//! administration API exists yet; the ingest, query and compatibility
//! endpoints arrive with their milestones.
//!
//! This crate is transport only: it decodes requests, calls the catalog
//! and other services, and encodes responses. No rule about tenants or
//! data lives here.

pub mod admin;

pub use admin::{AdminState, router};
