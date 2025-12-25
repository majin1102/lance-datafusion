// SPDX-License-Identifier: Apache-2.0

//! High-level Session wrapper for the Lance DataFusion integration.
//!
//! This crate re-exports the lower-level `lance-integrations-datafusion`
//! types and adds a convenient [`Session`] API that wraps
//! [`datafusion::execution::context::SessionContext`].
//!
//! The [`SessionBuilder`] allows configuring the underlying
//! `SessionContext`, in particular by attaching a root
//! [`lance_namespace::LanceNamespace`] that is exposed as a dynamic
//! catalog hierarchy (catalog / schema / table) using
//! [`LanceCatalogProviderList`].

pub use lance_integrations_datafusion::*;
