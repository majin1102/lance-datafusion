// SPDX-License-Identifier: Apache-2.0

//! Facade crate for the Lance DataFusion integration.
//!
//! This crate simply re-exports the lower-level
//! `lance-integrations-datafusion` crate so existing code can continue
//! to depend on the `lance-datafusion-catalog` crate name without
//! changes.
//!
//! New code is encouraged to use the types in `lance-integrations-datafusion`
//! directly, such as [`SessionBuilder`] and the dynamic catalog / schema
//! providers.

pub use lance_integrations_datafusion::*;
