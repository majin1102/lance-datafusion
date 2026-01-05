// SPDX-License-Identifier: Apache-2.0

//! Lance integration with Apache DataFusion.
//!
//! This crate mirrors the module layout of `iceberg-rust`'s DataFusion
//! integration while delegating table IO to Lance datasets.

pub mod catalog;
pub mod dml_extensions;
pub mod error;
pub mod namespace;
pub mod physical_plan;
pub mod schema;
pub mod session_builder;
pub mod table_provider;

pub use catalog::{LanceCatalogProvider, LanceCatalogProviderList};
pub use dml_extensions as dml;
pub use namespace::Namespace;
pub use schema::LanceSchemaProvider;
pub use session_builder::SessionBuilder;
