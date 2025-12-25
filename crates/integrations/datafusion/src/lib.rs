// SPDX-License-Identifier: Apache-2.0

//! Lance integration with Apache DataFusion.
//!
//! This crate mirrors the module layout of `iceberg-rust`'s DataFusion
//! integration while delegating table IO to Lance datasets.

pub mod catalog;
pub mod dml;
pub mod error;
pub mod physical_plan;
pub mod schema;
pub mod session_builder;
pub mod table;
pub mod task_writer;

pub use catalog::{LanceCatalogProvider, LanceCatalogProviderList};
pub use schema::LanceSchemaProvider;
pub use session_builder::SessionBuilder;
pub use table::lance_static_table_provider::LanceStaticTableProvider;
pub use table::lance_table_provider::LanceTableProvider;
