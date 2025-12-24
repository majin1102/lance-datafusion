// SPDX-License-Identifier: Apache-2.0
// DataFusion CatalogProvider integration for Lance using lance-namespace.

pub mod catalog;
pub mod catalog_list;
pub mod dml;
pub mod register;
pub mod schema;
pub mod session;
pub mod url_table;

pub use catalog::LanceCatalogProvider;
pub use catalog_list::LanceCatalogProviderList;
pub use dml::execute_lance_sql;
pub use schema::LanceSchemaProvider;
pub use session::{NamespaceBackend, NamespaceConfig, Session};
pub use url_table::SessionContextExt;
pub use url_table::SessionContextExt as LanceUrlTableExt;
