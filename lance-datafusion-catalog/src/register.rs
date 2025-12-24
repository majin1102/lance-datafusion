// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)]

use std::sync::Arc;

use datafusion::catalog::CatalogProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SessionContext;
use lance_namespace::LanceNamespace;
use lance_namespace_impls::DirectoryNamespaceBuilder;

use crate::{LanceCatalogProvider, LanceCatalogProviderList};

/// Register a [`LanceCatalogProviderList`] as the active catalog list for the
/// given session.
///
/// This replaces DataFusion's default in-memory catalog list.
pub(crate) fn register_lance_catalog_provider_list(
    ctx: &SessionContext,
    list: Arc<LanceCatalogProviderList>,
) {
    ctx.register_catalog_list(list);
}

/// Register a [`LanceCatalogProvider`] under the given catalog name.
pub(crate) fn register_lance_catalog(
    ctx: &SessionContext,
    catalog_name: &str,
    catalog: Arc<LanceCatalogProvider>,
) -> Option<Arc<dyn CatalogProvider>> {
    ctx.register_catalog(catalog_name.to_string(), catalog)
}

/// Convenience helper: create a directory-based Lance namespace for `root`,
/// expose it as `catalog_name.schema_name` in the provided [`SessionContext`],
/// and register it using a [`LanceCatalogProvider`].
///
/// This is the easiest way to get started when the dataset layout is
/// `root/<table>.lance`.
pub(crate) async fn register_directory_namespace_as_schema(
    ctx: &SessionContext,
    catalog_name: &str,
    schema_name: &str,
    root: &str,
) -> DFResult<()> {
    let namespace = DirectoryNamespaceBuilder::new(root)
        .build()
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to build DirectoryNamespace for root '{}': {}",
                root, e
            ))
        })?;

    let namespace: Arc<dyn LanceNamespace> = Arc::new(namespace);

    let catalog = Arc::new(LanceCatalogProvider::new());
    catalog
        .register_namespace(schema_name.to_string(), namespace)
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to register Lance schema '{}.{}': {}",
                catalog_name, schema_name, e
            ))
        })?;

    ctx.register_catalog(catalog_name.to_string(), catalog);

    Ok(())
}
