// SPDX-License-Identifier: Apache-2.0

//! Builder for constructing DataFusion [`SessionContext`] instances
//! backed by Lance namespaces.
//!
//! This API is intentionally minimal and returns the native
//! [`SessionContext`] without adding an extra wrapper. It wires up
//! dynamic catalog and schema providers so that Lance namespaces can
//! be accessed via `catalog.schema.table` names.

use std::sync::Arc;

use datafusion::catalog::DynamicFileCatalog;
use datafusion::datasource::dynamic_file::DynamicListTableFactory;
use datafusion::error::Result;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion_session::SessionStore;

use crate::catalog::LanceCatalogProviderList;
use crate::dml::LanceSession;
use crate::namespace::Namespace;
use crate::url_factory::{LanceUrlTableFactory, MultiUrlTableFactory};
use crate::LanceCatalogProvider;

/// Builder for configuring a [`SessionContext`] with Lance namespaces.
#[derive(Clone, Debug, Default)]
pub struct SessionBuilder {
    /// Optional root namespace exposed via a dynamic
    /// [`LanceCatalogProviderList`].
    root: Option<Namespace>,
    /// Explicit catalogs to register by name.
    catalogs: Vec<(String, Namespace)>,
    /// Optional DataFusion session configuration.
    config: Option<SessionConfig>,
}

impl SessionBuilder {
    /// Create a new builder with no namespaces or configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a root [`LanceNamespace`] that is exposed as a dynamic
    /// catalog list via [`LanceCatalogProviderList`].
    pub fn with_root(mut self, ns: Namespace) -> Self {
        self.root = Some(ns);
        self
    }

    /// Register an additional catalog backed by the given namespace.
    ///
    /// The catalog is identified by `name` and can later be combined
    /// with schemas via [`SessionBuilder::add_schema`] using the same
    /// namespace.
    pub fn add_catalog(mut self, name: &str, ns: Namespace) -> Self {
        self.catalogs.push((name.to_string(), ns));
        self
    }

    /// Provide an explicit [`SessionConfig`] for the underlying
    /// [`SessionContext`].
    pub fn with_config(mut self, config: SessionConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Build a [`SessionContext`] with all configured namespaces.
    pub async fn build(self) -> Result<LanceSession> {
        let ctx = if let Some(config) = self.config {
            SessionContext::new_with_config(config)
        } else {
            SessionContext::new()
        };

        // Register Lance-backed catalogs first so that they participate in the
        // base catalog list before URL table support is layered on top.
        if let Some(root) = self.root {
            let catalog_list = Arc::new(LanceCatalogProviderList::try_new(root).await?);
            ctx.register_catalog_list(catalog_list);
        }

        for (catalog_name, namespace) in self.catalogs {
            ctx.register_catalog(
                &catalog_name,
                Arc::new(LanceCatalogProvider::try_new(namespace).await?),
            );
        }

        // Enable URL table support by wrapping the current catalog list with a
        // DynamicFileCatalog that uses both DataFusion's DynamicListTableFactory
        // (for Parquet/CSV/JSON/etc) and a LanceUrlTableFactory (for .lance URLs).
        let current_catalog_list = Arc::clone(ctx.state().catalog_list());

        let list_factory = Arc::new(DynamicListTableFactory::new(SessionStore::new()));
        let lance_factory = Arc::new(LanceUrlTableFactory::new(SessionStore::new()));
        let url_factory = Arc::new(MultiUrlTableFactory::new(vec![
            lance_factory.clone() as Arc<dyn datafusion::catalog::UrlTableFactory>,
            list_factory.clone() as Arc<dyn datafusion::catalog::UrlTableFactory>,
        ]));

        let catalog_list = Arc::new(DynamicFileCatalog::new(current_catalog_list, url_factory));

        let session_id = ctx.session_id();
        let ctx: SessionContext = ctx
            .into_state_builder()
            .with_session_id(session_id)
            .with_catalog_list(catalog_list)
            .build()
            .into();

        // Register the new SessionState with each SessionStore so that
        // URL table factories can access the runtime session as needed.
        list_factory
            .session_store()
            .with_state(ctx.state_weak_ref());
        lance_factory
            .session_store()
            .with_state(ctx.state_weak_ref());

        Ok(LanceSession::new(ctx))
    }
}
