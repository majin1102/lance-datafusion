// SPDX-License-Identifier: Apache-2.0

use lance::dataset::builder::DatasetBuilder;
use lance::Dataset;
use lance::Result;
use lance_namespace::models::{ListNamespacesRequest, ListTablesRequest};
use lance_namespace::LanceNamespace;
use std::sync::Arc;

const DEFAULT_CATALOG_NAME: &str = "lance";

#[derive(Debug, Clone)]
pub struct Namespace {
    root: Arc<dyn LanceNamespace>,
    /// Full namespace identifier, e.g. [dir1, dir2].
    namespace_id: Option<Vec<String>>,
}

impl From<Arc<dyn LanceNamespace>> for Namespace {
    fn from(lance_namespace: Arc<dyn LanceNamespace>) -> Self {
        Self::from_root(Arc::clone(&lance_namespace))
    }
}

impl From<(Arc<dyn LanceNamespace>, String)> for Namespace {
    fn from(lance_namespace: (Arc<dyn LanceNamespace>, String)) -> Self {
        Self::from_namespace(Arc::clone(&lance_namespace.0), vec![lance_namespace.1])
    }
}

impl From<(Arc<dyn LanceNamespace>, Vec<String>)> for Namespace {
    fn from(lance_namespace: (Arc<dyn LanceNamespace>, Vec<String>)) -> Self {
        Self::from_namespace(Arc::clone(&lance_namespace.0), lance_namespace.1)
    }
}

impl Namespace {
    pub fn from_root(root: Arc<dyn LanceNamespace>) -> Self {
        Self {
            root,
            namespace_id: None,
        }
    }

    pub fn from_namespace(root: Arc<dyn LanceNamespace>, namespace_id: Vec<String>) -> Self {
        Self {
            root,
            namespace_id: Some(namespace_id),
        }
    }

    pub fn id(&self) -> Vec<String> {
        self.namespace_id.clone().unwrap_or_default()
    }

    pub fn name(&self) -> &str {
        self.namespace_id
            .as_deref()
            .and_then(|v| v.last())
            .map_or(DEFAULT_CATALOG_NAME, |relative_name| relative_name.as_str())
    }

    pub fn child_id(&self, child_name: String) -> Vec<String> {
        match &self.namespace_id {
            Some(namespace_id) => {
                let mut child_namespace = namespace_id.clone();
                child_namespace.push(child_name.to_string());
                child_namespace
            }
            None => vec![child_name.to_string()],
        }
    }

    pub async fn children(&self) -> Result<Vec<Namespace>> {
        let root = Arc::clone(&self.root);
        let namespace_id = self.namespace_id.clone();
        let request = ListNamespacesRequest {
            id: namespace_id,
            page_token: None,
            limit: None,
        };

        let namespaces = root
            .list_namespaces(request)
            .await
            .map(|response| response.namespaces)?;

        Ok(namespaces
            .into_iter()
            .map(|relative_ns_id| {
                Namespace::from_namespace(Arc::clone(&self.root), self.child_id(relative_ns_id))
            })
            .collect())
    }

    pub async fn tables(&self) -> Result<Vec<String>> {
        let root = Arc::clone(&self.root);
        let namespace_id = self.namespace_id.clone();
        let request = ListTablesRequest {
            id: namespace_id,
            page_token: None,
            limit: None,
        };

        root.list_tables(request).await.map(|resp| resp.tables)
    }

    pub async fn load_dataset(&self, table_name: &str) -> Result<Dataset> {
        DatasetBuilder::from_namespace(
            Arc::clone(&self.root),
            self.child_id(table_name.to_string()),
            false,
        )
        .await?
        .load()
        .await
    }
}
