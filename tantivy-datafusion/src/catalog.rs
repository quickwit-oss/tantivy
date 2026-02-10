use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use tantivy::Index;

use crate::table_provider::TantivyTableProvider;

/// A DataFusion catalog that discovers tantivy indices from a root directory.
///
/// Each subdirectory containing a `meta.json` file is treated as a tantivy index
/// and exposed as a table.
pub struct TantivyCatalog {
    schema_provider: Arc<TantivySchema>,
}

impl fmt::Debug for TantivyCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TantivyCatalog").finish()
    }
}

impl TantivyCatalog {
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            schema_provider: Arc::new(TantivySchema::new(root_dir)),
        }
    }
}

impl CatalogProvider for TantivyCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["default".to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "default" {
            Some(self.schema_provider.clone())
        } else {
            None
        }
    }
}

/// A DataFusion schema provider that discovers tantivy indices from a directory.
pub struct TantivySchema {
    root_dir: PathBuf,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl fmt::Debug for TantivySchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TantivySchema")
            .field("root_dir", &self.root_dir)
            .finish()
    }
}

impl TantivySchema {
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
            tables: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl SchemaProvider for TantivySchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(&self.root_dir) else {
            return vec![];
        };
        entries
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.is_dir() && path.join("meta.json").exists() {
                    entry.file_name().to_str().map(String::from)
                } else {
                    None
                }
            })
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // Check cache first
        {
            let tables = self
                .tables
                .read()
                .map_err(|e| DataFusionError::Internal(format!("lock poisoned: {e}")))?;
            if let Some(table) = tables.get(name) {
                return Ok(Some(table.clone()));
            }
        }

        let index_path = self.root_dir.join(name);
        if !is_tantivy_index(&index_path) {
            return Ok(None);
        }

        let index = Index::open_in_dir(&index_path)
            .map_err(|e| DataFusionError::Internal(format!("open index '{name}': {e}")))?;
        let provider: Arc<dyn TableProvider> = Arc::new(TantivyTableProvider::new(index));

        let mut tables = self
            .tables
            .write()
            .map_err(|e| DataFusionError::Internal(format!("lock poisoned: {e}")))?;
        tables.insert(name.to_string(), provider.clone());
        Ok(Some(provider))
    }

    fn table_exist(&self, name: &str) -> bool {
        is_tantivy_index(&self.root_dir.join(name))
    }
}

fn is_tantivy_index(path: &Path) -> bool {
    path.is_dir() && path.join("meta.json").exists()
}
