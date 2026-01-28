//! Resource loading system for external assets.
//!
//! Provides support for loading external files (SQL queries, JSON templates, HTML, etc.)
//! using resource keys instead of embedding content in configuration files.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::fs;

/// Generic source for content that can be inline or loaded from external resource.
///
/// This enum is used across different task configurations to support both
/// inline content (embedded in YAML) and external resource files.
///
/// # Examples
///
/// Inline SQL query:
/// ```yaml
/// query:
///   inline: "SELECT * FROM table"
/// ```
///
/// External resource file:
/// ```yaml
/// query:
///   resource: "queries/get_orders.sql"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// Inline content embedded directly in configuration.
    Inline(String),
    /// Resource key referencing an external file.
    /// Example: "queries/get_orders.sql" resolves to "{resource_path}/queries/get_orders.sql".
    Resource(String),
}

/// Errors that can occur during resource operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to read resource '{key}': {source}")]
    ReadResource {
        key: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Resource not found: {}", key)]
    ResourceNotFound { key: String },
    #[error("Resource path is not configured in config.yaml")]
    ResourcePathNotConfigured,
}

/// Resource loader that resolves resource keys to file content.
///
/// Maps resource keys to files in the configured resource directory.
/// For example, with `path: "local/resources/"`:
/// - Key `"query.sql"` → `"local/resources/query.sql"`
/// - Key `"queries/orders.sql"` → `"local/resources/queries/orders.sql"`
#[derive(Debug, Clone)]
pub struct ResourceLoader {
    /// Base path for resources. If None, resource loading is disabled.
    base_path: Option<PathBuf>,
}

impl ResourceLoader {
    /// Creates a new ResourceLoader with the given base path.
    ///
    /// # Arguments
    /// * `base_path` - Base directory for resources, or None to disable resource loading.
    pub fn new(base_path: Option<PathBuf>) -> Self {
        Self { base_path }
    }

    /// Loads a resource by key.
    ///
    /// # Arguments
    /// * `key` - Resource key (e.g., "query.sql" or "queries/orders.sql").
    ///
    /// # Returns
    /// The file content as a String.
    pub async fn load(&self, key: &str) -> Result<String, Error> {
        let base = self
            .base_path
            .as_ref()
            .ok_or(Error::ResourcePathNotConfigured)?;

        let full_path = base.join(key);

        fs::read_to_string(&full_path)
            .await
            .map_err(|source| match source.kind() {
                std::io::ErrorKind::NotFound => Error::ResourceNotFound {
                    key: key.to_string(),
                },
                _ => Error::ReadResource {
                    key: key.to_string(),
                    source,
                },
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_new_with_path() {
        let loader = ResourceLoader::new(Some(PathBuf::from("local/resources")));
        assert!(loader.base_path.is_some());
    }

    #[test]
    fn test_new_without_path() {
        let loader = ResourceLoader::new(None);
        assert!(loader.base_path.is_none());
    }

    #[tokio::test]
    async fn test_load_without_base_path() {
        let loader = ResourceLoader::new(None);
        let result = loader.load("test.sql").await;
        assert!(matches!(result, Err(Error::ResourcePathNotConfigured)));
    }
}
