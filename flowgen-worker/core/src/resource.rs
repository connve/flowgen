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
/// Uses untagged serde deserialization for clean YAML syntax.
///
/// # Examples
///
/// Inline SQL query (single line):
/// ```yaml
/// query: "SELECT * FROM table"
/// ```
///
/// Inline SQL query (multi-line):
/// ```yaml
/// query: |
///   SELECT * FROM table
///   WHERE id = 1
/// ```
///
/// External resource file:
/// ```yaml
/// query:
///   resource: "queries/get_orders.sql"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Source {
    /// Resource key referencing an external file.
    /// Example: "queries/get_orders.sql" resolves to "{resource_path}/queries/get_orders.sql".
    /// Must be specified as object with "resource" key in YAML.
    Resource { resource: String },
    /// Inline content embedded directly in configuration.
    /// Any plain string value is treated as inline content.
    Inline(String),
}

impl Source {
    /// Resolves the source to its content string.
    ///
    /// For inline sources, returns the content directly.
    /// For resource sources, loads the file from the configured resource path.
    ///
    /// # Arguments
    /// * `loader` - Optional resource loader for resolving file paths.
    ///
    /// # Returns
    /// The resolved content string.
    pub async fn resolve(&self, loader: Option<&ResourceLoader>) -> Result<String, Error> {
        match self {
            Source::Inline(content) => Ok(content.clone()),
            Source::Resource { resource } => {
                let loader = loader.ok_or(Error::ResourcePathNotConfigured)?;
                loader.load(resource).await
            }
        }
    }
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

    #[test]
    fn test_source_inline_single_line() {
        let yaml = r#"query: "SELECT * FROM table""#;
        let parsed: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        let source: Source = serde_yaml::from_value(parsed["query"].clone()).unwrap();

        match source {
            Source::Inline(content) => assert_eq!(content, "SELECT * FROM table"),
            _ => panic!("Expected Inline variant"),
        }
    }

    #[test]
    fn test_source_inline_multiline() {
        let yaml = r#"
query: |
  SELECT * FROM table
  WHERE id = 1
"#;
        let parsed: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        let source: Source = serde_yaml::from_value(parsed["query"].clone()).unwrap();

        match source {
            Source::Inline(content) => {
                assert!(content.contains("SELECT * FROM table"));
                assert!(content.contains("WHERE id = 1"));
            }
            _ => panic!("Expected Inline variant"),
        }
    }

    #[test]
    fn test_source_resource() {
        let yaml = r#"
query:
  resource: "queries/orders.sql"
"#;
        let parsed: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        let source: Source = serde_yaml::from_value(parsed["query"].clone()).unwrap();

        match source {
            Source::Resource { resource } => assert_eq!(resource, "queries/orders.sql"),
            _ => panic!("Expected Resource variant"),
        }
    }

    #[tokio::test]
    async fn test_source_resolve_inline() {
        let source = Source::Inline("SELECT * FROM table".to_string());
        let result = source.resolve(None).await.unwrap();
        assert_eq!(result, "SELECT * FROM table");
    }

    #[tokio::test]
    async fn test_source_resolve_resource_without_loader() {
        let source = Source::Resource {
            resource: "queries/test.sql".to_string(),
        };
        let result = source.resolve(None).await;
        assert!(matches!(result, Err(Error::ResourcePathNotConfigured)));
    }

    #[test]
    fn test_source_serialization_inline() {
        let source = Source::Inline("SELECT * FROM table".to_string());
        let serialized = serde_yaml::to_string(&source).unwrap();
        assert_eq!(serialized.trim(), "SELECT * FROM table");
    }

    #[test]
    fn test_source_serialization_resource() {
        let source = Source::Resource {
            resource: "queries/orders.sql".to_string(),
        };
        let serialized = serde_yaml::to_string(&source).unwrap();
        assert!(serialized.contains("resource"));
        assert!(serialized.contains("queries/orders.sql"));
    }

    #[test]
    fn test_source_clone() {
        let source = Source::Inline("test content".to_string());
        let cloned = source.clone();
        assert_eq!(source, cloned);
    }

    #[test]
    fn test_source_debug() {
        let source = Source::Inline("test".to_string());
        let debug_str = format!("{source:?}");
        assert!(debug_str.contains("Inline"));
        assert!(debug_str.contains("test"));
    }
}
