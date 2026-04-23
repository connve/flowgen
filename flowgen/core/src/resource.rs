//! Resource loading system for external assets.
//!
//! Provides support for loading external files (SQL queries, JSON templates, HTML, etc.)
//! using resource keys instead of embedding content in configuration files.
//!
//! Resources can be loaded from two backends:
//! - **Filesystem**: Base directory path, resource keys resolve to files (default).
//! - **Cache**: Distributed cache, resource keys resolve to cache keys with a configured prefix.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
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

    /// Renders the source with template substitution.
    ///
    /// For inline sources, returns the content directly (already rendered by config.render()).
    /// For resource sources, loads the file and renders it as a Handlebars template.
    ///
    /// This is the preferred method for handling queries, scripts, and templates that support
    /// dynamic variable substitution from event data.
    ///
    /// # Arguments
    /// * `loader` - Optional resource loader for resolving file paths.
    /// * `template_data` - Data context for template variable substitution.
    ///
    /// # Returns
    /// The resolved and rendered content string.
    ///
    /// # Example
    /// ```rust
    /// use flowgen_core::resource::{Source, ResourceLoader};
    /// use serde_json::json;
    /// use std::path::PathBuf;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let source = Source::Inline("SELECT * FROM table WHERE id = {{event.data.id}}".to_string());
    /// let data = json!({"event": {"data": {"id": 123}}});
    /// let result = source.render(None, &data).await?;
    /// assert_eq!(result, "SELECT * FROM table WHERE id = 123");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn render<T>(
        &self,
        loader: Option<&ResourceLoader>,
        template_data: &T,
    ) -> Result<String, Error>
    where
        T: serde::Serialize,
    {
        match self {
            // Inline content is already rendered by config.render(), return as-is.
            Source::Inline(content) => Ok(content.clone()),
            // Resource content needs to be loaded and then rendered.
            Source::Resource { resource } => {
                let loader = loader.ok_or(Error::ResourcePathNotConfigured)?;
                let template = loader.load(resource).await?;
                crate::config::render_template(&template, template_data)
                    .map_err(|source| Error::TemplateRender { source })
            }
        }
    }
}

/// Errors that can occur during resource operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error reading resource '{key}': {source}")]
    ReadResource {
        key: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Resource not found: {}", key)]
    ResourceNotFound { key: String },
    #[error("Resource path is not configured in config.yaml")]
    ResourcePathNotConfigured,
    #[error("Template rendering error: {source}")]
    TemplateRender {
        #[source]
        source: crate::config::Error,
    },
    #[error("Cache error loading resource '{key}': {source}")]
    Cache {
        key: String,
        #[source]
        source: crate::cache::CacheError,
    },
    #[error("Cache resource '{key}' is not valid UTF-8: {source}")]
    CacheUtf8 {
        key: String,
        #[source]
        source: std::string::FromUtf8Error,
    },
}

/// Backend for the `ResourceLoader`.
///
/// Resources can be loaded from either the local filesystem or the distributed cache.
#[derive(Clone)]
enum Backend {
    /// Filesystem backend: resource keys resolve to files under `base_path`.
    Filesystem { base_path: PathBuf },
    /// Cache backend: resource keys resolve to cache keys under `prefix`.
    Cache {
        cache: Arc<dyn crate::cache::Cache>,
        prefix: String,
    },
    /// Disabled backend: all loads fail with `ResourcePathNotConfigured`.
    Disabled,
}

impl std::fmt::Debug for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Backend::Filesystem { base_path } => f
                .debug_struct("Filesystem")
                .field("base_path", base_path)
                .finish(),
            Backend::Cache { prefix, .. } => {
                f.debug_struct("Cache").field("prefix", prefix).finish()
            }
            Backend::Disabled => f.write_str("Disabled"),
        }
    }
}

/// Resource loader that resolves resource keys to file content.
///
/// Maps resource keys to content using one of two backends:
///
/// **Filesystem** (with `path: "local/resources/"`):
/// - Key `"query.sql"` → `"local/resources/query.sql"`
/// - Key `"queries/orders.sql"` → `"local/resources/queries/orders.sql"`
///
/// **Cache** (with `prefix: "flowgen.resources"`):
/// - Key `"query.sql"` → cache key `"flowgen.resources.query.sql"`
/// - Key `"queries/orders.sql"` → cache key `"flowgen.resources.queries/orders.sql"`
#[derive(Debug, Clone)]
pub struct ResourceLoader {
    backend: Backend,
}

impl ResourceLoader {
    /// Creates a new filesystem-backed `ResourceLoader` with the given base path.
    ///
    /// # Arguments
    /// * `base_path` - Base directory for resources, or None to disable resource loading.
    pub fn new(base_path: Option<PathBuf>) -> Self {
        let backend = match base_path {
            Some(path) => Backend::Filesystem { base_path: path },
            None => Backend::Disabled,
        };
        Self { backend }
    }

    /// Creates a new cache-backed `ResourceLoader`.
    ///
    /// Resources are loaded from the distributed cache using keys formed by
    /// joining `prefix` with the resource key (e.g., `"flowgen.resources.query.sql"`).
    ///
    /// # Arguments
    /// * `cache` - Cache backend for loading resources.
    /// * `prefix` - Cache key prefix for resources (e.g., "flowgen.resources").
    pub fn from_cache(cache: Arc<dyn crate::cache::Cache>, prefix: String) -> Self {
        Self {
            backend: Backend::Cache { cache, prefix },
        }
    }

    /// Loads a resource by key.
    ///
    /// # Arguments
    /// * `key` - Resource key (e.g., "query.sql" or "queries/orders.sql").
    ///
    /// # Returns
    /// The resource content as a String.
    pub async fn load(&self, key: &str) -> Result<String, Error> {
        match &self.backend {
            Backend::Filesystem { base_path } => {
                let full_path = base_path.join(key);
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
            Backend::Cache { cache, prefix } => {
                let cache_key = format!("{prefix}.{key}");
                let bytes = cache
                    .get(&cache_key)
                    .await
                    .map_err(|source| Error::Cache {
                        key: key.to_string(),
                        source,
                    })?
                    .ok_or_else(|| Error::ResourceNotFound {
                        key: key.to_string(),
                    })?;
                String::from_utf8(bytes.to_vec()).map_err(|source| Error::CacheUtf8 {
                    key: key.to_string(),
                    source,
                })
            }
            Backend::Disabled => Err(Error::ResourcePathNotConfigured),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_new_with_path() {
        let loader = ResourceLoader::new(Some(PathBuf::from("local/resources")));
        assert!(matches!(loader.backend, Backend::Filesystem { .. }));
    }

    #[test]
    fn test_new_without_path() {
        let loader = ResourceLoader::new(None);
        assert!(matches!(loader.backend, Backend::Disabled));
    }

    #[tokio::test]
    async fn test_load_without_base_path() {
        let loader = ResourceLoader::new(None);
        let result = loader.load("test.sql").await;
        assert!(matches!(result, Err(Error::ResourcePathNotConfigured)));
    }

    #[tokio::test]
    async fn test_from_cache_load_existing() {
        let cache =
            Arc::new(crate::cache::memory::MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        cache
            .put(
                "flowgen.resources.queries/orders.sql",
                bytes::Bytes::from("SELECT * FROM orders"),
                None,
            )
            .await
            .unwrap();

        let loader = ResourceLoader::from_cache(cache, "flowgen.resources".to_string());
        let result = loader.load("queries/orders.sql").await.unwrap();
        assert_eq!(result, "SELECT * FROM orders");
    }

    #[tokio::test]
    async fn test_from_cache_load_missing() {
        let cache =
            Arc::new(crate::cache::memory::MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let loader = ResourceLoader::from_cache(cache, "flowgen.resources".to_string());
        let result = loader.load("nonexistent.sql").await;
        assert!(matches!(result, Err(Error::ResourceNotFound { .. })));
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
