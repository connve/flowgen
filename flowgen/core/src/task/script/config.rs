//! Configuration for script-based event transformation task.
//!
//! Defines the configuration structure for executing scripts (Rhai)
//! to transform, filter, or manipulate event data in the pipeline.

use serde::{Deserialize, Serialize};

/// Script processor configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Processor {
    /// Task name for identification.
    pub name: String,
    /// Script engine type (defaults to Rhai).
    #[serde(default)]
    pub engine: ScriptEngine,
    /// Script source code to execute (inline or from resource file).
    ///
    /// # Examples
    ///
    /// Inline script:
    /// ```yaml
    /// code: "event.data.value = event.data.value * 2; event"
    /// ```
    ///
    /// Inline multi-line script:
    /// ```yaml
    /// code: |
    ///   let value = event.data.value;
    ///   event.data.result = value * 2;
    ///   event
    /// ```
    ///
    /// External script file:
    /// ```yaml
    /// code:
    ///   resource: "scripts/transform_data.rhai"
    /// ```
    pub code: crate::resource::Source,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<crate::retry::RetryConfig>,
}

impl Default for Processor {
    fn default() -> Self {
        Self {
            name: String::new(),
            engine: ScriptEngine::default(),
            code: crate::resource::Source::Inline(String::new()),
            retry: None,
        }
    }
}

/// Supported script engine types.
#[derive(PartialEq, Eq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ScriptEngine {
    /// Rhai scripting engine.
    #[default]
    Rhai,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processor_config_creation() {
        let config = Processor {
            name: "test_script".to_string(),
            engine: ScriptEngine::Rhai,
            code: crate::resource::Source::Inline("data + 1".to_string()),
            retry: None,
        };

        assert_eq!(config.name, "test_script");
        assert_eq!(config.engine, ScriptEngine::Rhai);
        assert_eq!(
            config.code,
            crate::resource::Source::Inline("data + 1".to_string())
        );
    }

    #[test]
    fn test_processor_config_default() {
        let config = Processor::default();
        assert_eq!(config.name, "");
        assert_eq!(config.engine, ScriptEngine::Rhai);
        assert_eq!(config.code, crate::resource::Source::Inline("".to_string()));
        assert!(config.retry.is_none());
    }

    #[test]
    fn test_script_engine_default() {
        let engine = ScriptEngine::default();
        assert_eq!(engine, ScriptEngine::Rhai);
    }

    #[test]
    fn test_config_serialization() {
        let config = Processor {
            name: "transform".to_string(),
            engine: ScriptEngine::Rhai,
            code: crate::resource::Source::Inline("data * 2".to_string()),
            retry: None,
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: Processor = serde_json::from_str(&serialized).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_config_clone() {
        let config = Processor {
            name: "clone_test".to_string(),
            engine: ScriptEngine::Rhai,
            code: crate::resource::Source::Inline("data".to_string()),
            retry: None,
        };

        let cloned = config.clone();
        assert_eq!(config, cloned);
    }

    #[test]
    fn test_config_with_resource() {
        let config = Processor {
            name: "resource_script".to_string(),
            engine: ScriptEngine::Rhai,
            code: crate::resource::Source::Resource {
                resource: "scripts/transform.rhai".to_string(),
            },
            retry: None,
        };

        assert_eq!(config.name, "resource_script");
        match config.code {
            crate::resource::Source::Resource { resource } => {
                assert_eq!(resource, "scripts/transform.rhai");
            }
            _ => panic!("Expected Resource variant"),
        }
    }
}
