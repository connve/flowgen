//! Configuration for script-based event transformation task.
//!
//! Defines the configuration structure for executing scripts (Rhai)
//! to transform, filter, or manipulate event data in the pipeline.

use serde::{Deserialize, Serialize};

/// Script processor configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, Default)]
pub struct Processor {
    /// Task name for identification.
    pub name: String,
    /// Script engine type (defaults to Rhai).
    #[serde(default)]
    pub engine: ScriptEngine,
    /// Script source code to execute.
    pub code: String,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<crate::retry::RetryConfig>,
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
            code: "data + 1".to_string(),
            retry: None,
        };

        assert_eq!(config.name, "test_script");
        assert_eq!(config.engine, ScriptEngine::Rhai);
        assert_eq!(config.code, "data + 1");
    }

    #[test]
    fn test_processor_config_default() {
        let config = Processor::default();
        assert_eq!(config.name, "");
        assert_eq!(config.engine, ScriptEngine::Rhai);
        assert_eq!(config.code, "");
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
            code: "data * 2".to_string(),
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
            code: "data".to_string(),
            retry: None,
        };

        let cloned = config.clone();
        assert_eq!(config, cloned);
    }
}
