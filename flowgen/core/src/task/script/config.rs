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
    /// Resource limits for the Rhai engine to bound CPU and memory use.
    /// Defaults are chosen to keep a single misbehaving script from
    /// stalling the worker. Overridable per-task in flow YAML.
    #[serde(default)]
    pub limits: RhaiLimits,
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
    /// Optional sandbox configuration for script execution.
    /// Required for Python/Bash scripts, not needed for Rhai (safe embedded language).
    #[serde(default)]
    pub sandbox: Option<crate::nsjail::SandboxConfig>,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
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
            sandbox: None,
            limits: RhaiLimits::default(),
            depends_on: None,
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

fn default_max_operations() -> u64 {
    10_000_000
}
fn default_max_call_depth() -> usize {
    64
}
fn default_max_string_size() -> usize {
    16 * 1024 * 1024
}
fn default_max_array_size() -> usize {
    100_000
}
fn default_max_map_size() -> usize {
    100_000
}

/// Rhai engine resource limits.
///
/// Bounds the CPU and memory a single script invocation can consume,
/// preventing a malicious or buggy script from stalling the worker.
#[derive(PartialEq, Eq, Clone, Debug, Deserialize, Serialize)]
pub struct RhaiLimits {
    /// Maximum number of operations the script may execute before
    /// the engine aborts it. Each Rhai operation is roughly one
    /// bytecode step; the default ~10M operations is on the order
    /// of a few seconds of CPU on modern hardware.
    #[serde(default = "default_max_operations")]
    pub max_operations: u64,
    /// Maximum function call nesting depth.
    #[serde(default = "default_max_call_depth")]
    pub max_call_depth: usize,
    /// Maximum string length in bytes.
    #[serde(default = "default_max_string_size")]
    pub max_string_size: usize,
    /// Maximum number of elements in an array.
    #[serde(default = "default_max_array_size")]
    pub max_array_size: usize,
    /// Maximum number of entries in a map.
    #[serde(default = "default_max_map_size")]
    pub max_map_size: usize,
}

impl Default for RhaiLimits {
    fn default() -> Self {
        Self {
            max_operations: default_max_operations(),
            max_call_depth: default_max_call_depth(),
            max_string_size: default_max_string_size(),
            max_array_size: default_max_array_size(),
            max_map_size: default_max_map_size(),
        }
    }
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
            sandbox: None,
            limits: RhaiLimits::default(),
            depends_on: None,
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
        assert!(config.sandbox.is_none());
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
            sandbox: None,
            limits: RhaiLimits::default(),
            depends_on: None,
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
            sandbox: None,
            limits: RhaiLimits::default(),
            depends_on: None,
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
            sandbox: None,
            limits: RhaiLimits::default(),
            depends_on: None,
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
