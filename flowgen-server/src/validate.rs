//! Flow validation module.
//!
//! Validates discovered flow YAML files by parsing them and enforcing
//! unique flow names across the entire repository.

use std::collections::HashMap;
use std::path::PathBuf;

/// Errors that can occur during flow validation.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to read file '{path}': {source}")]
    FileRead {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Failed to parse YAML in '{path}': {source}")]
    YamlParse {
        path: String,
        #[source]
        source: serde_yaml::Error,
    },

    #[error("Duplicate flow name '{name}' found in '{file_a}' and '{file_b}'")]
    DuplicateFlowName {
        name: String,
        file_a: String,
        file_b: String,
    },

    #[error("Missing 'flow.name' field in '{path}'")]
    MissingFlowName { path: String },
}

/// Lightweight struct for extracting the flow name without pulling in
/// the full FlowConfig type (which depends on every task processor crate).
#[derive(serde::Deserialize)]
struct FlowNameCheck {
    flow: FlowNameOnly,
}

/// Minimal flow struct that only extracts the name field.
#[derive(serde::Deserialize)]
struct FlowNameOnly {
    name: Option<String>,
}

/// A validated flow with its source path and raw YAML content.
pub struct ValidatedFlow {
    /// The source file path within the repository.
    pub path: PathBuf,
    /// The raw YAML content.
    pub content: String,
}

/// Validates a set of discovered flow files.
///
/// Parses each file, extracts the flow name, and checks for duplicates.
/// Returns a map of flow name to validated flow data.
pub fn validate_flows(flow_files: &[PathBuf]) -> Result<HashMap<String, ValidatedFlow>, Error> {
    let mut flows: HashMap<String, ValidatedFlow> = HashMap::new();

    for file_path in flow_files {
        let content = std::fs::read_to_string(file_path).map_err(|source| Error::FileRead {
            path: file_path.display().to_string(),
            source,
        })?;

        let parsed: FlowNameCheck =
            serde_yaml::from_str(&content).map_err(|source| Error::YamlParse {
                path: file_path.display().to_string(),
                source,
            })?;

        let name = parsed.flow.name.ok_or_else(|| Error::MissingFlowName {
            path: file_path.display().to_string(),
        })?;

        if let Some(existing) = flows.get(&name) {
            return Err(Error::DuplicateFlowName {
                name,
                file_a: existing.path.display().to_string(),
                file_b: file_path.display().to_string(),
            });
        }

        flows.insert(
            name.clone(),
            ValidatedFlow {
                path: file_path.clone(),
                content,
            },
        );
    }

    Ok(flows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_validate_flows_extracts_names() {
        let dir = tempfile::tempdir().unwrap();
        let flow_a = dir.path().join("flow_a.yaml");
        let flow_b = dir.path().join("flow_b.yaml");

        fs::write(
            &flow_a,
            "flow:\n  name: alpha\n  tasks:\n    - log:\n        name: test\n",
        )
        .unwrap();
        fs::write(
            &flow_b,
            "flow:\n  name: beta\n  tasks:\n    - log:\n        name: test\n",
        )
        .unwrap();

        let files = vec![flow_a, flow_b];
        let result = validate_flows(&files).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains_key("alpha"));
        assert!(result.contains_key("beta"));
    }

    #[test]
    fn test_validate_flows_detects_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let flow_a = dir.path().join("flow_a.yaml");
        let flow_b = dir.path().join("flow_b.yaml");

        fs::write(&flow_a, "flow:\n  name: same_name\n  tasks: []\n").unwrap();
        fs::write(&flow_b, "flow:\n  name: same_name\n  tasks: []\n").unwrap();

        let files = vec![flow_a, flow_b];
        let result = validate_flows(&files);
        assert!(matches!(result, Err(Error::DuplicateFlowName { .. })));
    }

    #[test]
    fn test_validate_flows_missing_name() {
        let dir = tempfile::tempdir().unwrap();
        let flow = dir.path().join("bad.yaml");
        fs::write(&flow, "flow:\n  tasks: []\n").unwrap();

        let files = vec![flow];
        let result = validate_flows(&files);
        assert!(matches!(result, Err(Error::MissingFlowName { .. })));
    }

    #[test]
    fn test_validate_flows_invalid_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let flow = dir.path().join("bad.yaml");
        fs::write(&flow, "not: valid: yaml: {{{}}}").unwrap();

        let files = vec![flow];
        let result = validate_flows(&files);
        assert!(matches!(result, Err(Error::YamlParse { .. })));
    }
}
