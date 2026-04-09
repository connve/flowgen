//! Recursive file discovery for flows and resources.
//!
//! Scans directories within a cloned Git repository to find
//! flow YAML files and resource files for cache synchronization.

use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// Supported flow configuration file extensions.
const FLOW_EXTENSIONS: &[&str] = &["yaml", "yml"];

/// Recursively discovers flow YAML files under the given base path.
pub fn discover_flows(base_path: &Path) -> Result<Vec<PathBuf>, walkdir::Error> {
    let mut flow_files = Vec::new();

    for entry in WalkDir::new(base_path)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();

        if path.is_file() {
            if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                if FLOW_EXTENSIONS.contains(&ext) {
                    flow_files.push(path.to_path_buf());
                }
            }
        }
    }

    Ok(flow_files)
}

/// Recursively discovers all resource files under the given base path.
pub fn discover_resources(base_path: &Path) -> Result<Vec<PathBuf>, walkdir::Error> {
    let mut resource_files = Vec::new();

    for entry in WalkDir::new(base_path)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();

        if path.is_file() {
            resource_files.push(path.to_path_buf());
        }
    }

    Ok(resource_files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_discover_flows_finds_yaml_files() {
        let dir = tempfile::tempdir().unwrap();
        let flows_dir = dir.path().join("flows");
        fs::create_dir_all(flows_dir.join("nested")).unwrap();

        fs::write(flows_dir.join("flow_a.yaml"), "flow: {}").unwrap();
        fs::write(flows_dir.join("nested/flow_b.yml"), "flow: {}").unwrap();
        fs::write(flows_dir.join("readme.md"), "# readme").unwrap();

        let results = discover_flows(&flows_dir).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_discover_resources_finds_all_files() {
        let dir = tempfile::tempdir().unwrap();
        let resources_dir = dir.path().join("resources");
        fs::create_dir_all(resources_dir.join("scripts")).unwrap();

        fs::write(resources_dir.join("template.hbs"), "hello").unwrap();
        fs::write(resources_dir.join("scripts/transform.rhai"), "42").unwrap();

        let results = discover_resources(&resources_dir).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_discover_flows_empty_directory() {
        let dir = tempfile::tempdir().unwrap();
        let results = discover_flows(dir.path()).unwrap();
        assert!(results.is_empty());
    }
}
