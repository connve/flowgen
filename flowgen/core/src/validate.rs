//! Validation helpers for config-supplied identifiers and paths.
//!
//! Names and paths from config flow into derived filesystem locations
//! (e.g. the default `git_sync` `clone_path`). These helpers reject
//! inputs that could escape the intended directory or otherwise break
//! path handling, and are reused by both the flow loader and individual
//! processor configs.

use std::path::Path;

/// Identifies which config field a name belongs to so error messages
/// can point at the offending source without stringly-typed labels.
#[derive(Clone, Copy, Debug)]
pub enum NameField {
    Flow,
    Task,
    Resource,
}

impl NameField {
    const fn as_str(self) -> &'static str {
        match self {
            NameField::Flow => "Flow",
            NameField::Task => "Task",
            NameField::Resource => "Resource",
        }
    }
}

/// Identifies which config field a filesystem path belongs to.
#[derive(Clone, Copy, Debug)]
pub enum PathField {
    ClonePath,
}

impl PathField {
    const fn as_str(self) -> &'static str {
        match self {
            PathField::ClonePath => "clone_path",
        }
    }
}

/// Validates that a name is safe to use as a filesystem path segment.
///
/// Only ASCII alphanumerics, `_`, and `-` are accepted. Rejects empty
/// strings and anything that could escape a parent directory (`/`, `\`,
/// `..`, leading-slash absolutes, NUL, whitespace, Unicode look-alikes).
pub fn validate_name(field: NameField, name: &str) -> Result<(), String> {
    let label = field.as_str();
    if name.is_empty() {
        return Err(format!("{label} name is empty"));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(format!(
            "{label} name '{name}' contains invalid characters \
             (only ASCII alphanumerics, '_', and '-' are allowed)"
        ));
    }
    Ok(())
}

/// Validates that an operator-supplied path is safe to use as a working
/// directory.
///
/// Rejects paths containing `..` segments. Absolute paths are accepted
/// because overriding to a stable persistent volume is a legitimate use
/// case; traversal segments never are.
pub fn validate_path(field: PathField, path: &Path) -> Result<(), String> {
    use std::path::Component;
    let label = field.as_str();
    for component in path.components() {
        if matches!(component, Component::ParentDir) {
            return Err(format!(
                "{label} '{}' contains a '..' segment which is not allowed",
                path.display()
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn name_accepts_valid() {
        assert!(validate_name(NameField::Flow, "my_flow-1").is_ok());
        assert!(validate_name(NameField::Task, "TaskName").is_ok());
    }

    #[test]
    fn name_rejects_empty() {
        assert!(validate_name(NameField::Flow, "").is_err());
    }

    #[test]
    fn name_rejects_traversal() {
        assert!(validate_name(NameField::Flow, "..").is_err());
        assert!(validate_name(NameField::Flow, "../etc").is_err());
    }

    #[test]
    fn name_rejects_separators() {
        assert!(validate_name(NameField::Flow, "a/b").is_err());
        assert!(validate_name(NameField::Flow, "a\\b").is_err());
    }

    #[test]
    fn name_rejects_dot_segment() {
        // '.' is not in the allowed set, so it gets rejected. This also
        // prevents disguised-traversal names like ".env" or "a.b".
        assert!(validate_name(NameField::Flow, ".").is_err());
        assert!(validate_name(NameField::Flow, "a.b").is_err());
    }

    #[test]
    fn path_accepts_normal() {
        assert!(validate_path(PathField::ClonePath, &PathBuf::from("/var/tmp/repo")).is_ok());
        assert!(validate_path(PathField::ClonePath, &PathBuf::from("repos/x")).is_ok());
    }

    #[test]
    fn path_rejects_traversal() {
        assert!(validate_path(PathField::ClonePath, &PathBuf::from("/tmp/../etc")).is_err());
        assert!(validate_path(PathField::ClonePath, &PathBuf::from("../escape")).is_err());
    }
}
