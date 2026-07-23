//! Validation helpers for config-supplied identifiers and paths.
//!
//! Names and paths from config flow into derived filesystem locations
//! (e.g. the default `git_sync` `clone_path`). These helpers reject
//! inputs that could escape the intended directory or otherwise break
//! path handling, and are reused by both the flow loader and individual
//! processor configs.

use std::path::{Path, PathBuf};

/// Identifies which config field a name belongs to so error messages
/// can point at the offending source without stringly-typed labels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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
///
/// Carries the field name as a static string so callers (e.g. `clone_path`,
/// `credentials_path`) can reuse the same validator without this enum
/// growing a variant per call site.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PathField(pub &'static str);

impl PathField {
    const fn as_str(self) -> &'static str {
        self.0
    }
}

/// Errors produced by the validation helpers.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("{} name is empty", field.as_str())]
    EmptyName { field: NameField },
    #[error(
        "{} name '{name}' contains invalid characters \
         (only ASCII alphanumerics, '_', and '-' are allowed)",
        field.as_str()
    )]
    InvalidNameChars { field: NameField, name: String },
    #[error("{} '{}' contains a '..' segment which is not allowed", field.as_str(), path.display())]
    PathTraversal { field: PathField, path: PathBuf },
}

/// Validates that a name is safe to use as a filesystem path segment.
///
/// Only ASCII alphanumerics, `_`, and `-` are accepted. Rejects empty
/// strings and anything that could escape a parent directory (`/`, `\`,
/// `..`, leading-slash absolutes, NUL, whitespace, Unicode look-alikes).
pub fn validate_name(field: NameField, name: &str) -> Result<(), Error> {
    if name.is_empty() {
        return Err(Error::EmptyName { field });
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(Error::InvalidNameChars {
            field,
            name: name.to_string(),
        });
    }
    Ok(())
}

/// Validates that an operator-supplied path is safe to use as a working
/// directory.
///
/// Rejects paths containing `..` segments. Absolute paths are accepted
/// because overriding to a stable persistent volume is a legitimate use
/// case; traversal segments never are.
pub fn validate_path(field: PathField, path: &Path) -> Result<(), Error> {
    use std::path::Component;
    for component in path.components() {
        if matches!(component, Component::ParentDir) {
            return Err(Error::PathTraversal {
                field,
                path: path.to_path_buf(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_accepts_valid() {
        assert!(validate_name(NameField::Flow, "my_flow-1").is_ok());
        assert!(validate_name(NameField::Task, "TaskName").is_ok());
    }

    #[test]
    fn name_rejects_empty() {
        assert!(matches!(
            validate_name(NameField::Flow, ""),
            Err(Error::EmptyName {
                field: NameField::Flow
            })
        ));
    }

    #[test]
    fn name_rejects_traversal() {
        assert!(matches!(
            validate_name(NameField::Flow, ".."),
            Err(Error::InvalidNameChars { .. })
        ));
        assert!(validate_name(NameField::Flow, "../etc").is_err());
    }

    #[test]
    fn name_rejects_separators() {
        assert!(validate_name(NameField::Flow, "a/b").is_err());
        assert!(validate_name(NameField::Flow, "a\\b").is_err());
    }

    #[test]
    fn name_rejects_dot_segment() {
        assert!(validate_name(NameField::Flow, ".").is_err());
        assert!(validate_name(NameField::Flow, "a.b").is_err());
    }

    #[test]
    fn path_accepts_normal() {
        assert!(validate_path(PathField("clone_path"), &PathBuf::from("/var/tmp/repo")).is_ok());
        assert!(validate_path(PathField("clone_path"), &PathBuf::from("repos/x")).is_ok());
    }

    #[test]
    fn path_rejects_traversal() {
        assert!(matches!(
            validate_path(PathField("clone_path"), &PathBuf::from("/tmp/../etc")),
            Err(Error::PathTraversal {
                field: PathField("clone_path"),
                ..
            })
        ));
        assert!(validate_path(PathField("clone_path"), &PathBuf::from("../escape")).is_err());
    }
}
