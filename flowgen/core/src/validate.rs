//! Validation of flow, task and resource names, which become path segments.

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
}
