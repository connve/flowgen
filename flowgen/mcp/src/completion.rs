//! Shared completion-source primitives used by `mcp_prompt` argument
//! completion and `mcp_resource` URI-template parameter completion.

use serde::{Deserialize, Serialize};

/// Source of completion values for an argument or URI-template parameter.
///
/// # Inline
/// ```yaml
/// completion:
///   values: [python, pytorch, pyside]
/// ```
///
/// # From loader-backed resource (one value per line)
/// ```yaml
/// completion:
///   resource: "completions/languages.txt"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, untagged)]
pub enum Completion {
    /// Statically-declared list of candidate values.
    Values { values: Vec<String> },
    /// Path to a loader-backed file; one value per line. Blank lines
    /// and lines beginning with `#` are ignored.
    Resource { resource: String },
}

/// Parses one-value-per-line completion resource content. Blank lines
/// and lines beginning with `#` are ignored; leading and trailing
/// whitespace on remaining lines is trimmed.
pub fn parse_completion_lines(content: &str) -> Vec<String> {
    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_strips_comments_and_blanks() {
        let content = "# header\n\npython\n  rust  \n\n# footer\nzig\n";
        assert_eq!(
            parse_completion_lines(content),
            vec!["python".to_string(), "rust".to_string(), "zig".to_string()],
        );
    }

    #[test]
    fn parse_empty_input() {
        assert!(parse_completion_lines("").is_empty());
    }

    #[test]
    fn parse_only_comments() {
        assert!(parse_completion_lines("# a\n# b\n").is_empty());
    }

    #[test]
    fn values_variant_deserializes() {
        let json = r#"{"values":["a","b"]}"#;
        match serde_json::from_str::<Completion>(json).unwrap() {
            Completion::Values { values } => {
                assert_eq!(values, vec!["a".to_string(), "b".to_string()]);
            }
            other => panic!("expected Values, got {other:?}"),
        }
    }

    #[test]
    fn resource_variant_deserializes() {
        let json = r#"{"resource":"completions/x.txt"}"#;
        match serde_json::from_str::<Completion>(json).unwrap() {
            Completion::Resource { resource } => assert_eq!(resource, "completions/x.txt"),
            other => panic!("expected Resource, got {other:?}"),
        }
    }
}
