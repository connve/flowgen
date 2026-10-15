//! Generated HTTP client for the flowgen API. See `openapi.yaml`.

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

/// Raw OpenAPI spec bundled with this crate. Served verbatim on
/// `GET /api/openapi.yaml`.
pub const OPENAPI_YAML: &str = include_str!("../openapi.yaml");

#[cfg(test)]
mod tests {
    #[test]
    fn the_spec_web_ui_and_docs_carry_the_workspace_version() {
        let hint = "run .githooks/pre-commit to copy the version from Cargo.toml";
        let spec: serde_yaml::Value = serde_yaml::from_str(super::OPENAPI_YAML).unwrap();
        assert_eq!(
            spec["info"]["version"].as_str(),
            Some(env!("CARGO_PKG_VERSION")),
            "openapi.yaml: {hint}"
        );

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        for path in [
            "web/package.json",
            "web/package-lock.json",
            "docs/package.json",
            "docs/package-lock.json",
        ] {
            let file: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(root.join(path)).unwrap()).unwrap();
            assert_eq!(
                file["version"].as_str(),
                Some(env!("CARGO_PKG_VERSION")),
                "{path}: {hint}"
            );
        }
    }
}
