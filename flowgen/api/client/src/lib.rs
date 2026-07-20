//! Generated HTTP client for the flowgen API. See `openapi.yaml`.

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

/// Raw OpenAPI spec bundled with this crate. Served verbatim on
/// `GET /api/openapi.yaml`.
pub const OPENAPI_YAML: &str = include_str!("../openapi.yaml");
