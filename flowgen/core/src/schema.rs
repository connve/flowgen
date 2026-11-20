use schemars::{json_schema, JsonSchema, Schema, SchemaGenerator};
use std::borrow::Cow;

/// The schema of a `Duration` read with `humantime_serde`: a string such as
/// `30s`, `5m` or `1h`. A field takes it with
/// `#[schemars(with = "flowgen_core::schema::Duration")]`.
pub struct Duration;

impl JsonSchema for Duration {
    fn schema_name() -> Cow<'static, str> {
        "Duration".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "format": "duration",
            "examples": ["30s", "5m", "1h"]
        })
    }
}
