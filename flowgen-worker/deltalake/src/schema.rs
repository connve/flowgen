//! Utilities for normalizing Apache Arrow Schema data types.
use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

/// Default timezone setting for Timestamp columns.
const DEFAULT_TIMESTAMP_TIMEZONE: &str = "UTC";

/// Errors encountered during Schema normalization.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Wraps an Apache Arrow error.
    #[error("error with an Apache Arrow data")]
    Arrow(#[source] arrow::error::ArrowError),
}

/// Extends `Schema` functionality, particularly for data type normalization.
pub trait SchemaExt {
    /// The error type for operations in this trait.
    type Error;

    /// Consumes the schema, normalizes its fields, and returns a new schema.
    ///
    /// Normalizes timestamp precision and removes null columns.
    fn normalize(self) -> Result<Schema, Self::Error>;
}

/// Implements `SchemaExt` for `arrow::datatypes::Schema`.
impl SchemaExt for Schema {
    type Error = Error;

    /// Consumes the original `Schema` and returns a new one with normalized data types.
    ///
    /// This method iterates through all fields of the consumed schema:
    /// - If a field is `Timestamp(Millisecond, ...)`, its data type is changed to `Timestamp(Microsecond, ...)`.
    /// - Fields with `DataType::Null` are removed from the schema.
    /// - Other field data types are preserved.
    /// - Field names, nullability, and metadata are retained for all remaining fields.
    /// - The original schema's top-level metadata is also preserved in the new schema.
    ///
    /// # Errors
    /// Adheres to the trait's error signature. As implemented, direct operations
    /// (field and schema construction from existing valid parts) are unlikely to
    /// produce an error. The `Result` type is maintained for trait compatibility and
    /// potential future extensions that might introduce error conditions.
    fn normalize(self) -> Result<Schema, Self::Error> {
        // Transform fields: normalize timestamp precision, remove null columns, preserve others.
        let normalized_fields: Vec<Field> = self
            .fields()
            .iter()
            .filter_map(|field| {
                // Skip null columns entirely
                if matches!(field.data_type(), DataType::Null) {
                    return None;
                }

                // `field` here is &Arc<Field> or &Field depending on Arrow version's iter details
                let new_data_type = match field.data_type() {
                    DataType::Timestamp(TimeUnit::Millisecond, _) => DataType::Timestamp(
                        TimeUnit::Microsecond,
                        Some(DEFAULT_TIMESTAMP_TIMEZONE.to_string().into()),
                    ),
                    other => other.clone(),
                };

                Some(
                    Field::new(field.name(), new_data_type, field.is_nullable())
                        .with_metadata(field.metadata().clone()),
                )
            })
            .collect();

        // Construct the new schema with normalized fields and original top-level metadata.
        let new_schema = Schema::new(normalized_fields).with_metadata(self.metadata().clone());
        Ok(new_schema)
    }
}
