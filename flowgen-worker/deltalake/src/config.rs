use flowgen_core::cache::CacheOptions;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration for the data writer component.
///
/// This struct defines the necessary parameters to configure how data
/// should be written to a target destination. It includes credentials,
/// the target path, the write operation mode, an optional predicate
/// for merge operations, and parameters controlling optional target creation.
///
/// # Example: Append Operation (Implicitly No Creation)
///
/// Note: `create_options` is mandatory, but creation depends on its internal flags.
/// Setting `create_if_not_exist: false` disables creation attempts.
///
/// ```json
/// {
///     "writer": {
///         "credentials": "my_secret_credentials",
///         "path": "/path/to/target/data",
///         "operation": "Append",
///         "create_options": {
///             "create_if_not_exist": false
///         }
///     }
/// }
/// ```
///
/// # Example: Merge Operation with Creation Enabled
///
/// ```json
/// {
///     "writer": {
///         "credentials": "connection_string_or_token",
///         "path": "database/schema/table_name",
///         "operation": "Merge",
///         "predicate": "target.id = source.id",
///         "create_options": {
///             "create_if_not_exist": true,
///             "columns": [
///                 {"name": "id", "data_type": "Utf8", "nullable": false},
///                 {"name": "value", "data_type": "Utf8", "nullable": true},
///                 {"name": "timestamp", "data_type": "Utf8", "nullable": false}
///             ]
///         }
///     }
/// }
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Writer {
    /// Credentials required for accessing the target data store or system.
    /// The specific format depends on the target (e.g., connection string, token, etc.).
    pub credentials: String,
    /// Path identifying the target location (e.g., file path, table identifier).
    pub path: PathBuf,
    /// The writing operation to perform. See `Operation` enum.
    pub operation: Operation,
    /// An optional condition used primarily for the `Merge` operation.
    /// This string typically defines how source and target records are matched
    /// (e.g., `"target.id = source.id"`).
    pub predicate: Option<String>,
    /// Parameters controlling the optional creation of the target resource (e.g., a table)
    /// if it does not already exist. See `CreateOptions`. Note that this field itself is mandatory.
    pub create_options: CreateOptions,
}

/// Defines the properties of a single column, typically used for schema definition.
///
/// Part of `CreateOptions` to specify the structure of a target table or dataset
/// when it needs to be created.
///
/// # Example:
///
/// ```json
/// {
///     "name": "user_email",
///     "data_type": "Utf8",
///     "nullable": true
/// }
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Column {
    /// The name of the column.
    pub name: String,
    /// The data type of the column. See `DataType` enum.
    pub data_type: DataType,
    /// Indicates whether the column can contain null (missing) values.
    pub nullable: bool,
}

/// Specifies the data type for a column.
///
/// Currently, only a limited set of types might be defined.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum DataType {
    #[default]
    /// UTF-8 encoded string of characters.
    String,
    /// i64: 8-byte signed integer. Range: -9223372036854775808 to 9223372036854775807.
    Long,
    /// i32: 4-byte signed integer. Range: -2147483648 to 2147483647.
    Integer,
    /// i16: 2-byte signed integer numbers. Range: -32768 to 32767.
    Short,
    /// i8: 1-byte signed integer number. Range: -128 to 127.
    Byte,
    /// f32: 4-byte single-precision floating-point numbers.
    Float,
    /// f64: 8-byte double-precision floating-point numbers.
    Double,
    /// bool: boolean values.
    Boolean,
    /// Sequence of bytes.
    Binary,
    /// Date32: 4-byte date .
    Date,
    /// Microsecond precision timestamp, adjusted to UTC.
    Timestamp,
    // Microsecond precision timestamp, no time zone.
    // This is currently not supported accross Delta Lake environments.
    // TimestampNtz,
}

/// Defines the write strategy or operation mode for the writer.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum Operation {
    /// Appends new data to the target. If the target is a table,
    /// new rows are added. If it's a file, data is appended to the end. (Default)
    #[default]
    Append,
    /// Merges new data with existing data in the target based on a predicate.
    /// This typically involves updating existing records that match the predicate
    /// and inserting new records that don't match. Requires `predicate` to be set
    /// in the `Writer` configuration.
    Merge,
    // Add other potential operations here, e.g.:
    // Overwrite, Upsert, etc.
}

/// Options specifically controlling the creation of the target resource if it doesn't exist.
///
/// Used within the `Writer` configuration to define if and how a target
/// (e.g., a Delta table) should be created when it's not found at the specified path.
///
/// # Example: Enable creation with specific columns
///
/// ```json
/// {
///     "create_options": {
///         "create_if_not_exist": true,
///         "columns": [
///             {"name": "id", "data_type": "Utf8", "nullable": false},
///             {"name": "data", "data_type": "Utf8", "nullable": true}
///         ]
///     }
/// }
/// ```
///
/// # Example: Disable creation attempt
///
/// ```json
/// {
///     "create_options": {
///         "create_if_not_exist": false
///         // "columns" might be omitted or null here
///     }
/// }
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct CreateOptions {
    /// Flag indicating whether the target resource should be created if it's not found.
    /// If `false`, no creation attempt will be made even if `columns` are provided.
    /// Defaults to `false` if not specified during deserialization (due to `Default` trait).
    pub create_if_not_exist: bool,
    /// An optional list of column definitions (`Column`) specifying the schema
    /// of the target to be created. This is required for creation if `create_if_not_exist` is `true`.
    /// If `create_if_not_exist` is `true` but `columns` is `None` or empty, creation might fail or be skipped.
    pub columns: Option<Vec<Column>>,
    /// Cache options i.e. whether the schema should be retrieved and a new schema added to cache.
    pub cache_options: Option<CacheOptions>,
}
