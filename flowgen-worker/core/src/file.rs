//! File handling utilities and types.
//!
//! Provides abstractions for reading different file formats and converting
//! them to EventData variants.
use std::io::{Read, Seek};

/// Supported file types with their specific configuration options.
#[derive(Debug, Clone)]
pub enum FileType {
    /// JSON file format.
    Json,
    /// CSV file format with batch size and header configuration.
    Csv {
        /// Number of records to process in each batch.
        batch_size: usize,
        /// Whether the CSV file has a header row.
        has_header: bool,
    },
    /// Apache Avro file format.
    Avro,
}

/// Trait for converting readers to EventData based on file type.
pub trait FromReader<R: Read + Seek> {
    /// Error type for conversion operations.
    type Error;

    /// Converts a reader to a vector of EventData based on the specified file type.
    ///
    /// # Arguments
    /// * `reader` - The reader to consume data from
    /// * `file_type` - The type of file and its configuration
    ///
    /// # Returns
    /// Vector of EventData instances parsed from the reader
    fn from_reader(reader: R, file_type: FileType) -> Result<Vec<Self>, Self::Error>
    where
        Self: Sized;
}
