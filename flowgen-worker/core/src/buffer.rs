//! Buffer handling utilities and types.
//!
//! Provides abstractions for reading and writing different content formats and converting
//! them to/from EventData variants.
use std::io::{Read, Seek, Write};

/// Supported content types with their specific configuration options.
#[derive(Debug, Clone)]
pub enum ContentType {
    /// JSON content format.
    Json,
    /// CSV content format with batch size and header configuration.
    Csv {
        /// Number of records to process in each batch.
        batch_size: usize,
        /// Whether the CSV content has a header row.
        has_header: bool,
        /// CSV delimiter character (defaults to comma if not specified).
        delimiter: Option<u8>,
    },
    /// Apache Avro content format.
    Avro,
}

/// Trait for converting readers to EventData based on content type.
pub trait FromReader<R: Read + Seek> {
    /// Error type for conversion operations.
    type Error;

    /// Converts a reader to a vector of EventData based on the specified content type.
    ///
    /// # Arguments
    /// * `reader` - The reader to consume data from
    /// * `content_type` - The type of content and its configuration
    ///
    /// # Returns
    /// Vector of EventData instances parsed from the reader
    fn from_reader(reader: R, content_type: ContentType) -> Result<Vec<Self>, Self::Error>
    where
        Self: Sized;
}

/// Trait for converting EventData to writers.
pub trait ToWriter<W: Write> {
    /// Error type for conversion operations.
    type Error;

    /// Converts EventData to a writer using the data's native format.
    ///
    /// # Arguments
    /// * `writer` - The writer to output data to
    ///
    /// # Returns
    /// Result indicating success or failure
    fn to_writer(self, writer: W) -> Result<(), Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_type_debug() {
        let json_type = ContentType::Json;
        assert_eq!(format!("{json_type:?}"), "Json");

        let csv_type = ContentType::Csv {
            batch_size: 100,
            has_header: true,
            delimiter: None,
        };
        assert_eq!(
            format!("{csv_type:?}"),
            "Csv { batch_size: 100, has_header: true, delimiter: None }"
        );

        let avro_type = ContentType::Avro;
        assert_eq!(format!("{avro_type:?}"), "Avro");
    }

    #[test]
    fn test_content_type_clone() {
        let csv_type = ContentType::Csv {
            batch_size: 50,
            has_header: false,
            delimiter: Some(b';'),
        };
        let cloned = csv_type.clone();

        match cloned {
            ContentType::Csv {
                batch_size,
                has_header,
                delimiter,
            } => {
                assert_eq!(batch_size, 50);
                assert!(!has_header);
                assert_eq!(delimiter, Some(b';'));
            }
            _ => panic!("Clone should preserve variant"),
        }
    }
}
