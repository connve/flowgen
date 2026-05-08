//! Buffer handling utilities and types.
//!
//! Provides centralized format parsing for reading and writing different content
//! formats. The `FromReader` trait streams parsed items one at a time via a
//! callback, so callers never collect all items in memory.
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
        /// Maximum number of rows to sample for schema inference.
        /// None means scan all rows for accurate type detection.
        /// Some(n) scans only first n rows for faster inference on large files.
        infer_schema_max_records: Option<usize>,
    },
    /// Apache Avro content format.
    Avro,
    /// Apache Parquet columnar format with batch size configuration.
    Parquet {
        /// Number of records to process in each batch.
        batch_size: usize,
    },
}

/// Boxed iterator that yields parsed items one at a time.
pub type ReaderIter<T, E> = Box<dyn Iterator<Item = Result<T, E>> + Send>;

/// Returns a lazy iterator that yields parsed items one at a time from a reader.
///
/// Only one parsed item lives in memory at a time alongside the raw bytes.
/// Callers drive the iterator at their own pace and can perform async work
/// (such as sending events downstream) between iterations.
pub trait FromReader<R: Read + Seek> {
    /// Error type for conversion operations.
    type Error;

    /// Parses a reader according to the content type, returning a lazy iterator.
    fn from_reader(
        reader: R,
        content_type: ContentType,
    ) -> Result<ReaderIter<Self, Self::Error>, Self::Error>
    where
        Self: Sized;
}

/// Trait for converting EventData to writers.
pub trait ToWriter<W: Write> {
    /// Error type for conversion operations.
    type Error;

    /// Converts EventData to a writer using the data's native format.
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
            infer_schema_max_records: None,
        };
        assert_eq!(
            format!("{csv_type:?}"),
            "Csv { batch_size: 100, has_header: true, delimiter: None, infer_schema_max_records: None }"
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
            infer_schema_max_records: None,
        };
        let cloned = csv_type.clone();

        match cloned {
            ContentType::Csv {
                batch_size,
                has_header,
                delimiter,
                infer_schema_max_records,
            } => {
                assert_eq!(batch_size, 50);
                assert!(!has_header);
                assert_eq!(delimiter, Some(b';'));
                assert_eq!(infer_schema_max_records, None);
            }
            _ => panic!("Clone should preserve variant"),
        }
    }
}
