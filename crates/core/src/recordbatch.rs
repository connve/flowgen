use std::{io::BufReader, sync::Arc};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum RecordBatchError {
    #[error("There was an error with an Apache Arrow data.")]
    ArrowError(#[source] arrow::error::ArrowError),
    #[error("There is not data available in the buffer.")]
    EmptyBuffer(),
}
pub trait RecordBatchExt {
    type Error;
    fn to_recordbatch(&self) -> Result<arrow::array::RecordBatch, Self::Error>;
}

impl RecordBatchExt for String {
    type Error = RecordBatchError;
    fn to_recordbatch(&self) -> Result<arrow::array::RecordBatch, Self::Error> {
        let reader = BufReader::new(self.as_bytes());
        let (schema, _) = arrow_json::reader::infer_json_schema(reader, None)
            .map_err(RecordBatchError::ArrowError)?;

        let reader = BufReader::new(self.as_bytes());
        let reader_result = arrow_json::ReaderBuilder::new(Arc::new(schema))
            .build(reader)
            .map_err(RecordBatchError::ArrowError)?
            .next()
            .ok_or_else(RecordBatchError::EmptyBuffer)?;

        let recordbatch = reader_result.map_err(RecordBatchError::ArrowError)?;
        Ok(recordbatch)
    }
}
