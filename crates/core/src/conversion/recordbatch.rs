use std::{io::BufReader, sync::Arc};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("error with an Apache Arrow data")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("no data available in the buffer")]
    EmptyBuffer(),
}
pub trait RecordBatchExt {
    type Error;
    fn to_recordbatch(&self) -> Result<arrow::array::RecordBatch, Self::Error>;
}

impl RecordBatchExt for String {
    type Error = Error;
    fn to_recordbatch(&self) -> Result<arrow::array::RecordBatch, Self::Error> {
        let reader = BufReader::new(self.as_bytes());
        let (schema, _) =
            arrow_json::reader::infer_json_schema(reader, None).map_err(Error::Arrow)?;

        let reader = BufReader::new(self.as_bytes());
        let reader_result = arrow_json::ReaderBuilder::new(Arc::new(schema))
            .build(reader)
            .map_err(Error::Arrow)?
            .next()
            .ok_or_else(Error::EmptyBuffer)?;

        let recordbatch = reader_result.map_err(Error::Arrow)?;
        Ok(recordbatch)
    }
}
