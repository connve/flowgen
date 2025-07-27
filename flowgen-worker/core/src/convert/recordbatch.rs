use std::{io::BufReader, sync::Arc};


#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
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
        let recordbatch = match arrow_json::reader::infer_json_schema(reader, None) {
            Ok((schema, _)) => {
                // Use inferred schema and parse json.
                let reader = BufReader::new(self.as_bytes());
                let reader_result = arrow_json::ReaderBuilder::new(Arc::new(schema.clone()))
                    .build(reader)
                    .map_err(Error::Arrow)?
                    .next()
                    .ok_or_else(Error::EmptyBuffer)?;

                reader_result.map_err(Error::Arrow)?
            }
            Err(_) => {
                // Create fallback schema and record batch with entire string as content.
                let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                    "content",
                    arrow::datatypes::DataType::Utf8,
                    false,
                )]);
                let string_array = arrow::array::StringArray::from(vec![self.as_str()]);

                arrow::array::RecordBatch::try_new(
                    Arc::new(schema.clone()),
                    vec![Arc::new(string_array)],
                )
                .map_err(Error::Arrow)?
            }
        };

        Ok(recordbatch)
    }
}

