use std::str::FromStr;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
}
pub trait MapExt {
    type Error;
    fn to_string(&self) -> Result<String, Self::Error>;
}

impl<K, V> MapExt for serde_json::Map<K, V>
where
    serde_json::Map<K, V>: serde::Serialize,
{
    type Error = Error;
    fn to_string(&self) -> Result<String, Self::Error> {
        let string = serde_json::to_string(self).map_err(Error::Serde)?;
        Ok(string)
    }
}

pub trait StringExt {
    type Error;
    fn to_value(&self) -> Result<serde_json::Value, Self::Error>;
}

impl StringExt for String {
    type Error = Error;
    fn to_value(&self) -> Result<serde_json::Value, Self::Error> {
        let value = serde_json::Value::from_str(self).map_err(Error::Serde)?;
        Ok(value)
    }
}

pub trait SerdeValueExt {
    type Error;
    fn try_from(&self) -> Result<serde_json::Value, Self::Error>;
}

impl SerdeValueExt for arrow::record_batch::RecordBatch {
    type Error = Error;
    fn try_from(&self) -> Result<serde_json::Value, Self::Error> {
        let buf = Vec::new();
        let mut writer = arrow_json::ArrayWriter::new(buf);
        writer.write_batches(&[self]).map_err(Error::Arrow)?;
        writer.finish().map_err(Error::Arrow)?;
        let json_data = writer.into_inner();

        use serde_json::{Map, Value};
        let json_rows: Vec<Map<String, Value>> =
            serde_json::from_reader(json_data.as_slice()).map_err(Error::Serde)?;
        Ok(json_rows.into())
    }
}
