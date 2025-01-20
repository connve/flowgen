use std::str::FromStr;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error parsing a given type.")]
    SerdeJson(#[source] serde_json::Error),
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
        let string = serde_json::to_string(self).map_err(Error::SerdeJson)?;
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
        let value = serde_json::Value::from_str(self).map_err(Error::SerdeJson)?;
        Ok(value)
    }
}
