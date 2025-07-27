use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct CacheOptions {
    /// Key to use when storing data in cache.
    pub insert_key: Option<String>,
    /// Key to use when retrieving data from cache.
    pub retrieve_key: Option<String>,
}

pub trait Cache: Debug + Send + Sync + 'static {
    type Error: Debug + Send + Sync + 'static;
    fn init(
        self,
        bucket: &str,
    ) -> impl std::future::Future<Output = Result<Self, Self::Error>> + Send
    where
        Self: Sized;
    fn put(
        &self,
        key: &str,
        value: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send
    where
        Self: Sized;
    fn get(
        &self,
        key: &str,
    ) -> impl std::future::Future<Output = Result<bytes::Bytes, Self::Error>> + Send
    where
        Self: Sized;
}
