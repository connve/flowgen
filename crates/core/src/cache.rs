use std::fmt::Debug;

pub trait Cache: Debug + Send + Sync + 'static {
    type Error: Debug + Send + Sync + 'static;
    fn put(&self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send
    where
        Self: Sized;
}
