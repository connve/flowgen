pub trait Publisher {
    type Error;
    fn publish(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send
    where
        Self: Sized;
}
