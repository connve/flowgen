pub trait Runner {
    type Error;
    fn run(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send
    where
        Self: Sized;
}
