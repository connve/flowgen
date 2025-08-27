pub trait Client {
    type Error;
    fn connect(self) -> impl std::future::Future<Output = Result<Self, Self::Error>> + Send
    where
        Self: Sized;
}
