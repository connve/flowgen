use tokio::task::JoinHandle;

pub trait Runner {
    type Error;
    fn run(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send
    where
        Self: Sized;
}

pub trait RunnerNew {
    type Error;
    fn run(self) -> impl std::future::Future<Output = JoinHandle<Result<(), Self::Error>>> + Send
    where
        Self: Sized;
}
