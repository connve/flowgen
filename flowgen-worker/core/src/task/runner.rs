//! Base trait for all task execution implementations.
//!
//! Defines the common interface that all flowgen task types must implement
//! to participate in the workflow execution pipeline.

/// Trait for executing workflow tasks asynchronously.
///
/// All task implementations (subscribers, publishers, processors) must implement
/// this trait to provide a standardized execution interface for the flowgen runtime.
pub trait Runner {
    /// Error type for task execution failures.
    type Error;
    
    /// Executes the task until completion or error.
    ///
    /// # Returns
    /// Success or an error if the task execution fails
    fn run(self) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send
    where
        Self: Sized;
}
