//! Base trait for all task execution implementations.
//!
//! Defines the common interface that all flowgen task types must implement
//! to participate in the workflow execution pipeline.

/// Trait for executing workflow tasks asynchronously.
///
/// All task implementations (subscribers, publishers, processors) must implement
/// this trait to provide a standardized execution interface for the flowgen runtime.
#[async_trait::async_trait]
pub trait Runner {
    /// Error type for task execution failures.
    type Error;

    /// Event handler type for processing events.
    type EventHandler: Send;

    /// Initializes the task by setting up connections, authentication, and other resources.
    ///
    /// This method performs all setup operations that can fail, allowing errors to be
    /// logged with proper span context before the task enters its main event loop.
    ///
    /// # Returns
    /// The initialized event handler or an error if setup fails
    async fn init(&self) -> Result<Self::EventHandler, Self::Error>;

    /// Executes the task until completion or error.
    ///
    /// # Returns
    /// Success or an error if the task execution fails
    async fn run(self) -> Result<(), Self::Error>
    where
        Self: Sized;
}
