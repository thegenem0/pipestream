use std::error::Error;

#[derive(Debug, thiserror::Error)]
pub enum StageError {
    #[error("Pipeline component '{0}' failed: {1}")]
    ComponentError(String, Box<dyn Error + Send + Sync>),
    #[error("Pipeline error: {0}")]
    Other(String),
    #[error("Task cancelled")]
    Cancelled,
    #[error("Worker thread panicked")]
    WorkerPanic,
}
