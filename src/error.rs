use thiserror::Error;

impl LibError {
    pub fn component<S: Into<String>>(component_name: S, error: impl Into<ComponentError>) -> Self {
        LibError::Component {
            component: component_name.into(),
            source: error.into(),
        }
    }

    pub fn config<S: Into<String>>(message: S) -> Self {
        LibError::Config(message.into())
    }

    pub fn unknown<S: Into<String>>(message: S) -> Self {
        LibError::Unknown(message.into())
    }
}

impl From<String> for ComponentError {
    fn from(s: String) -> Self {
        ComponentError::Other(s)
    }
}

impl From<&str> for ComponentError {
    fn from(s: &str) -> Self {
        ComponentError::Other(s.to_string())
    }
}

#[derive(Error, Debug, Clone)]
pub enum LibError {
    #[error("Component error in '{component}': {source}")]
    Component {
        component: String,
        #[source]
        source: ComponentError,
    },

    #[error("Processing error: {0}")]
    Processing(#[from] ProcessingError),

    #[error("I/O error: {0}")]
    IO(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Timeout error after {0:?}")]
    Timeout(std::time::Duration),

    #[error("Circuit breaker for '{0}' is open")]
    CircuitOpen(String),

    #[error("Task was cancelled")]
    Cancelled,

    #[error("Worker thread panicked: {0}")]
    WorkerPanic(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

#[derive(Error, Debug, Clone)]
pub enum ComponentError {
    #[error("Processing failed: {0}")]
    ProcessingFailed(String),

    #[error("Resource unavailable: {0}")]
    ResourceUnavailable(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Other component error: {0}")]
    Other(String),
}

#[derive(Error, Debug, Clone)]
pub enum ProcessingError {
    #[error("Failed to process batch: {0}")]
    BatchFailed(String),

    #[error("Pipeline error: {0}")]
    Pipeline(String),

    #[error("Stage error: {0}")]
    Stage(String),

    #[error("Worker error: {0}")]
    Worker(String),

    #[error("Pool error: {0}")]
    Pool(String),
}
