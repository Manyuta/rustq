use thiserror::Error;

#[derive(Debug, Error)]
pub enum JobQueueError {
    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Queue error: {0}")]
    QueueError(String),

    #[error("Handler error: {0}")]
    HandlerError(String),

    #[error("Job not found: {0}")]
    JobNotFound(String),

    #[error("Invalid job state: {0}")]
    InvalidJobState(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Worker error: {0}")]
    WorkerError(String),
}

pub type Result<T> = std::result::Result<T, JobQueueError>;

impl From<lapin::Error> for JobQueueError {
    fn from(value: lapin::Error) -> Self {
        JobQueueError::QueueError(value.to_string())
    }
}
