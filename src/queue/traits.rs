use crate::{Job, Result};
use async_trait::async_trait;
use lapin::Consumer;

#[async_trait]
pub trait JobQueue: Send + Sync {
    /// Push a job to the specified queue
    async fn push(&self, job: &Job, queue_name: &str) -> Result<()>;

    /// Push a job to a retry queue with specified delay
    async fn push_retry(&self, job: &Job, queue_name: &str, delay_ms: u32) -> Result<()>;

    /// Get a consumer for the specified queue
    async fn get_consumer(&self, queue_name: &str, consumer_tag: &str) -> Result<Consumer>;

    /// Check if the queue connection is healthy
    async fn health_check(&self) -> Result<bool>;
}
