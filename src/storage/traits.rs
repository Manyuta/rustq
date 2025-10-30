use crate::{Job, JobStatus, Result};
use async_trait::async_trait;

#[async_trait]
pub trait JobStorage: Send + Sync {
    /// Store a new job
    async fn store_job(&self, job: &Job) -> Result<()>;

    /// Retrieve a job by ID
    async fn get_job(&self, job_id: &str) -> Result<Option<Job>>;

    /// Get the current status of a job
    async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>>;

    /// Update a job's status
    async fn update_job_status(&self, job_id: &str, status: JobStatus) -> Result<()>;

    /// Get all registered job types (TODO: create monitoring tool)
    async fn get_job_types(&self) -> Result<Vec<String>>;

    /// Get count of jobs by status (TODO: create monitoring tool)
    async fn get_job_count_by_status(&self, status: JobStatus) -> Result<usize>;

    /// Check if storage is healthy
    async fn health_check(&self) -> Result<bool>;
}
