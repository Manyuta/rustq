use crate::{Job, JobQueueError, JobStatus, JobStorage, Result};
use async_trait::async_trait;
use deadpool_redis::{Config, Connection, Pool, Runtime};
use redis::AsyncCommands;

#[derive(Clone, Debug)]
pub struct RedisStorage {
    pool: Pool,
}

impl RedisStorage {
    pub fn new(redis_url: &str) -> Result<Self> {
        let cfg = Config::from_url(redis_url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| JobQueueError::StorageError(e.to_string()))?;

        Ok(Self { pool })
    }

    fn job_data_key(&self, job_id: &str) -> String {
        format!("job:data:{}", job_id)
    }

    fn job_status_key(&self, job_id: &str) -> String {
        format!("job:status:{}", job_id)
    }

    fn job_types_key(&self) -> String {
        "job:types".to_string()
    }

    fn job_status_set_key(&self, status: JobStatus) -> String {
        format!("jobs:{}", status)
    }

    fn jobs_by_created_key(&self) -> String {
        "jobs:by_created".to_string()
    }

    async fn get_conn(&self) -> Result<Connection> {
        self.pool.get().await.map_err(|e| {
            JobQueueError::StorageError(format!("Failed to get Redis connection: {}", e))
        })
    }

    async fn update_job_indexes(
        &self,
        job: &Job,
        old_status: Option<JobStatus>,
        new_status: JobStatus,
    ) -> Result<()> {
        let mut conn = self.get_conn().await?;

        let mut pipe = redis::pipe();

        // Add to status set
        let new_status_set = self.job_status_set_key(new_status);
        pipe.sadd(&new_status_set, &job.id);

        // Remove from old status set if exists
        if let Some(old_status) = old_status.as_ref() {
            let old_status_set = self.job_status_set_key(old_status.clone());
            pipe.srem(&old_status_set, &job.id);
        }

        // Add to sorted set by creation time (only if new job)
        if old_status.is_none() {
            pipe.zadd(self.jobs_by_created_key(), &job.id, job.created_at);
        }

        // Add job type
        pipe.sadd(self.job_types_key(), &job.job_type);

        let _: () = pipe
            .query_async(&mut conn)
            .await
            .map_err(|e| JobQueueError::StorageError(e.to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl JobStorage for RedisStorage {
    async fn store_job(&self, job: &Job) -> Result<()> {
        let mut conn = self.get_conn().await?;

        let serialized = serde_json::to_string(job)
            .map_err(|e| JobQueueError::SerializationError(e.to_string()))?;

        let mut pipe = redis::pipe();

        // Store job data
        pipe.set(self.job_data_key(&job.id), &serialized);

        // Set initial status
        pipe.set(self.job_status_key(&job.id), JobStatus::Pending.to_string());

        let _: () = pipe
            .query_async(&mut conn)
            .await
            .map_err(|e| JobQueueError::StorageError(e.to_string()))?;

        // Update indexes
        self.update_job_indexes(job, None, JobStatus::Pending)
            .await?;

        Ok(())
    }

    async fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        let mut conn = self.get_conn().await?;

        let data: Option<String> = conn
            .get(self.job_data_key(job_id))
            .await
            .map_err(|e| JobQueueError::StorageError(e.to_string()))?;

        match data {
            Some(json) => {
                let job: Job = serde_json::from_str(&json)
                    .map_err(|e| JobQueueError::DeserializationError(e.to_string()))?;
                Ok(Some(job))
            }
            None => Ok(None),
        }
    }

    async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        let mut conn = self.get_conn().await?;

        let status_opt: Option<String> = conn
            .get(self.job_status_key(job_id))
            .await
            .map_err(|e| JobQueueError::StorageError(e.to_string()))?;

        match status_opt {
            Some(status) => Ok(Some(status.into())),
            None => Ok(None),
        }
    }

    async fn update_job_status(&self, job_id: &str, new_status: JobStatus) -> Result<()> {
        let mut conn = self.get_conn().await?;

        // Get current status first
        let old_status = self.get_job_status(job_id).await?;

        // Update status
        let _: () = conn
            .set(self.job_status_key(job_id), new_status.to_string())
            .await
            .map_err(|e| JobQueueError::StorageError(e.to_string()))?;

        // Update indexes if job exists and status changed
        if let (Some(old_status), Some(job)) = (old_status, self.get_job(job_id).await?)
            && old_status != new_status
        {
            self.update_job_indexes(&job, Some(old_status), new_status)
                .await?;
        }

        Ok(())
    }

    async fn health_check(&self) -> Result<bool> {
        let mut conn = self.get_conn().await?;

        let _: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| JobQueueError::StorageError(e.to_string()))?;

        Ok(true)
    }

    async fn get_job_types(&self) -> Result<Vec<String>> {
        let mut conn = self.get_conn().await?;
        let job_types: Vec<String> = conn
            .smembers(self.job_types_key())
            .await
            .map_err(|e| JobQueueError::StorageError(e.to_string()))?;
        Ok(job_types)
    }

    async fn get_job_count_by_status(&self, status: JobStatus) -> Result<usize> {
        let mut conn = self.get_conn().await?;
        let count: usize = conn
            .scard(self.job_status_set_key(status))
            .await
            .map_err(|e| JobQueueError::StorageError(e.to_string()))?;
        Ok(count)
    }
}
