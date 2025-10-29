use crate::{JobQueueError, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub job_type: String,
    pub payload: Vec<u8>,
    pub status: JobStatus,
    //pub priority: i32,
    pub created_at: i64, // unix timestamp when job was first submitted
    pub updated_at: i64, // when job status was last updated
    pub retry_count: u32,
    pub max_retries: u32,
    pub last_error: Option<String>,
    pub backoff: BackoffStrategy,
}

impl Job {
    pub fn new(job_type: String, payload: Vec<u8>, max_retries: u32) -> Self {
        let now = OffsetDateTime::now_utc().unix_timestamp();

        Self {
            id: Uuid::new_v4().to_string(),
            job_type,
            payload,
            status: JobStatus::Pending,
            //priority,
            created_at: now,
            updated_at: now,
            retry_count: 0,
            max_retries,
            last_error: None,
            backoff: BackoffStrategy::default(),
        }
    }

    pub fn with_retry(mut self, error: Option<String>) -> Self {
        self.retry_count += 1;
        self.last_error = error;
        self.updated_at = OffsetDateTime::now_utc().unix_timestamp();
        self
    }

    pub fn can_retry(&self) -> bool {
        self.retry_count < self.max_retries
    }

    pub fn get_retry_delay(&self) -> Duration {
        self.backoff.calculate_delay(self.retry_count)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JobStatus {
    Pending,
    Processing,
    Completed,
    Failed,
    Cancelled,
}

impl std::str::FromStr for JobStatus {
    type Err = JobQueueError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(JobStatus::Pending),
            "processing" => Ok(JobStatus::Processing),
            "completed" => Ok(JobStatus::Completed),
            "failed" => Ok(JobStatus::Failed),
            "cancelled" => Ok(JobStatus::Cancelled),
            _ => Err(JobQueueError::DeserializationError(format!(
                "Invalid job status: {}",
                s
            ))),
        }
    }
}

impl From<JobStatus> for String {
    fn from(status: JobStatus) -> Self {
        match status {
            JobStatus::Pending => "pending".to_string(),
            JobStatus::Processing => "processing".to_string(),
            JobStatus::Completed => "completed".to_string(),
            JobStatus::Failed => "failed".to_string(),
            JobStatus::Cancelled => "cancelled".to_string(),
        }
    }
}

impl From<String> for JobStatus {
    fn from(status: String) -> Self {
        match status.to_lowercase().as_str() {
            "pending" => JobStatus::Pending,
            "processing" => JobStatus::Processing,
            "completed" => JobStatus::Completed,
            "failed" => JobStatus::Failed,
            "cancelled" => JobStatus::Cancelled,
            _ => JobStatus::Pending, // Default fallback
        }
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status_str = match self {
            JobStatus::Pending => "Pending",
            JobStatus::Processing => "Processing",
            JobStatus::Completed => "Completed",
            JobStatus::Failed => "Failed",
            JobStatus::Cancelled => "Cancelled",
        };
        write!(f, "{}", status_str)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackoffStrategy {
    Fixed(Duration),
    Exponential(Duration),
    Custom(Vec<Duration>),
}

impl Default for BackoffStrategy {
    fn default() -> Self {
        BackoffStrategy::Exponential(Duration::from_secs(1))
    }
}

impl BackoffStrategy {
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        match self {
            BackoffStrategy::Fixed(duration) => *duration,
            BackoffStrategy::Exponential(base) => {
                let multiplier = 2u32.pow(attempt);
                *base * multiplier
            }
            BackoffStrategy::Custom(delays) => {
                let index = (attempt as usize).min(delays.len() - 1);
                delays[index]
            }
        }
    }
}

impl BackoffStrategy {
    pub fn fixed(seconds: u64) -> Self {
        Self::Fixed(Duration::from_secs(seconds))
    }

    pub fn exponential(seconds: u64) -> Self {
        Self::Exponential(Duration::from_secs(seconds))
    }

    pub fn custom(delays: Vec<u64>) -> Self {
        Self::Custom(delays.into_iter().map(Duration::from_secs).collect())
    }
}

#[derive(Debug, Clone)]
pub struct JobOptions {
    pub delay: Option<Duration>,
    pub priority: i32,
    pub attempts: u32,
    pub backoff: BackoffOptions,
    pub max_retries: u32,
}

impl Default for JobOptions {
    fn default() -> Self {
        Self {
            delay: None,
            priority: 1,
            attempts: 3,
            backoff: BackoffOptions::Fixed(Duration::from_secs(5)),
            max_retries: 3,
        }
    }
}

#[derive(Debug, Clone)]
pub enum BackoffOptions {
    Fixed(Duration),
    Exponential(Duration),
}
