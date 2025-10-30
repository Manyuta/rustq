//! A distributed job queue system with Redis storage and RabbitMQ backend.
//!
//! # Features
//!
//! - **Distributed Processing**: Scale horizontally across multiple workers
//! - **Persistent Storage**: Redis-backed job storage with efficient indexing
//! - **Reliable Queueing**: RabbitMQ with dead letter exchanges for retries
//! - **Flexible Backoff**: Configurable retry strategies (fixed, exponential, custom)
//! - **Async/Sync Support**: Handle both I/O-bound and CPU-bound tasks
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use rustq::{Queue, WorkerOptions, JobOptions, BackoffStrategy};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct EmailData {
//!     to: String,
//!     subject: String,
//!     body: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create a queue
//!     let queue = Queue::new(
//!         "emails",
//!         "redis://localhost:6379",
//!         "amqp://localhost:5672",
//!     ).await?;
//!
//!     // Add a job
//!     let job_id = queue.add(
//!         "send_email",
//!         EmailData {
//!             to: "user@example.com".to_string(),
//!             subject: "Welcome".to_string(),
//!             body: "Hello!".to_string(),
//!         },
//!         JobOptions::default(),
//!     ).await?;
//!
//!     // Create and run a worker
//!     let worker = queue.create_worker(WorkerOptions::default())?;
//!     
//!     worker.process_async("send_email", |data: EmailData| async move {
//!         println!("Sending email to: {}", data.to);
//!         Ok(())
//!     }).await?;
//!
//!     
//!     worker.run().await.unwrap();
//!     
//!
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod job;
pub mod queue;
pub mod queue_wrapper;
pub mod storage;
pub mod worker;

pub use error::{JobQueueError, Result};
pub use job::{BackoffStrategy, Job, JobOptions, JobStatus};
pub use queue::{JobQueue, RabbitMQQueue};
pub use queue_wrapper::*;
pub use storage::{JobStorage, RedisStorage};
pub use worker::*;
