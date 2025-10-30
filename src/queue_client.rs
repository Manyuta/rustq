use crate::{
    ConcurrentWorker, Job, JobOptions, JobQueue, JobQueueError, JobStorage, RabbitMQQueue,
    RedisStorage, Result,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Queue {
    name: String,
    queue: Arc<RabbitMQQueue>,
    storage: Arc<RedisStorage>,
}

impl Queue {
    pub async fn new(name: &str, redis_url: &str, rabbitmq_url: &str) -> Result<Self> {
        let queue = RabbitMQQueue::new(rabbitmq_url).await?;
        let storage = RedisStorage::new(redis_url)?;

        Ok(Self {
            name: name.to_string(),
            queue: Arc::new(queue),
            storage: Arc::new(storage),
        })
    }

    /// Add a job to the queue
    pub async fn add<T: serde::Serialize>(
        &self,
        job_type: &str,
        data: T,
        options: JobOptions,
    ) -> Result<String> {
        let payload = serde_json::to_vec(&data)
            .map_err(|e| JobQueueError::SerializationError(e.to_string()))?;

        let job = Job::new(job_type.to_string(), payload, options.max_retries);

        // Store job in Redis
        self.storage.store_job(&job).await?;

        // Push to queue
        self.queue.push(&job, &self.name).await?;

        Ok(job.id)
    }

    /// Get job by ID
    pub async fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        self.storage.get_job(job_id).await
    }

    /// Get job status
    pub async fn get_job_status(&self, job_id: &str) -> Result<Option<crate::JobStatus>> {
        self.storage.get_job_status(job_id).await
    }

    /// Create a worker for this queue
    pub fn create_worker(&self, opts: WorkerOptions) -> Result<Worker> {
        Worker::new(self.storage.clone(), self.queue.clone(), &self.name, opts)
    }

    /// Close the queue (cleanup resources)
    pub async fn close(&self) -> Result<()> {
        Ok(())
    }

    pub fn storage(&self) -> &Arc<RedisStorage> {
        &self.storage
    }

    pub fn queue(&self) -> &Arc<RabbitMQQueue> {
        &self.queue
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone)]
pub struct Worker {
    worker: Arc<ConcurrentWorker<RedisStorage, RabbitMQQueue>>,
    queue_name: String,
}

impl Worker {
    fn new(
        storage: Arc<RedisStorage>,
        queue: Arc<RabbitMQQueue>,
        queue_name: &str,
        opts: WorkerOptions,
    ) -> Result<Self> {
        let worker = ConcurrentWorker::new((*queue).clone(), (*storage).clone(), opts);

        Ok(Self {
            worker: Arc::new(worker),
            queue_name: queue_name.to_string(),
        })
    }

    /// Process a job type with async handler
    pub async fn process_async<F, Fut, T>(&self, job_type: &str, handler: F) -> Result<()>
    where
        F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
        T: for<'de> serde::Deserialize<'de> + Send + 'static,
    {
        self.worker
            .register_async_handler(job_type, handler)
            .await?;

        Ok(())
    }

    /// Process sync functions (CPU-bound operations)
    pub async fn process_sync<F, T>(&self, job_type: &str, handler: F) -> Result<()>
    where
        F: Fn(T) -> Result<()> + Send + Sync + Clone + 'static,
        T: for<'de> serde::Deserialize<'de> + Send + 'static,
    {
        self.worker.register_sync_handler(job_type, handler).await?;

        Ok(())
    }

    /// Start the worker
    pub async fn run(&self) -> Result<()> {
        self.worker.start(&self.queue_name, 1).await;
        Ok(())
    }

    /// Stop the worker
    pub async fn close(self) -> Result<()> {
        self.worker.stop().await;
        Ok(())
    }

    // Get the number of currently active tasks
    pub fn get_active_tasks_count(&self) -> usize {
        self.worker.get_active_task_count()
    }

    // Get the number of available current slots
    pub fn get_available_slots(&self) -> usize {
        self.worker.get_available_slots()
    }

    // Get worker metrics
    pub fn get_metrics(&self) -> WorkerMetrics {
        WorkerMetrics {
            active_tasks: self.get_active_tasks_count(),
            available_slots: self.get_available_slots(),
            max_concurrency: self.worker.max_concurrent_tasks,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkerOptions {
    pub concurrency: usize,
    pub num_workers: usize,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            concurrency: 5,
            num_workers: 3,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkerMetrics {
    pub active_tasks: usize,
    pub available_slots: usize,
    pub max_concurrency: usize,
}
