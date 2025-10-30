use crate::{Job, JobQueue, JobQueueError, JobStatus, JobStorage, Result};
use futures_util::stream::StreamExt;
use lapin::options::{BasicAckOptions, BasicNackOptions};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{RwLock, Semaphore, broadcast};
use uuid::Uuid;

pub type AsyncJobHandler =
    Box<dyn Fn(Job) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

pub struct ConcurrentWorker<S, Q> {
    queue: Arc<Q>,
    storage: Arc<S>,
    handlers: Arc<RwLock<HashMap<String, AsyncJobHandler>>>,
    task_semaphore: Arc<Semaphore>,
    running: Arc<RwLock<bool>>,
    shutdown_tx: tokio::sync::broadcast::Sender<()>, // To properly shut-down worker when there is no messages left
    worker_id: String,
    active_tasks: Arc<AtomicUsize>,
    pub max_concurrent_tasks: usize,
}

impl<S, Q> ConcurrentWorker<S, Q>
where
    S: JobStorage + Send + Sync + 'static,
    Q: JobQueue + Send + Sync + 'static,
{
    pub fn new(queue: Q, storage: S, max_concurrent_tasks: usize) -> Self {
        let worker_id = Uuid::new_v4().to_string();
        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            queue: Arc::new(queue),
            storage: Arc::new(storage),
            handlers: Arc::new(RwLock::new(HashMap::new())),
            task_semaphore: Arc::new(Semaphore::new(max_concurrent_tasks)),
            running: Arc::new(RwLock::new(false)),
            shutdown_tx,
            worker_id,
            active_tasks: Arc::new(AtomicUsize::new(0)),
            max_concurrent_tasks,
        }
    }

    pub async fn register_async_handler<F, Fut, T>(&self, job_type: &str, handler: F) -> Result<()>
    where
        F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
        T: for<'de> serde::Deserialize<'de> + Send + 'static,
    {
        let boxed_handler: AsyncJobHandler = Box::new(move |job: Job| {
            let handler = handler.clone();

            Box::pin(async move {
                let data: T = serde_json::from_slice(&job.payload)
                    .map_err(|e| JobQueueError::DeserializationError(e.to_string()))?;
                handler(data).await
            })
        });

        self.handlers
            .write()
            .await
            .insert(job_type.to_string(), boxed_handler);

        info!("Registered async handler for job type: {}", job_type);
        Ok(())
    }

    // For heavy CPU tasks that block the thread
    pub async fn register_sync_handler<F, T>(&self, job_type: &str, handler: F) -> Result<()>
    where
        F: Fn(T) -> Result<()> + Send + Sync + Clone + 'static,
        T: for<'de> serde::Deserialize<'de> + Send + 'static,
    {
        let boxed_handler: AsyncJobHandler = Box::new(move |job: Job| {
            let handler = handler.clone();
            Box::pin(async move {
                let data: T = serde_json::from_slice(&job.payload)
                    .map_err(|e| JobQueueError::DeserializationError(e.to_string()))?;

                // Run sync handler in blocking task
                tokio::task::spawn_blocking(move || handler(data))
                    .await
                    .map_err(|e| JobQueueError::HandlerError(e.to_string()))?
            })
        });

        self.handlers
            .write()
            .await
            .insert(job_type.to_string(), boxed_handler);

        info!("Registered sync handler for job type: {}", job_type);
        Ok(())
    }

    pub async fn start(&self, queue_name: &str, num_workers: usize) {
        info!(
            "Starting worker {} with {} workers",
            self.worker_id, num_workers
        );

        *self.running.write().await = true;

        let mut worker_handles = Vec::new();

        for worker_id in 0..num_workers {
            let worker = self.clone();
            let queue_name = queue_name.to_string();

            let handle = tokio::spawn(async move {
                worker.worker_loop(worker_id, &queue_name).await;
            });
            worker_handles.push(handle);
        }

        futures::future::join_all(worker_handles).await;
        info!("All workers stopped for worker {}", self.worker_id);
    }

    // A single worker loop
    async fn worker_loop(&self, worker_id: usize, queue_name: &str) {
        info!("Worker {}-{} started", self.worker_id, worker_id);

        let consumer_tag = format!("{}_{}", self.worker_id, worker_id);
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let mut consumer = match self.queue.get_consumer(queue_name, &consumer_tag).await {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "Worker {}-{} failed to create consumer: {}",
                    self.worker_id, worker_id, e
                );

                return;
            }
        };

        // Monitor concurrency of the worker
        let monitor_handle = self.start_concurrency_monitor();

        while *self.running.read().await {
            tokio::select! {
                delivery = consumer.next() => {
                    match delivery {
                        Some(Ok(delivery)) => {


                            let permit = match self.task_semaphore.clone().acquire_owned().await {
                                Ok(permit) => permit,
                                Err(_) => {
                                    warn!("Worker {}-{} semaphore closed", self.worker_id, worker_id);
                                    break;
                                }
                            };

                            // Increment active task counter
                            self.active_tasks.fetch_add(1, Ordering::SeqCst);


                            let worker = self.clone();
                            let queue_name = queue_name.to_string();

                            tokio::spawn(async move {
                                // Process delivery while holding the permit
                                if let Err(e) = worker.process_delivery(delivery, &queue_name).await {
                                    error!("Error processing delivery: {}", e);
                                }

                                // Decrement active task counter
                                worker.active_tasks.fetch_sub(1, Ordering::SeqCst);



                                drop(permit);
                            });
                        }
                        Some(Err(e)) => {
                            error!("Worker {}-{} delivery error: {}", self.worker_id, worker_id, e);

                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        }
                        None => {
                            warn!("Worker {}-{} consumer connection broken", self.worker_id, worker_id);

                            break;
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Worker {}-{} received shutdown signal", self.worker_id, worker_id);
                    break;
                }
            }
        }

        // Stop the monitor
        drop(monitor_handle);

        info!("Worker {}-{} stopped", self.worker_id, worker_id);
    }

    async fn process_delivery(
        &self,
        delivery: lapin::message::Delivery,
        queue_name: &str,
    ) -> Result<()> {
        let job: Job = match serde_json::from_slice(&delivery.data) {
            Ok(job) => job,
            Err(e) => {
                error!("Failed to deserialize job: {}", e);

                // Nack malformed messages without requeue
                delivery
                    .nack(BasicNackOptions {
                        multiple: false,
                        requeue: false,
                    })
                    .await?;
                return Ok(());
            }
        };

        debug!("Processing job {}", job.id);

        if let Err(e) = self
            .storage
            .update_job_status(&job.id, JobStatus::Processing)
            .await
        {
            error!("Failed to update job status to processing: {}", e);
        }

        match self.execute_job(job.clone()).await {
            Ok(()) => {
                info!("Job {} completed successfully", job.id);

                if let Err(e) = self
                    .storage
                    .update_job_status(&job.id, JobStatus::Completed)
                    .await
                {
                    error!("Failed to update job status to completed: {}", e);
                }

                delivery.ack(BasicAckOptions::default()).await?;
            }
            Err(e) => {
                self.handle_failed_job(job, delivery, queue_name, e).await?;
            }
        }

        Ok(())
    }

    async fn execute_job(&self, job: Job) -> Result<()> {
        let handlers = self.handlers.read().await;

        // Early return if there is no registered handlers for the job
        let handler = match handlers.get(&job.job_type) {
            Some(handler) => handler,
            None => {
                warn!("No handler found for the job id: {}", job.id);

                if let Err(e) = self
                    .storage
                    .update_job_status(&job.id, JobStatus::Failed)
                    .await
                {
                    error!("Failed to update job status to failed: {}", e);
                }

                return Err(JobQueueError::HandlerError(format!(
                    "No handler registered for the job id: {}",
                    job.id
                )));
            }
        };

        handler(job).await
    }

    async fn handle_failed_job(
        &self,
        mut job: Job,
        delivery: lapin::message::Delivery,
        queue_name: &str,
        error: JobQueueError,
    ) -> Result<()> {
        let error_msg = error.to_string();
        warn!("Job {} failed: {}", job.id, error_msg);

        if job.can_retry() {
            let delay = job.get_retry_delay();

            info!(
                "Retrying job {} in {}ms (attempt {})",
                job.id,
                delay.as_millis(),
                job.retry_count + 1
            );

            // Update retry count
            job = job.with_retry(Some(error_msg));

            // Store the job in storage with the new retry count
            if let Err(e) = self.storage.store_job(&job).await {
                error!("Failed to store job with id: {}, error: {}", &job.id, e);
            }

            if let Err(e) = self
                .queue
                .push_retry(&job, queue_name, delay.as_millis() as u32)
                .await
            {
                error!("Failed to push retry job {}: {}", job.id, e);

                delivery
                    .nack(BasicNackOptions {
                        multiple: false,
                        requeue: true,
                    })
                    .await?;
            } else {
                delivery.ack(BasicAckOptions::default()).await?;
            }
        } else {
            error!("Job {} failed after {} retries", job.id, job.max_retries);

            if let Err(e) = self
                .storage
                .update_job_status(&job.id, JobStatus::Failed)
                .await
            {
                error!("Failed to update job status to failed: {}", e);
            }

            // Permanent failure, job goes to dead-letter queue
            delivery
                .nack(BasicNackOptions {
                    multiple: false,
                    requeue: false,
                })
                .await?;
        }

        Ok(())
    }

    pub async fn stop(&self) {
        *self.running.write().await = false;
        let _ = self.shutdown_tx.send(());
        info!("Worker {} stopped", self.worker_id);
    }

    pub async fn health_check(&self) -> Result<bool> {
        let queue_health = self.queue.health_check().await.unwrap_or(false);
        let storage_health = self.storage.health_check().await.unwrap_or(false);

        Ok(queue_health && storage_health)
    }

    // Concurrency monitor helper function
    fn start_concurrency_monitor(&self) -> tokio::task::JoinHandle<()> {
        let active_tasks = self.active_tasks.clone();
        let max_concurrent = self.max_concurrent_tasks;
        let worker_id = self.worker_id.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
            loop {
                interval.tick().await;
                let active = active_tasks.load(Ordering::SeqCst);
                let available = max_concurrent.saturating_sub(active);
                println!(
                    "[MONITOR] Worker {} - Active: {}/{} (Available: {})",
                    worker_id, active, max_concurrent, available
                );

                // Alert if we're at capacity
                if available == 0 {
                    println!("Worker {} at maximum concurrency!", worker_id);
                }
            }
        })
    }

    pub fn get_active_task_count(&self) -> usize {
        self.active_tasks.load(Ordering::SeqCst)
    }

    pub fn get_available_slots(&self) -> usize {
        let active = self.active_tasks.load(Ordering::SeqCst);
        self.max_concurrent_tasks.saturating_sub(active)
    }
}

impl<S, Q> Clone for ConcurrentWorker<S, Q> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            storage: self.storage.clone(),
            handlers: self.handlers.clone(),
            task_semaphore: self.task_semaphore.clone(),
            running: self.running.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            worker_id: self.worker_id.clone(),
            active_tasks: self.active_tasks.clone(),
            max_concurrent_tasks: self.max_concurrent_tasks,
        }
    }
}
