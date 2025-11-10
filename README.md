## Rust distibuted job queue system
A distributed job queue built in **Rust** using **RabbitMQ** for message delivery and **Redis** for persistent job storage. It supports asynchronous workers, retries with backoff strategies, and horizontal scalability.

## Features 
- **Distributed Queue** - RabbitMQ manages message routing and delivery.
- **Persistent Storage** - Redis stores job data and status. 
- **Concurrent Workers** - Run miltiple async workers with configurable concurrency and number of workers.
- **Jobs Retry and Backoff** - The jobs are retried with configurable backoff strategy.
- **Extensible** - Easily extensible with custom job types and storage engines.

## Architecture 
                         ┌────────────────────┐
                         │      Producer      │
                         │                    │
                         └───────┬────────────┘
                                 │
                       ┌─────────▼─────────┐
                       │    RabbitMQ       │
                       │  (Message Broker) │
                       └─────────┬─────────┘
                                 │
                 ┌───────────────┴────────────────┐
                 │                                │
       ┌─────────▼───────────┐         ┌──────────▼───────────┐
       │    Redis Storage    │         │   Concurrent Worker   │
       │ Job Data + Metadata │         │  Async/Sync Handlers  │
       └─────────┬───────────┘         │ Retry / Backoff / DLQ │
                 │                     └──────────┬────────────┘
                 │                                │
                 └────────────────────────────────┘
                         Status Updates & Results

## Overview 
### RabbitMQQueue 
Handles connection, channel reuse, and message publishing.
- Declares queue dynamically.
- Supports retry queues (`x-dead-letter-exchange`, `x-message-ttl`).

### RedisStorage 
Manages persistent job state and metadata. Stores job data, creation timestamps and status.

### Worker 
A distributed async worker pool that:
- Processes messages from RabbitMQ.
- Controls configurable concurrency.
- Handles retries, backoff and permament failures.
- Automatically acknowledges or rejects messages.


### Prerequisites:
- Rust (v1.80+)
- RabbitMq running (`localhost:5672`)
- Redis running (`localhost:6379`)
You can start both via Docker:

```bash
docker compose up -d
```

## Usage 

1. Create a job, either with default configuration or provide custom configuration, like retries and backoff strategy.
2. Create a Queue with name, and connect the queue to Redis and RabbitMQ.
3. Create a Worker with configurable concurrency and number of workers.
4. Define a handler to process the job.
5. Register handler and the job type in the worker.
6. Run the worker. 

```rust
use rustq::{Queue, JobOptions, Result};
use serde::Serialize;

#[derive(Serialize)]
struct EmailJob {
    to: String,
    subject: String,
    body: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let queue = Queue::new("email_jobs", "redis://127.0.0.1/", "amqp://127.0.0.1:5672/").await?;

    let job_id = queue
        .add(
            "send_email",
            EmailJob {
                to: "user@example.com".into(),
                subject: "Welcome!".into(),
                body: "Thanks for joining.".into(),
            },
            JobOptions::default(),
        )
        .await?;

    let worker = queue.create_worker(WorkerOptions { concurrency: 5 })?;

    worker
        .process_async("send_email", send_email)
        .await?;

    worker.run().await?;
    
    Ok(())
}

// Handler to process the job
async fn send_email(job: EmailJob) -> Result<()> {
  println!("Sending email to {}", job.to);
  Ok(())
}
```

