use rustq::{JobOptions, Queue, WorkerOptions};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
struct EmailData {
    to: String,
    subject: String,
    body: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProcessData {
    input: String,
    complexity: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a queue
    let queue = Queue::new("default", "redis://localhost:6379", "amqp://localhost:5672").await?;

    // Add some jobs
    for i in 0..5 {
        let job_id = queue
            .add(
                "send_email",
                EmailData {
                    to: format!("user{}@example.com", i),
                    subject: "Welcome".to_string(),
                    body: "Hello!".to_string(),
                },
                JobOptions {
                    max_retries: 3,
                    ..Default::default()
                },
            )
            .await?;
        println!("Added email job: {}", job_id);
    }

    // Add a CPU-intensive job
    let _ = queue
        .add(
            "process_data",
            ProcessData {
                input: "large dataset".to_string(),
                complexity: 1000,
            },
            JobOptions::default(),
        )
        .await?;

    // Create and configure worker
    let worker = queue.create_worker(WorkerOptions {
        concurrency: 3,
        num_workers: 2,
    })?;

    // Register async handler for I/O-bound tasks
    worker
        .process_async("send_email", |data: EmailData| async move {
            println!("Sending email to: {}", data.to);
            // Simulate async work
            tokio::time::sleep(Duration::from_secs(1)).await;

            println!("Email sent to: {}", data.to);

            Ok(())
        })
        .await?;

    // Register sync handler for CPU-bound tasks
    worker
        .process_sync("process_data", |data: ProcessData| {
            println!("Processing data with complexity: {}", data.complexity);
            // Simulate CPU-intensive work

            std::thread::sleep(Duration::from_millis(500));

            println!("Data processed: {}", data.input);

            Ok(())
        })
        .await?;

    println!("Worker started. Press Ctrl+C to stop.");

    // Run worker in a separate task
    let _worker_handle = tokio::spawn(async move {
        worker.run().await.unwrap();
        // Stop the worker
        worker.close().await.unwrap();
    });

    // Wait for all jobs to be processed
    tokio::time::sleep(Duration::from_secs(10)).await;

    println!("Finished");

    Ok(())
}
