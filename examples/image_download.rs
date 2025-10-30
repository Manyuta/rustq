use rustq::{JobOptions, JobQueueError, Queue, WorkerOptions};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

// Job struct
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DownloadJob {
    pub url: String,
    pub save_dir: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = Queue::new("jobs", "redis://localhost:6379", "amqp://localhost:5672").await?;

    // download images to specified directory
    let num_images = 100;
    for i in 1..=num_images {
        let url = format!(
            "https://yavuzceliker.github.io/sample-images/image-{}.jpg",
            i
        );

        queue
            .add(
                "image_download",
                DownloadJob {
                    url,
                    save_dir: "./images".to_string(),
                },
                JobOptions {
                    max_retries: 3,
                    ..Default::default()
                },
            )
            .await?;
    }

    // Create a worker with concurrency
    let worker = queue.create_worker(WorkerOptions {
        concurrency: 10,
        num_workers: 2,
    })?;

    worker
        .process_async("image_download", |job_data: DownloadJob| async move {
            println!("Starting download: {}", job_data.url);
            match download_image(job_data).await {
                Ok(_) => {
                    println!("Successfully downloaded");
                    Ok(())
                }
                Err(e) => {
                    eprintln!("Failed to download: {}", e);
                    Err(e)
                }
            }
        })
        .await?;

    // Run worker in background
    let worker_handle = tokio::spawn(async move {
        if let Err(e) = worker.run().await {
            eprintln!("Worker error: {}", e);
        }
    });

    // Force stop the worker
    worker_handle.abort();
    println!("Worker stopped");
    println!("Finished");
    Ok(())
}

async fn download_image(job: DownloadJob) -> Result<(), JobQueueError> {
    let bytes = reqwest::get(&job.url).await?.bytes().await?;

    let save_dir = job.save_dir.clone();
    let filename = job
        .url
        .split('/')
        .last()
        .unwrap_or("downloaded_image")
        .to_string();
    let save_path = std::path::Path::new(&save_dir).join(filename);

    tokio::fs::create_dir_all(&save_dir)
        .await
        .expect("Failed to create dir");

    let mut file = tokio::fs::File::create(&save_path)
        .await
        .expect("Failed to create file");

    file.write_all(&bytes)
        .await
        .expect("Failed to write to a file");

    Ok(())
}
