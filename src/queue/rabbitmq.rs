use crate::{Job, JobQueueError, Result};
use async_trait::async_trait;
use lapin::{
    Channel, ChannelState, Connection, ConnectionProperties, Consumer,
    options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable},
};
use log::{info, warn};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub struct RabbitMQQueue {
    connection: Arc<Connection>,
    channel: Arc<Mutex<Option<Channel>>>,
}

impl RabbitMQQueue {
    pub async fn new(uri: &str) -> Result<Self> {
        let connection = Connection::connect(uri, ConnectionProperties::default())
            .await
            .map_err(|e| JobQueueError::QueueError(e.to_string()))?;

        Ok(Self {
            connection: Arc::new(connection),
            channel: Arc::new(Mutex::new(None)),
        })
    }

    async fn get_channel(&self) -> Result<Channel> {
        let mut channel_guard = self.channel.lock().await;

        if channel_guard.is_none()
            || matches!(
                channel_guard.as_ref().unwrap().status().state(),
                ChannelState::Closed | ChannelState::Error | ChannelState::Closing
            )
        {
            *channel_guard = Some(
                self.connection
                    .create_channel()
                    .await
                    .map_err(|e| JobQueueError::QueueError(e.to_string()))?,
            );
            info!("Created new RabbitMQ channel");
        }

        Ok(channel_guard.as_ref().unwrap().clone())
    }

    async fn ensure_queue(&self, queue_name: &str, args: FieldTable) -> Result<()> {
        let channel = self.get_channel().await?;

        channel
            .queue_declare(
                queue_name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                args,
            )
            .await
            .map_err(|e| JobQueueError::QueueError(e.to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl crate::JobQueue for RabbitMQQueue {
    async fn push(&self, job: &Job, queue_name: &str) -> Result<()> {
        self.ensure_queue(queue_name, FieldTable::default()).await?;

        let channel = self.get_channel().await?;
        let payload = serde_json::to_vec(job)
            .map_err(|e| JobQueueError::SerializationError(e.to_string()))?;

        channel
            .basic_publish(
                "",
                queue_name,
                BasicPublishOptions::default(),
                &payload,
                Default::default(),
            )
            .await
            .map_err(|e| JobQueueError::QueueError(e.to_string()))?
            .await
            .map_err(|e| JobQueueError::QueueError(e.to_string()))?;

        info!("Pushed job {} to queue {}", job.id, queue_name);
        Ok(())
    }

    async fn push_retry(&self, job: &Job, queue_name: &str, delay_ms: u32) -> Result<()> {
        let retry_queue = format!("{}_retry_{}", &job.job_type, delay_ms);

        let mut args = FieldTable::default();
        args.insert("x-message-ttl".into(), delay_ms.into());
        args.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString("".into()),
        );
        args.insert(
            "x-dead-letter-routing-key".into(),
            AMQPValue::LongString(queue_name.into()),
        );

        self.ensure_queue(&retry_queue, args).await?;

        let channel = self.get_channel().await?;
        let payload = serde_json::to_vec(job)
            .map_err(|e| JobQueueError::SerializationError(e.to_string()))?;

        channel
            .basic_publish(
                "",
                &retry_queue,
                BasicPublishOptions::default(),
                &payload,
                Default::default(),
            )
            .await
            .map_err(|e| JobQueueError::QueueError(e.to_string()))?
            .await
            .map_err(|e| JobQueueError::QueueError(e.to_string()))?;

        info!("Pushed job {} to retry queue {}", job.id, retry_queue);
        Ok(())
    }

    async fn get_consumer(&self, queue_name: &str, consumer_tag: &str) -> Result<Consumer> {
        self.ensure_queue(queue_name, FieldTable::default()).await?;

        let channel = self.get_channel().await?;
        let consumer = channel
            .basic_consume(
                queue_name,
                consumer_tag,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| JobQueueError::QueueError(e.to_string()))?;

        Ok(consumer)
    }

    async fn health_check(&self) -> Result<bool> {
        match self.connection.status().connected() {
            true => Ok(true),
            false => {
                warn!("RabbitMQ connection health check failed");
                Ok(false)
            }
        }
    }
}
