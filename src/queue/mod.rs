//! Queue implementations for different message brokers

pub mod rabbitmq;
pub mod traits;

pub use rabbitmq::RabbitMQQueue;
pub use traits::JobQueue;
