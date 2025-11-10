//! Storage implementation for job persistence

pub mod redis;
pub mod traits;

pub use redis::*;
pub use traits::JobStorage;
