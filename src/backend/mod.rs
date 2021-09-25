use crate::error::BackendError;
use log::error;
use crate::task::TaskStatus;
use tokio::time::{self, Duration};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

mod redis;
pub use self::redis::{RedisBackend, RedisBackendBuilder};

#[cfg(test)]
pub mod mock;

/// A results [`Backend`] is used to store and retrive the results and status of the tasks.
#[async_trait]
pub trait Backend: Send + Sync + Sized {
    /// The builder type used to create the results backend with a custom configuration.
    type Builder: BackendBuilder<Backend = Self>;

    /// Update task state and result.
    async fn store_result(
        &self,
        task_id: &str,
        result: String,
        state: TaskStatus,
    ) -> Result<(), ()>;
    
    /// Get task meta from backend.
    async fn get_task_meta(&self, task_id: &str) -> Result<ResultMetadata, BackendError>;
    
    /// Get current state of a given task.
    async fn get_state(&self, task_id: &str) -> Result<TaskStatus, BackendError> {
        Ok(self.get_task_meta(task_id).await?.status)
    }
    
    /// Get result of a given task.
    async fn get_result(&self, task_id: &str) -> Result<Option<String>, BackendError> {
        Ok(self.get_task_meta(task_id).await?.result)
    }

    fn safe_url(&self) -> String;
    
    fn builder(backend_url: &str) -> Result<Self::Builder, BackendError> {
        Ok(Self::Builder::new(backend_url))
    }
}

/// Metadata of the task stored in the storage used.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultMetadata {
    /// id of the task
    task_id: String,
    /// Current status of the task.
    status: TaskStatus,
    /// Result of the task.
    result: Option<String>,
    /// Date of culmination of the task
    date_done: Option<DateTime<Utc>>,
}

impl ResultMetadata {
    pub fn new(task_id: &str, result: String, status: TaskStatus) -> Self {
        let date_done = if let TaskStatus::Finished = status {
            Some(Utc::now())
        } else {
            None
        };

        Self {
            status,
            result: Some(result),
            task_id: task_id.to_string(),
            date_done,
        }
    }
}

/// A [`BackendBuilder`] is used to create a type of results [`Backend`] with a custom configuration.
#[async_trait]
pub trait BackendBuilder {
    type Backend: Backend;

    fn new(backend_url: &str) -> Self;
    /// Construct the `Backend` with the given configuration.
    async fn build(&self, connection_timeout: u32) -> Result<Self::Backend, BackendError>;
}


pub(crate) async fn build_and_connect_backend<Bb: BackendBuilder>(
    backend_builder: Bb,
    connection_timeout: u32,
    connection_max_retries: u32,
    connection_retry_delay: u32,
) -> Result<Bb::Backend, BackendError> {
    let mut backend: Option<Bb::Backend> = None;

    for _ in 0..connection_max_retries {
        match backend_builder.build(connection_timeout).await {
            Err(err) => {
                if err.is_connection_error() {
                    error!("{}", err);
                    error!(
                        "Failed to establish connection with broker, trying again in {}s...",
                        connection_retry_delay
                    );
                    time::sleep(Duration::from_secs(connection_retry_delay as u64)).await;
                    continue;
                }
                return Err(err);
            }
            Ok(b) => {
                backend = Some(b);
                break;
            }
        };
    }

    Ok(backend.ok_or_else(|| {
        error!("Failed to establish connection with broker");
        BackendError::NotConnected
    })?)
}
