use crate::error::BackendError;
use crate::task::TaskStatus;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::error;
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, from_value, json, Value};
use tokio::time::{self, Duration};

mod redis;
pub use self::redis::{RedisBackend, RedisBackendBuilder};

// TODO
// #[cfg(test)]
// pub mod mock;

/// A results [`Backend`] is used to store and retrive the results and status of the tasks.
#[async_trait]
pub trait Backend: Send + Sync + Sized {
    /// The builder type used to create the results backend with a custom configuration.
    type Builder: BackendBuilder<Backend = Self>;

    /// Update task state and result.
    async fn store_result(&self, task_id: &str, result: Option<String>, state: TaskStatus) -> Result<(), BackendError>;

    /// Get task meta from backend.
    async fn get_task_meta(&self, task_id: &str, cache: bool) -> Result<TaskResultMetadata, BackendError>;

    async fn get_result_meta(&self, task_id: &str, result: Option<String>, state: TaskStatus) -> TaskResultMetadata;

    async fn encode(&self, meta: TaskResultMetadata) -> Result<Vec<u8>, BackendError>;

    /// Get current state of a given task.
    async fn get_state(&self, task_id: &str) -> Result<TaskStatus, BackendError> {
        Ok(self.get_task_meta(task_id, true).await?.status)
    }

    /// Get result of a given task.
    async fn get_result(&self, task_id: &str) -> Result<Option<String>, BackendError> {
        Ok(self.get_task_meta(task_id, true).await?.result)
    }

    async fn mark_as_started(&self, task_id: &str, meta: TaskResultMetadata) -> Result<(), BackendError>;

    async fn mark_as_done(&self, task_id: &str, meta: TaskResultMetadata) -> Result<(), BackendError>;

    // TODO
    // async fn mark_as_failure(&self, task_id: &str, meta: TaskResultMetadata);

    fn safe_url(&self) -> String;

    async fn forget(&self, task_id: &str) -> Result<(), BackendError>;

    async fn sleep(&self, seconds: u64) {
        tokio::time::sleep(Duration::from_secs(seconds));
    }

    async fn is_cached(&self, task_id: &str) -> bool;

    fn builder(backend_url: &str) -> Result<Self::Builder, BackendError> {
        Ok(Self::Builder::new(backend_url))
    }
}


// Trait for a key/value store result [`Backend`].
#[async_trait]
pub trait KeyValueStoreBackend: Backend {
    const TASK_KEYPREFIX: &'static str;

    async fn get(&self, key: &str) -> Result<String, BackendError>;

    async fn mget(&self, keys: &[&str]) -> Result<Vec<String>, BackendError>;

    async fn set(&self, key: &str, value: TaskResultMetadata) -> Result<(), BackendError>;

    async fn delete(&self, key: &str) -> Result<(), BackendError>;

    async fn incr(&self, key: &str) -> Result<(), BackendError>;

    async fn expire(&self, key: &str, seconds: usize) -> Result<(), BackendError>;

    async fn get_key_for_task(&self, task_id: &str, key: &str) -> Result<String, BackendError>;

    async fn get_task_meta_for(&self, task_id: &str) -> Result<TaskResultMetadata, BackendError>;
}

/// Metadata of a task stored in a [`Backend`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResultMetadata {
    /// id of the task
    task_id: String,
    /// Current status of the task.
    status: TaskStatus,
    /// Result of the task.
    result: Option<String>,
    /// Date of culmination of the task
    date_done: Option<DateTime<Utc>>,
}

impl TaskResultMetadata {
    pub fn new(task_id: &str, result: Option<String>, status: TaskStatus) -> Self {
        let date_done = if let TaskStatus::Finished = status {
            Some(Utc::now())
        } else {
            None
        };

        Self {
            status,
            result: result,
            task_id: task_id.to_string(),
            date_done,
        }
    }

    // Serializes the [`TaskResultMetadata`] for storage in a [`Backend`]
    pub fn json_serialized(&self) -> Result<Vec<u8>, BackendError> {
        let result = match &self.result {
            Some(result) => json!(result.clone()),
            None => Value::Null,
        };
        let json_value = json!({
            "task_id": self.task_id.clone(),
            "result": result,
            "status": self.status.clone(),
            "date_done": self.date_done.clone(),
        });
        let res = serde_json::to_string(&json_value)?;
        Ok(res.into_bytes())
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
