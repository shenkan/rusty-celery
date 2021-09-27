#![allow(dead_code)]
use super::{Backend, BackendBuilder, KeyValueStoreBackend, TaskResultMetadata};
use crate::error::{BackendError, ProtocolError};
use crate::protocol::Delivery;
use crate::protocol::Message;
use crate::protocol::TryDeserializeMessage;
use crate::task::TaskStatus;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use log::{debug, error, warn};
use lru_cache::LruCache;
use redis::aio::ConnectionManager;
use redis::RedisError;
use redis::{from_redis_value, ErrorKind};
use redis::{AsyncCommands, Client, Commands};
use std::clone::Clone;
use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::task::{Poll, Waker};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use uuid::Uuid;

const MAX_STR_VALUE_SIZE: usize = 536870912;
const MAX_CACHE_RESULTS: usize = 100;

struct Config {
    // add other fields
    backend_url: String,
    // always_retry: bool,
}

pub struct RedisBackendBuilder {
    // add other fields
    config: Config,
}

#[async_trait]
impl BackendBuilder for RedisBackendBuilder {
    type Backend = RedisBackend;

    fn new(backend_url: &str) -> Self {
        RedisBackendBuilder {
            config: Config {
                // add other fields
                backend_url: backend_url.into(),
                // always_retry: true,
                // prefetch_count: 10,
                // queues: HashSet::new(),
                // heartbeat: Some(60),
            },
        }
    }

    async fn build(&self, connection_timeout: u32) -> Result<Self::Backend, BackendError> {
        println!("Creating client");
        let client = Client::open(&self.config.backend_url[..])
            .map_err(|_| BackendError::InvalidBackendUrl(self.config.backend_url.clone()))?;

        let manager = client.get_tokio_connection_manager().await?;

        let (tx, rx) = channel(1);

        Ok(RedisBackend {
            uri: self.config.backend_url.clone(),
            client: client,
            manager: manager,
            waker_rx: Mutex::new(rx),
            waker_tx: tx,
            cache: Arc::new(Mutex::new(LruCache::new(MAX_CACHE_RESULTS))),
        })
    }
}

pub struct RedisBackend {
    uri: String,
    client: Client,
    manager: ConnectionManager,
    waker_rx: Mutex<Receiver<Waker>>,
    waker_tx: Sender<Waker>,
    cache: Arc<Mutex<LruCache<String, TaskResultMetadata>>>,
}

#[async_trait]
impl KeyValueStoreBackend for RedisBackend {
    const TASK_KEYPREFIX: &'static str = "celery-task-meta-";

    async fn get(&self, key: &str) -> Result<String, BackendError> {
        // let mut conn = self.client.clone().get_async_connection().await?;
        // Ok(conn.get(key)?)
        let cmd = redis::cmd("GET")
            .arg(key)
            .query_async(&mut self.manager.clone())
            .await?;
        Ok(cmd)
    }

    async fn mget(&self, keys: &[&str]) -> Result<Vec<String>, BackendError> {
        // let mut conn = self.client.clone().get_async_connection().await?;
        // Ok(conn.get(keys.to_vec())?)
        let cmd = redis::cmd("MGET")
            .arg(keys.to_vec())
            .query_async(&mut self.manager.clone())
            .await?;
        Ok(cmd)
    }

    async fn set(&self, key: &str, value: TaskResultMetadata) -> Result<(), BackendError> {
        // let mut conn = self.client.get_tokio_connection().await?;
        // Ok(conn.set(key, value).await?)
        // handle value being too large
        let cmd = redis::cmd("SET")
            .arg(key)
            .arg(self.encode(value).await?)
            .query_async(&mut self.manager.clone())
            .await?;
        Ok(cmd)
    }

    async fn delete(&self, key: &str) -> Result<(), BackendError> {
        // let mut conn = self.client.clone().get_async_connection().await?;
        // Ok(conn.del(key).await?)
        let cmd = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut self.manager.clone())
            .await?;
        Ok(cmd)
    }

    async fn incr(&self, key: &str) -> Result<(), BackendError> {
        // let mut conn = self.client.clone().get_async_connection().await?;
        // Ok(conn.incr(key, 1).await?)
        let cmd = redis::cmd("INCR")
            .arg(key)
            .query_async(&mut self.manager.clone())
            .await?;
        Ok(cmd)
    }

    async fn expire(&self, key: &str, seconds: usize) -> Result<(), BackendError> {
        // let mut conn = self.client.clone().get_async_connection().await?;
        // Ok(conn.expire(key, seconds).await?)
        let cmd = redis::cmd("EXPIRE")
            .arg(key)
            .arg(seconds)
            .query_async(&mut self.manager.clone())
            .await?;
        Ok(cmd)
    }

    // Get the cache key for a `Task` by `task_id`
    async fn get_key_for_task(&self, task_id: &str, key: &str) -> Result<String, BackendError> {
        Ok(vec![Self::TASK_KEYPREFIX, task_id, key].join(""))
    }

    // Get task metadata for a `Task` by `task_id`
    async fn get_task_meta_for(&self, task_id: &str) -> Result<TaskResultMetadata, BackendError> {
        match self.get(self.get_key_for_task(task_id, "").await?.as_str()).await.ok() {
            Some(result) => {
                let meta = TaskResultMetadata::new(task_id, Some(result), TaskStatus::Finished);
                Ok(meta)
            }
            None => Ok(TaskResultMetadata::new(task_id, None, TaskStatus::Pending)),
        }
    }
}

#[async_trait]
impl Backend for RedisBackend {
    type Builder = RedisBackendBuilder;

    async fn store_result(&self, task_id: &str, result: Option<String>, state: TaskStatus) -> Result<(), BackendError> {
        // TODO: implement retries as follows:
        // on error, we check if always_retry_backend_operation is `true` and that the
        // error `is_safe_to_retry()`.  If so, we continue to call `set_with_state(key, value, state)`
        // while retries < max_retries
        let meta = self.get_result_meta(task_id, result, state).await;
        self.set(self.get_key_for_task(task_id, "").await?.as_str(), meta)
            .await?;
        Ok(())
    }

    async fn encode(&self, meta: TaskResultMetadata) -> Result<Vec<u8>, BackendError> {
        Ok(meta.json_serialized()?)
    }

    async fn mark_as_started(&self, task_id: &str, meta: TaskResultMetadata) -> Result<(), BackendError> {
        Ok(self.store_result(task_id, None, TaskStatus::Pending).await?)
    }

    async fn mark_as_done(&self, task_id: &str, meta: TaskResultMetadata) -> Result<(), BackendError> {
        Ok(self.store_result(task_id, meta.result, TaskStatus::Finished).await?)
    }

    async fn get_result_meta(&self, task_id: &str, result: Option<String>, state: TaskStatus) -> TaskResultMetadata {
        TaskResultMetadata::new(task_id, result, state)
    }

    async fn get_task_meta(&self, task_id: &str, cache: bool) -> Result<TaskResultMetadata, BackendError> {
        if cache {
            let lrucache = self.cache.clone();
            // `LruCache` doesn't appear to have a way to get an immutable reference without removing the entry
            if let Some(meta) = lrucache.lock().await.remove(task_id) {
                // so we reinsert it
                lrucache.lock().await.insert(task_id.to_string(), meta.clone());
                return Ok(meta);
            };
        }
        match self.get_task_meta_for(task_id).await {
            Ok(meta) => {
                if cache && meta.status == TaskStatus::Finished {
                    let lrucache = self.cache.clone();
                    lrucache.lock().await.insert(task_id.to_string(), meta.clone());
                }
                Ok(meta)
            }
            Err(e) => {
                // TODO: implement retries as follows:
                // on error, we check if always_retry_backend_operation is `true` and that the
                // error `is_safe_to_retry()`.  If so, we continue to call `get_task_meta_for(task_id)`
                // while retries < max_retries
                Err(e)
            }
        }
    }

    // Default implementation
    async fn get_state(&self, task_id: &str) -> Result<TaskStatus, BackendError> {
        Ok(self.get_task_meta(task_id, true).await?.status)
    }

    // Default implementation
    async fn get_result(&self, task_id: &str) -> Result<Option<String>, BackendError> {
        Ok(self.get_task_meta(task_id, true).await?.result)
    }

    fn safe_url(&self) -> String {
        let parsed_url = redis::parse_redis_url(&self.uri[..]);
        match parsed_url {
            Some(url) => format!(
                "{}://{}:***@{}:{}/{}",
                url.scheme(),
                url.username(),
                url.host_str().unwrap(),
                url.port().unwrap(),
                url.path(),
            ),
            None => {
                error!("Invalid redis url.");
                String::from("")
            }
        }
    }

    async fn forget(&self, task_id: &str) -> Result<(), BackendError> {
        Ok(self.delete(self.get_key_for_task(task_id, "").await?.as_str()).await?)
    }

    async fn is_cached(&self, task_id: &str) -> bool {
        self.cache
            .clone()
            .lock()
            .await
            .contains_key(self.get_key_for_task(task_id, "").await.unwrap().as_str())
    }

    // Default implementation
    fn builder(backend_url: &str) -> Result<Self::Builder, BackendError> {
        Ok(Self::Builder::new(backend_url))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
