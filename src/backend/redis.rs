#![allow(dead_code)]
use super::{Backend, BackendBuilder, ResultMetadata};
use crate::error::{BackendError, ProtocolError};
use crate::protocol::Delivery;
use crate::protocol::Message;
use crate::protocol::TryDeserializeMessage;
use crate::task::TaskStatus;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use log::{debug, error, warn};
use redis::aio::ConnectionManager;
use redis::Client;
use redis::RedisError;
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

struct Config {
    // add other fields
    backend_url: String,
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
                // prefetch_count: 10,
                // queues: HashSet::new(),
                // heartbeat: Some(60),
            },
        }
    }

    async fn build(&self, connection_timeout: u32) -> Result<Self::Backend, BackendError> {
        // let mut queues: HashSet<String> = HashSet::new();
        // for queue_name in &self.config.queues {
        //     queues.insert(queue_name.into());
        // }
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
        })
    }
}

pub struct RedisBackend {
    uri: String,
    client: Client,
    manager: ConnectionManager,
    waker_rx: Mutex<Receiver<Waker>>,
    waker_tx: Sender<Waker>,
}

#[async_trait]
impl Backend for RedisBackend {
    type Builder = RedisBackendBuilder;

    async fn store_result(&self, task_id: &str, result: String, state: TaskStatus) -> Result<(), ()> {
        Ok(())
    }

    async fn get_task_meta(&self, task_id: &str) -> Result<ResultMetadata, BackendError> {
        Err(BackendError::NotConnected)
    }

    // Default implementation
    async fn get_state(&self, task_id: &str) -> Result<TaskStatus, BackendError> {
        Ok(self.get_task_meta(task_id).await?.status)
    }

    // Default implementation
    async fn get_result(&self, task_id: &str) -> Result<Option<String>, BackendError> {
        Ok(self.get_task_meta(task_id).await?.result)
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

    // Default implementation
    fn builder(backend_url: &str) -> Result<Self::Builder, BackendError> {
        Ok(Self::Builder::new(backend_url))
    }
}
