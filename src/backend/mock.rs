#![allow(dead_code)]
use super::{Backend, BackendBuilder, TaskResultMetadata};
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

pub struct MockBackendBuilder;

#[async_trait]
impl BackendBuilder for MockBackendBuilder {
    type Backend = MockBackend;

    #[allow(unused)]
    fn new(backend_url: &str) -> Self {
        Self {}
    }

    #[allow(unused)]
    async fn build(&self, connection_timeout: u32) -> Result<Self::Backend, BackendError> {
        Ok(MockBackend::new())
    }
}

#[derive(Default)]
pub struct MockBackend;

impl MockBackend {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn reset(&self) {
        // self.sent_tasks.write().await.clear();
    }
}

#[async_trait]
impl Backend for MockBackend {
    type Builder = MockBackendBuilder;

    /// Update task state and result.
    async fn store_result(&self, task_id: &str, result: String, state: TaskStatus) -> Result<(), ()> {
        Ok(())
    }

    async fn forget(&self, task_id: &str) -> Result<(), BackendError> {
        // Ok(self.delete(self.get_key_for_task(task_id, "").await?.as_str()).await?)
        Err(BackendError::NotConnected)
    }

    fn safe_url(&self) -> String {
        "mock://fake-url:8000/".into()
    }

    /// Get task meta from backend.
    async fn get_task_meta(&self, task_id: &str) -> Result<TaskResultMetadata, BackendError> {
        // Ok(ResultMetadata::new(task_id, ))
        Err(BackendError::NotConnected)
    }
}
