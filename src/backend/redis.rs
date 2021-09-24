#![allow(dead_code)]
use super::{Backend, BackendBuilder};
use crate::task::TaskStatus;
use crate::error::{BackendError, ProtocolError};
use crate::protocol::Delivery;
use crate::protocol::Message;
use crate::protocol::TryDeserializeMessage;
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
    // add fields
}

pub struct RedisBackendBuilder {
    config: Config,
}

#[async_trait]
impl BackendBuilder for RedisBackendBuilder {
    type Backend = RedisBackend;

}

pub struct RedisBackend {
    // add fields
}

#[async_trait]
impl Backend for RedisBackend { 
    type Builder = RedisBackendBuilder;

    async fn store_result(&self, task_id: &str, result: String, state: TaskStatus) -> Result<(), ()> {

    }

    async fn get_task_meta(&self, task_id: &str) -> Result<ResultMetadata, BackendError> {

    }

    async fn get_state(&self, task_id: &str) -> Result<TaskStatus, BackendError> {
        Ok(self.get_task_meta(task_id).await?.status)
    }

    async fn get_result(&self, task_id: &str) -> Result<Option<String>, BackendError> {
        Ok(self.get_task_meta(task_id).await?.result)
    }
}