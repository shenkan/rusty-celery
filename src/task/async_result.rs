use crate::backend::{Backend, BackendBuilder, TaskResultMetadata};
/// An [`AsyncResult`] is a handle for the result of a task.
#[derive(Debug, Clone)]
pub struct BackendAsyncResult<BE: Backend> {
    pub task_id: String,
    // if part of a chain
    pub parent: Option<Box<Self>>,
    // need a reference to the backend
    pub backend: Option<BE>,
}

impl<BE: Backend> BackendAsyncResult<BE> {
    pub fn new(task_id: &str, backend: Option<BE>) -> Self {
        Self {
            task_id: task_id.into(),
            parent: None,
            backend:  backend,
        }
    }
}

/// An [`AsyncResult`] is a handle for the result of a task.
#[derive(Debug, Clone)]
pub struct AsyncResult {
    pub task_id: String,
}

impl AsyncResult {
    pub fn new(task_id: &str) -> Self {
        Self {
            task_id: task_id.into(),
        }
    }
}
