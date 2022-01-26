use std::collections::{HashMap, VecDeque};

use crate::storage::message::Message;
use crate::storage::{RingBufferFile, RingBufferFileHeader};
use log::info;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{AcquireError, Mutex};

// Message storage
pub struct MessageStore {
    file: RingBufferFile,
}

impl MessageStore {
    pub async fn from_filename(queue_name: String) -> Self {
        let storage = MessageStore {
            file: RingBufferFile::new(queue_name).await,
        };

        storage
    }

    pub async fn commit(
        &mut self,
        application: &str,
        id: i64,
    ) -> Result<bool, MessageStorageError> {
        Ok(false)
    }

    pub async fn uncommit(
        &mut self,
        application: &str,
        id: i64,
    ) -> Result<bool, MessageStorageError> {
        Ok(false)
    }

    pub async fn push(&mut self, message: Message) -> Result<bool, MessageStorageError> {
        Ok(false)
    }

    pub async fn pop(
        &mut self,
        application: &str,
        invisibility_timeout: u32,
    ) -> Result<Option<Message>, MessageStorageError> {
        Ok(None)
    }

    pub fn start_restore_task(&self, data: (String, i64), invisibility_timeout: u32) {}

    pub async fn is_application_exists(&self, application: &str) -> bool {
        false
    }

    pub async fn create_application_queue(&self, application: &str) -> bool {
        false
    }
}

#[derive(Error, Debug)]
pub enum MessageStorageError {
    #[error("Application queue not exists")]
    NoSubqueue,
    #[error("Storage semaphore error")]
    LockError(#[from] AcquireError),
}
