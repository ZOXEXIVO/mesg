use crate::storage::utils::StorageIdGenerator;
use std::collections::{VecDeque};

use log::{info, warn};
use bytes::Bytes;
use prost::alloc::collections::{BTreeMap};
use std::cmp::Ordering;
use std::sync::Arc;
use chashmap::CHashMap;
use tokio::sync::{AcquireError, Semaphore, SemaphorePermit};
use crate::metrics::MetricsWriter;
use thiserror::Error;

pub struct Storage {
    storage: Arc<CHashMap<String, MessageStorage>>
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            storage: Arc::new(CHashMap::new())
        }
    }

    pub async fn push(&self, queue: &str, data: Bytes) -> Result<(), StorageError> {
        let message_id = StorageIdGenerator::generate();
        
        let message = Message::new(message_id, data);
              
        match self.storage.get_mut(queue) {
            Some(mut item) => {
                item.push(message).await?;
            },
            None => {
                MetricsWriter::inc_queues_count_metric();
                
                let mut storage = MessageStorage::new();

                storage.push(message).await?;

                self.storage.insert(queue.into(), storage);
            }
        };
        
        Ok(())
    }
    
    pub async fn pop(&self, queue: &str) -> Option<Message> {  
        match self.storage.get_mut(queue) {
            Some(mut storage) => {
                match storage.pop().await {
                    Ok(Some(msg)) => {
                        Some(msg)
                    },
                    _ => None
                }
            },
            _ => None
        }
    }
    
    pub async fn commit(&self, queue: &str, id: i64, consumer_id: u32) {
        match self.storage.get_mut(queue) {
            Some(mut guard) => {
                if let Some(_item) = guard.unacked.remove_entry(&id) {
                    info!("commited: queue={}, message_id={}, consumer_id={}", queue, &id, consumer_id);
                }
            },
            None => {
                warn!("commit failed: queue={}, message_id={}, consumer_id={}", queue, &id, consumer_id);
            }
        };
    }
}

impl Clone for Storage {
    fn clone(&self) -> Self {
        Storage {
            storage : Arc::clone(&self.storage)
        }
    }
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Storage lock error")]
    LockError(#[from] MessageStorageError)
}


// Message storage

type MessageStoragePushResult = Result<(), MessageStorageError>;
type MessageStoragePopResult = Result<Option<Message>, MessageStorageError>;

pub struct MessageStorage {
    data: VecDeque<Message>,   
    unacked: BTreeMap<i64, Message>,
    semaphore: Semaphore,
}

impl MessageStorage {
    pub fn new() -> Self {
        MessageStorage {
            data: VecDeque::new(),
            unacked: BTreeMap::new(),
            semaphore: Semaphore::new(1)
        }
    }

    pub async fn push(&mut self, message: Message) -> MessageStoragePushResult {
        let _ = self.semaphore.acquire().await?;

        self.data.push_back(Message::clone(&message));

        Ok(())
    }

    pub async fn pop(&mut self) -> MessageStoragePopResult {
        let _ = self.semaphore.acquire().await?;
        
        Ok(self.data.pop_front())
    }
}

#[derive(Error, Debug)]
pub enum MessageStorageError {
    #[error("Storage semaphore error")]
    LockError(#[from] AcquireError)
}

pub struct Message{
    pub id: i64,
    pub data: Bytes,
    pub delivered: bool
}

impl Ord for Message {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for Message {}

impl PartialOrd<Message> for Message {
    fn partial_cmp(&self, other: &Message) -> Option<Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Message{
    pub fn new(id: i64, data: Bytes) -> Self {
        Message {
            id,
            data: Bytes::clone(&data),
            delivered: false
        }
    }
}

impl Clone for Message {
    fn clone(&self) -> Self {
        Message {
            id: self.id.clone(),
            data: Bytes::clone(&self.data),
            delivered: self.delivered
        }
    }
}
