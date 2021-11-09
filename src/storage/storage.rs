use crate::storage::utils::StorageIdGenerator;
use std::collections::{VecDeque};

use log::{info, warn};
use bytes::Bytes;
use prost::alloc::collections::{BTreeMap};
use std::cmp::Ordering;
use std::sync::Arc;
use chashmap::CHashMap;
use tokio::sync::{AcquireError, Semaphore, SemaphorePermit, Notify};
use crate::metrics::MetricsWriter;
use thiserror::Error;
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver, unbounded_channel, channel, Sender, Receiver};

pub struct Storage {
    storage: Arc<CHashMap<String, MessageStorage>>
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            storage: Arc::new(CHashMap::new())
        }
    }

    pub async fn push(&self, queue: &str, data: Bytes, broadcast: bool) -> Result<(), StorageError> {
        let message_id = StorageIdGenerator::generate();
        
        let message = Message::new(message_id, data);
              
        match self.storage.get_mut(queue) {
            Some(mut item) => {
                item.push(message).await?;
                //item.notify.
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
    
    pub async fn get_queue_notify(&self, queue: &str) -> Option<Receiver<()>>{
        if let Some(item) = self.storage.get_mut(queue) {
            return Some(item.get_notify().await);
        }
        
        None
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
    notify: (Sender<()>, Receiver<()>)
}

impl MessageStorage {
    pub fn new() -> Self {
        MessageStorage {
            data: VecDeque::new(),
            unacked: BTreeMap::new(),
            semaphore: Semaphore::new(1),
            notify: channel(1024)
        }
    }

    pub async fn push(&mut self, message: Message) -> MessageStoragePushResult {
        let _ = self.semaphore.acquire().await?;

        self.data.push_back(Message::clone(&message));
        
        let (sender, _) = &self.notify;

        sender.send(()).await;
        
        Ok(())
    }

    pub async fn pop(&mut self) -> MessageStoragePopResult {
        let _ = self.semaphore.acquire().await?;
        
        Ok(self.data.pop_front())
    }
    
    pub async fn get_notify(&self) -> Receiver<()> {
        let (_, mut receiver) = &self.notify;
        receiver()
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
