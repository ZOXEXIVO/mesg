use crate::storage::utils::StorageIdGenerator;
use std::collections::{HashMap, VecDeque};

use log::{warn};
use bytes::Bytes;
use prost::alloc::collections::{BTreeMap};
use std::cmp::Ordering;
use std::sync::Arc;
use chashmap::CHashMap;
use tokio::sync::{AcquireError, Mutex};
use crate::metrics::MetricsWriter;
use thiserror::Error;
use tokio::sync::mpsc::{channel, Sender, Receiver};

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
    
    pub async fn pop(&self, queue: &str, application: &str) -> Option<Message> {  
        match self.storage.get_mut(queue) {
            Some(mut storage) => {
                match storage.pop(application).await {
                    Ok(Some(msg)) => {
                        Some(msg)
                    },
                    _ => None
                }
            },
            _ => None
        }
    }

    pub async fn is_subqueue_exists(&self, queue: &str, application: &str) -> bool {
        if let Some(storage) = self.storage.get(queue) {
            return storage.is_subqueue_exists(application).await;
        }

        false
    }
    
    pub async fn create_subqueue(&self, queue: &str, application: &str) -> bool {
        if let Some(storage) = self.storage.get_mut(queue) {
            return storage.create_subqueue(application).await;
        }
        
        false        
    }
    
    pub async fn commit(&self, id: i64, queue: &str, application: &str) -> bool {
        true
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

pub struct MessageStorage {
    sub_queues: Mutex<HashMap<String, VecDeque<Message>>>,
    unacked: BTreeMap<i64, Message>,
    notify: (Sender<()>, Receiver<()>)
}

impl MessageStorage {
    pub fn new() -> Self {
        MessageStorage {
            sub_queues: Mutex::new(HashMap::new()),
            unacked: BTreeMap::new(),
            notify: channel(1024)
        }
    }

    pub async fn push(&mut self, message: Message) -> MessageStoragePushResult {
        let mut guard = self.sub_queues.lock().await;

        let keys: Vec<String> = guard.keys().map(|k| String::from(k)).collect();

        for app_queue_key in &keys {
            if let Some(app_queue) = guard.get_mut(app_queue_key) {
                app_queue.push_back(Message::clone(&message));
            }
        }

        Ok(())
    }

    pub async fn pop(&mut self, application: &str) -> Result<Option<Message>, MessageStorageError> {
        let mut guard = self.sub_queues.lock().await;
        
        match guard.get_mut(application) {
            Some(application_queue) => {
                Ok(application_queue.pop_front())
            },
            None => Err(MessageStorageError::NoSubqueue)
        }
    }

    pub async fn is_subqueue_exists(&self, application: &str) -> bool {
        let guard = self.sub_queues.lock().await;

        guard.contains_key(application)
    }
    
    pub async fn create_subqueue(&self, application: &str) -> bool {
        let mut guard = self.sub_queues.lock().await;
        
        if !guard.contains_key(application) {
            guard.insert(application.into(), VecDeque::new());
        }
        
        true
    }
}

#[derive(Error, Debug)]
pub enum MessageStorageError {
    #[error("Application queue not exists")]
    NoSubqueue,
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
