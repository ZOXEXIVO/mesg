use crate::storage::utils::StorageIdGenerator;
use std::collections::{VecDeque};

use log::{info, warn};
use bytes::Bytes;
use prost::alloc::collections::{BTreeMap};
use std::cmp::Ordering;
use std::sync::Arc;
use chashmap::CHashMap;
use tokio::sync::Semaphore;
use crate::metrics::MetricsWriter;

pub struct Storage {
    storage: Arc<CHashMap<String, MessageStorage>>
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            storage: Arc::new(CHashMap::new())
        }
    }

    pub async fn push(&self, queue: &str, data: Bytes) {
        let message_id = StorageIdGenerator::generate();
        
        let message = Message::new(message_id, data);
              
        match self.storage.get_mut(queue) {
            Some(mut item) => {
                //TODO async
                item.push(message).await;
            },
            None => {
                MetricsWriter::inc_queues_count_metric();
                
                let mut storage = MessageStorage::new();

                storage.push(message).await;

                self.storage.insert(queue.into(), storage);
            }
        };
    }
    
    pub async fn pop(&self, queue: &str) -> Option<Message> {  
        match self.storage.get_mut(queue) {
            Some(mut item) => {
              item.pop().await
            },
            None => {
                None
            }
        }
    }
    
    pub async fn commit(&self, queue: &str, id: i64, consumer_id: u32) {
        match self.storage.get_mut(queue) {
            Some(mut guard) => {
                if let Some(_item) = guard.unacked.remove_entry(&id) {
                    info!("commited: queue={}, message_id={}", queue, &id);
                }
            },
            None => {
                warn!("commit failed: queue={}, message_id={}", queue, &id);
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

    pub async fn push(&mut self, message: Message)  {
        let _ = self.semaphore.acquire().await.unwrap();

        self.data.push_back(Message::clone(&message));
    }

    pub async fn pop(&mut self) -> Option<Message> {
        let _ = self.semaphore.acquire().await.unwrap();

        self.data.pop_front()
    }
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
