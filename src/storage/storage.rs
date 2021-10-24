use crate::storage::utils::StorageIdGenerator;
use std::collections::{VecDeque};

use log::{debug, info, warn};
use bytes::Bytes;
use prost::alloc::collections::{BTreeMap};
use std::cmp::Ordering;
use std::sync::Arc;
use chashmap::CHashMap;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::broadcast;
use crate::controller::ConsumerItem;
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

    pub async fn push(&self, queue: &str, data: Vec<u8>) {
        let message = Message::new(StorageIdGenerator::generate(), data);
              
        match self.storage.get_mut(queue) {
            Some(mut item) => {
                item.push(message);
            },
            None => {
                MetricsWriter::inc_queues_count_metric();
                
                let mut storage = MessageStorage::new();
 
                storage.push(message);

                self.storage.insert(queue.into(), storage);
            }
        };
    }

    pub fn subscribe(&self, queue: &str) -> Receiver<ConsumerItem> {
        match self.storage.get_mut(queue) {
            Some(guard) => {
                guard.subscribe()
            },
            None => {
                MetricsWriter::inc_queues_count_metric();
                
                let storage = MessageStorage::new();
                
                let reciever = storage.subscribe();
                    
                self.storage.insert(queue.into(), storage);

                reciever
            }
        }
    }
    
    pub async fn commit(&self, queue: &str, id: i64) {
        match self.storage.get_mut(queue) {
            Some(mut guard) => {
                if let Some(_item) = guard.unacked.remove_entry(&id) {
                    debug!("commited: queue={}, message_id={}", queue, &id);
                }
            },
            None => {
                warn!("commit failed: queue={}, message_id={}", queue, &id);
            }
        };
    }
}

pub struct MessageStorage {
    pub data: VecDeque<Message>,
    pub unacked: BTreeMap<i64, Message>,
    pub consumers: (Sender<ConsumerItem>, Receiver<ConsumerItem>)
}

impl MessageStorage {
    pub fn new() -> Self {
        MessageStorage {
            data: VecDeque::new(),
            unacked: BTreeMap::new(),
            consumers: broadcast::channel(1024)
        }
    }

    pub fn push(&mut self, message: Message) {
        self.data.push_back(Message::clone(&message));
        self.send_notification(message);
    }
    
    pub fn subscribe(&self) -> Receiver<ConsumerItem> {
        let (tx, _) = &self.consumers;
        tx.subscribe()
    }
    
    fn send_notification(&self, message: Message) {
        let (tx, _) = &self.consumers;

        tx.send(ConsumerItem{
            id: message.id,
            data: Bytes::clone(&message.data)
        });
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
    pub fn new(id: i64, data: Vec<u8>) -> Self {
        Message {
            id,
            data: Bytes::copy_from_slice(&data),
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
