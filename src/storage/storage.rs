use crate::storage::utils::StorageIdGenerator;
use std::collections::{VecDeque};

use log::{info, warn};
use bytes::Bytes;
use prost::alloc::collections::{BTreeMap};
use std::cmp::Ordering;
use std::sync::Arc;
use chashmap::CHashMap;
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

    pub async fn push(&self, queue: &str, data: Bytes) -> i64 {
        let message_id = StorageIdGenerator::generate();
        
        let message = Message::new(message_id, data);
              
        match self.storage.get_mut(queue) {
            Some(mut item) => {
                //TODO async
                item.push(message);
            },
            None => {
                MetricsWriter::inc_queues_count_metric();
                
                let mut storage = MessageStorage::new();

                //TODO async
                storage.push(message);

                self.storage.insert(queue.into(), storage);
            }
        };

        message_id
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

pub struct MessageStorage {
    pub data: VecDeque<Message>,
    pub unacked: BTreeMap<i64, Message>,

}

impl MessageStorage {
    pub fn new() -> Self {
        MessageStorage {
            data: VecDeque::new(),
            unacked: BTreeMap::new()
        }
    }

    pub fn push(&mut self, message: Message)  {
        self.data.push_back(Message::clone(&message));
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
