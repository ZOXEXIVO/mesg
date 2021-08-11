use crate::metrics::MetricsWriter;
use crate::storage::{StorageReader};
use crate::storage::utils::StorageIdGenerator;
use std::collections::{VecDeque};

use log::{info, warn};
use bytes::Bytes;
use prost::alloc::collections::{BTreeMap};
use std::cmp::Ordering;
use std::thread::JoinHandle;
use parking_lot::{Condvar};
use std::sync::Arc;
use chashmap::CHashMap;
use tokio::sync::watch::{Receiver, Sender};
use tokio::sync::watch;

pub struct Message{
    pub id: u64,
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
    pub fn new(id: u64, data: Vec<u8>) -> Self {
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
            id: self.id,
            data: Bytes::clone(&self.data),
            delivered: self.delivered
        }
    }
}

pub struct Storage {
    metrics_writer: MetricsWriter,
    storage: Arc<CHashMap<String, MessageStorage>>,
    condvar: Arc<Condvar>
}

impl Storage {
    pub fn new(metrics_writer: MetricsWriter) -> Self {
        Storage {            
            metrics_writer,
            storage: Arc::new(CHashMap::new()),
            condvar: Arc::new(Condvar::new())
        }
    }

    pub async fn push(&self, queue_name: String, data: Vec<u8>) {
        let message_id = StorageIdGenerator::generate();
        
        info!("message: id: {} pushed", message_id);
               
        let message = Message::new(message_id, data);
              
        match self.storage.get_mut(&queue_name) {
            Some(mut item) => {
                item.push(message);
            },
            None => {
                let mut storage = MessageStorage::new();
 
                storage.push(message);

                self.storage.insert(queue_name, storage);
            }
        };
    }

    pub fn get_reader(&self, queue_name: String) -> StorageReader {
        info!("consumer connected: queue_name: {}", &queue_name);

        StorageReader {
            queue_name,
            storage: Arc::clone(&self.storage),
            condvar: Arc::clone(&self.condvar)
        }
    }

    pub async fn commit(&self, queue_name: String, message_id: String) {
        match self.storage.get_mut(&queue_name) {
            Some(mut guard) => {
                if let Some(item) = guard.unacked.remove_entry(&message_id) {
                    info!("commited: queue_name={}, message_id={}", &queue_name, &message_id);
                }
            },
            None => {
                warn!("commit failed: queue_name={}, message_id={}", &queue_name, &message_id);
            }
        };
    }
}

pub struct MessageStorage {
    pub data: VecDeque<Message>,
    pub unacked: BTreeMap<String, Message>,
    pub worker_thread: Option<JoinHandle<()>>,
    pub notification: (Sender<()>, Receiver<()>)
}

impl MessageStorage {
    pub fn new() -> Self {
        MessageStorage {
            data: VecDeque::new(),
            unacked: BTreeMap::new(),
            worker_thread: None,
            notification: watch::channel(())
        }
    }

    pub fn push(&mut self, message: Message) {
        self.data.push_back(Message::clone(&message));
        self.notify();
    }
    
    fn notify(&self) {
        let (sender, _) = &self.notification;
        sender.send(()).unwrap_or(())       
    }
}

pub struct MessageReader{
    
}