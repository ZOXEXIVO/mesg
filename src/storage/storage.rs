use crate::metrics::MetricsWriter;
use crate::storage::{StorageReader};
use crate::storage::utils::StorageIdGenerator;
use std::collections::{VecDeque, HashMap};
use tokio::sync::mpsc::{UnboundedReceiver};

use log::{info, warn};
use bytes::Bytes;
use crate::storage::notifier::{DataSubscribers, DataSubscriber};
use prost::alloc::collections::{BTreeMap};
use std::cmp::Ordering;
use std::thread::JoinHandle;
use parking_lot::{Mutex};

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
    queue_storages: Mutex<HashMap<String, MessageStorage>>
}

impl Storage {
    pub fn new(metrics_writer: MetricsWriter) -> Self {
        Storage {            
            metrics_writer,
            queue_storages: Mutex::new(HashMap::new())
        }
    }

    pub async fn push(&self, queue_name: String, data: Vec<u8>) {
        let message_id = StorageIdGenerator::generate();
        
        info!("message: id: {} pushed", message_id);
        
        let message = Message::new(message_id, data);

        let mut locked_storage = self.queue_storages.lock();
        
        match locked_storage.get_mut(&queue_name) {
            Some(item) => {
                item.push(message);
            },
            None => {
                let mut storage = MessageStorage::new();
 
                storage.push(message);

                locked_storage.insert(queue_name, storage);
            }
        };
    }

    pub fn get_reader(&self, queue_name: String) -> StorageReader {
        info!("consumer connected: queue_name: {}", &queue_name);

        let mut store_lock = self.queue_storages.lock();
        
        return match store_lock.get_mut(&queue_name) {
            Some(item) => {
                StorageReader {
                    receiver: item.create_reader()
                }                
            },
            None => {
                let mut storage = MessageStorage::new();

                let reader = StorageReader {
                    receiver: storage.create_reader()
                };
                
                store_lock.insert(queue_name, storage);

                reader
            }
        };     
    }

    pub async fn commit(&self, queue_name: String, message_id: String) {
        let mut store_map = self.queue_storages.lock();
        
        match store_map.get_mut(&queue_name) {
            Some(message_storage) => {
                if let Some(item) = message_storage.unacked.remove_entry(&message_id) {
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
    pub subscribers: DataSubscribers,
    pub worker_thread: Option<JoinHandle<()>>
}

impl MessageStorage {
    pub fn new() -> Self {
        MessageStorage {
            data: VecDeque::new(),
            unacked: BTreeMap::new(),
            subscribers: DataSubscribers::new(),
            worker_thread: None
        }
    }

    pub fn push(&mut self, message: Message) {
        self.data.push_back(Message::clone(&message));
        self.subscribers.send(message);
    }

    pub fn create_reader(&mut self) -> UnboundedReceiver<Message> {
        let (sender, reciever) = tokio::sync::mpsc::unbounded_channel();

        let notifier = DataSubscriber::new(sender);

        self.subscribers.notifiers.push(notifier);

        reciever
    }
}