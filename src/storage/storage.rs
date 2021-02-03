use std::path::{Path};

use crate::metrics::MetricsWriter;
use crate::storage::{StorageReader};
use std::sync::{Arc, Mutex};
use crate::storage::utils::StorageIdGenerator;
use std::collections::{VecDeque, HashMap};
use tokio::sync::mpsc::{Receiver, UnboundedReceiver};

use log::{info};
use bytes::Bytes;
use crate::storage::notifier::{DataNotifiers, DataNotifier};

pub struct Message{
    pub id: u64,
    pub data: Bytes,
    pub delivered: bool
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

pub struct MessageStorage {
    pub data: VecDeque<Message>,
    pub data_notifiers: DataNotifiers
}

impl MessageStorage {
    pub fn new() -> Self {
        MessageStorage {
            data: VecDeque::new(),
            data_notifiers: DataNotifiers::new()
        }
    }
    
    pub fn push(&mut self, message: Message) {
        self.data.push_back(Message::clone(&message));
        self.data_notifiers.send(message);
    }

    pub fn create_reader(&mut self) -> UnboundedReceiver<Message> {
        let (sender, reciever) = tokio::sync::mpsc::unbounded_channel();

        let notifier = DataNotifier::new(sender);
        
        self.data_notifiers.notifiers.push(notifier);

        reciever
    }
}

pub struct Storage {
    metrics_writer: MetricsWriter,
    store: Arc<Mutex<HashMap<String, MessageStorage>>>
}

impl Storage {
    pub fn new(metrics_writer: MetricsWriter) -> Self {
        Storage {            
            metrics_writer,
            store: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    pub async fn push(&self, queue_name: String, data: Vec<u8>) {
        let message_id = StorageIdGenerator::generate();
        
        info!("message: id: {} pushed", message_id);
        
        let message = Message::new(message_id, data);

        let mut store_map = self.store.lock().unwrap();
        
        match store_map.get_mut(&queue_name) {
            Some(item) => {
                item.push(message);
            },
            None => {
                let mut storage = MessageStorage::new();
 
                storage.push(message);

                store_map.insert(queue_name, storage);
            }
        };
    }

    pub fn get_reader(&self, queue_name: String) -> StorageReader {
        info!("consumer connected: queue_name: {}", &queue_name);

        let mut store_lock = self.store.lock().unwrap();
        
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
    
    async fn add_to_wait_list(&mut self, queue_name: String){
        
 
    }

    pub async fn commit(&self, queue_name: String, message_id: String) {

    }
}
