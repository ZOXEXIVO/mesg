use std::path::{Path, PathBuf};

use crate::metrics::MetricsWriter;
use crate::storage::{FileWrapper};
use std::sync::{Arc, Mutex};
use crate::storage::utils::StorageIdGenerator;
use dashmap::DashMap;
use std::collections::{VecDeque, HashMap};
use crossbeam_channel::{Receiver, Sender};

use log::{info};
use tokio::sync::RwLock;
use bytes::Bytes;

pub struct Message{
    pub id: u64,
    pub data: Bytes
}

impl Message{
    pub fn new(id: u64, data: Vec<u8>) -> Self {
        Message {
            id,
            data: Bytes::copy_from_slice(&data)
        }
    }
}

impl Clone for Message {
    fn clone(&self) -> Self {
        Message {
            id: self.id,
            data: Bytes::clone(&self.data)
        }
    }
}

pub struct MessageStorage {
    pub data: VecDeque<Message>,
    pub sender: Sender<Message>,
    pub receiver: Receiver<Message>
}

pub struct Storage {
    metrics_writer: MetricsWriter,
 
    id_generator: StorageIdGenerator,
    
    store: Arc<Mutex<HashMap<String, MessageStorage>>>
}

impl Storage {
    pub fn new<P: AsRef<Path>>(db_path: P, metrics_writer: MetricsWriter) -> Self {
        Storage {
            id_generator: StorageIdGenerator::new(),
            metrics_writer,
            store: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    pub async fn push(&self, queue_name: String, data: Vec<u8>) {
        let message_id = self.id_generator.generate();
        
        info!("message: id: {} pushed", message_id);
        
        let message = Message::new(message_id, data);

        let mut hash_map = self.store.lock().unwrap();
        
        match hash_map.get_mut(&queue_name) {
            Some(mut item) => {
                item.data.push_back(Message::clone(&message));                
                item.sender.send(message);
            },
            None => {
                let (sender, receiver) = crossbeam_channel::unbounded();
                
                let mut storage = MessageStorage{
                    data: VecDeque::new(),
                    sender,
                    receiver
                };
 
                storage.data.push_back(message);

                hash_map.insert(queue_name,storage);
            }
        };
    }

    pub async fn get_reader(&self, queue_name: String) -> StorageReader {
        info!("consumer connected: queue_name: {}", &queue_name);

        let mut hash_map = self.store.lock().unwrap();
        
        return match hash_map.get_mut(&queue_name) {
            Some(mut item) => {
                StorageReader {
                    receiver: item.receiver.clone()
                }                
            },
            None => {
                let (sender, receiver) = crossbeam_channel::unbounded();
                StorageReader {
                    receiver: receiver.clone()
                }
            }
        };     
    }
    
    async fn add_to_wait_list(&mut self, queue_name: String){
        
 
    }

    pub async fn commit(&self, queue_name: String, message_id: String) {

    }
}

pub struct StorageItem{
    pub id: String,
    pub data: Vec<u8>
}

pub struct StorageReader {
    pub receiver: Receiver<Message>
}
