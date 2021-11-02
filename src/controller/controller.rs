use std::sync::atomic::{Ordering, AtomicU32};
use bytes::Bytes;
use chashmap::{CHashMap};
use tokio::sync::mpsc::{Sender, unbounded_channel, UnboundedSender};
use crate::storage::{Storage};
use crate::controller::{ConsumerItem, MesgConsumer};
use log::{info};
use std::fmt::{Debug};
use thiserror::Error;
use tokio::task::JoinHandle;

pub struct MesgController {
    storage: Storage,
    queue_consumers: CHashMap<String, ConsumerCollection>,
}

impl MesgController {
    pub fn new(storage: Storage) -> Self {
        MesgController {
            storage,
            queue_consumers: CHashMap::new(),
        }
    }

    pub fn create_consumer(&self, queue: &str) -> MesgConsumer {
        let (sender, reciever) = unbounded_channel();

        let cloned_storage = self.storage.clone();

        let consumer_shutdown_sender = match self.queue_consumers.get_mut(queue) {
            Some(mut consumer) => {
                consumer.add_consumer(cloned_storage, queue, sender)
            }
            None => {
                let mut consumer_collection = ConsumerCollection::new();

                let consumer_shudown_sender = consumer_collection.add_consumer(cloned_storage, queue, sender);

                self.queue_consumers.insert(queue.into(), consumer_collection);

                consumer_shudown_sender                
            }
        };

        info!("consumer created for queue={}", queue);

        MesgConsumer::new(reciever, consumer_shutdown_sender)
    }

    pub async fn push(&self, queue: &str, data: Bytes, broadcast: bool) {
        self.storage.push(queue, Bytes::clone(&data)).await;
    }

    pub async fn commit(&self, queue: &str, id: i64, consumer_id: u32) {
        self.storage.commit(queue, id, consumer_id).await;
    }
}

pub struct Consumer {
    id: u32,
    sender: UnboundedSender<ConsumerItem>,
    worker_task: Option<JoinHandle<()>>
}

pub struct ConsumerCollection {
    generator: AtomicU32,
    pub consumers: Vec<Consumer>
}

impl ConsumerCollection {
    pub fn new() -> Self {
        ConsumerCollection {
            generator: AtomicU32::new(0),
            consumers: Vec::new() 
        }
    }

    pub fn add_consumer(&mut self, storage: Storage, queue: &str, consumer_sender: UnboundedSender<ConsumerItem>) -> Sender<()> {
        let (shutdown_sender, mut shutdown_reciever) = tokio::sync::mpsc::channel(1);

        let id = self.generator.fetch_add(1, Ordering::SeqCst);
        
        let consumer_channel = consumer_sender.clone();
        
        let queue_name: String = queue.into();
        
        self.consumers.push(Consumer {
            id,
            sender: consumer_sender,
            worker_task: Some(tokio::spawn(async move {
                loop {
                    if shutdown_reciever.try_recv().is_ok() {
                        info!("consumer recieved shutdown signal");
                        break;
                    }

                    if let Some(msg) = storage.pop(&queue_name).await {
                        info!("consumer: {}, message recieved", id);
                        
                        let consumer_item = ConsumerItem{
                            id: msg.id,
                            data: Bytes::clone(&msg.data),
                            consumer_id: id
                        };
                        
                        match consumer_channel.send(consumer_item) {
                            Ok(_) => {
                                info!("consumer: {}, message pushed", id);
                            },
                            Err(err) => {
                                info!("consumer: {}, error while pushing message: {}", id, err);
                                storage.push(&queue_name, err.0.data).await;
                            }
                        }
                    }
                }
            }))
        });

        shutdown_sender
    }
}

#[derive(Error, Debug)]
pub enum ConsumingError {
    #[error("data store disconnected")]
    NoConsumers,
}
