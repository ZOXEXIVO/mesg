use std::sync::atomic::{Ordering, AtomicU32};
use bytes::Bytes;
use chashmap::{CHashMap};
use tokio::sync::mpsc::{Sender, unbounded_channel, UnboundedSender, Receiver, UnboundedReceiver};
use crate::storage::{Storage};
use crate::controller::{ConsumerItem, MesgConsumer};
use log::{info, error};

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
        let consumer_handle = match self.queue_consumers.get_mut(queue) {
            Some(mut consumer) => {
                consumer.add_consumer()
            }
            None => {
                let mut consumer_collection = ConsumerCollection::new();

                let consumer_handle = consumer_collection.add_consumer();

                self.queue_consumers.insert(queue.into(), consumer_collection);

                consumer_handle
            }
        };

        info!("consumer created for queue={}", queue);

        MesgConsumer::new(consumer_handle.data_receiver, 
                          consumer_handle.shutdown_sender)
    }

    pub async fn push(&self, queue: &str, data: Bytes, broadcast: bool) {
        self.storage.push(queue, Bytes::clone(&data)).await.unwrap();
    }

    pub async fn commit(&self, queue: &str, id: i64, consumer_id: u32) {
        self.storage.commit(queue, id, consumer_id).await;
    }
}

pub struct ConsumerCollection {
    generator: AtomicU32,
    consumers: Vec<Consumer>,
}

impl ConsumerCollection {
    pub fn new() -> Self {
        ConsumerCollection {
            generator: AtomicU32::new(0),
            consumers: Vec::new(),
        }
    }

    pub fn add_consumer(&mut self) -> ConsumerHandle {
        let id = self.generate_id();

        let (consumer_data_sender, consumer_data_receiver) = unbounded_channel();
        let (shutdown_sender, shutdown_receiver) = tokio::sync::mpsc::channel(1);
        
        let consumer = Consumer::new(id, consumer_data_sender, shutdown_receiver);

        self.consumers.push(consumer);
        
        ConsumerHandle {
            data_receiver: consumer_data_receiver,
            shutdown_sender
        }
    }

    fn generate_id(&self) -> u32 {
        self.generator.fetch_add(1, Ordering::SeqCst)
    }
}

pub struct Consumer {
    id: u32,
    send_channel: UnboundedSender<ConsumerItem>,
    shutdown_receiver: Receiver<()>,
}

impl Consumer {
    pub fn new(id: u32, send_channel: UnboundedSender<ConsumerItem>, shutdown_receiver: Receiver<()>) -> Self {
        Consumer {
            id,
            send_channel,
            shutdown_receiver,
        }
    }
}

pub struct ConsumerHandle {
    data_receiver: UnboundedReceiver<ConsumerItem>,
    shutdown_sender: Sender<()>,
}