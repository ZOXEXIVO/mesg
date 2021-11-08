use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicU32};
use bytes::Bytes;
use chashmap::{CHashMap};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use crate::storage::{Storage};
use crate::controller::{ConsumerCoordinator, ConsumerItem, MesgConsumer};
use log::{info};
use tokio::sync::Mutex;

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

    pub async fn create_consumer(&self, queue: &str) -> MesgConsumer {
        let consumer_handle = match self.queue_consumers.get_mut(queue) {
            Some(mut consumer) => {
                consumer.add_consumer().await
            }
            None => {
                let mut consumer_collection = ConsumerCollection::new();

                let consumer_handle = consumer_collection.add_consumer().await;

                self.queue_consumers.insert(queue.into(), consumer_collection);

                consumer_handle
            }
        };

        info!("consumer created for queue={}", queue);

        MesgConsumer::from(consumer_handle)
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
    consumers: Arc<Mutex<Vec<Consumer>>>,
    coordinator: ConsumerCoordinator,
    shutdown_sender: UnboundedSender<u32>,
}

impl ConsumerCollection {
    pub fn new() -> Self {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

        let consumers = ConsumerCollection {
            generator: AtomicU32::new(0),
            consumers: Arc::new(Mutex::new(Vec::new())),
            coordinator: ConsumerCoordinator::new(),
            shutdown_sender: sender,
        };

        // Run shutdown waiter
        Self::shutdown_waiter_start(Arc::clone(&consumers.consumers), receiver);
        // Run consuming task
        consumers.coordinator.start(Arc::clone(&consumers.consumers));

        consumers
    }

    pub async fn add_consumer(&mut self) -> ConsumerHandle {
        let (consumer_data_sender, consumer_data_receiver) = unbounded_channel();

        let consumer_id = self.generate_id();

        let mut consumers = self.consumers.lock().await;

        consumers.push(
            Consumer::new(consumer_id, consumer_data_sender)
        );

        ConsumerHandle {
            id: consumer_id,
            data_receiver: consumer_data_receiver,
            shutdown_sender: self.shutdown_sender.clone(),
        }
    }

    fn generate_id(&self) -> u32 {
        self.generator.fetch_add(1, Ordering::SeqCst)
    }


    fn shutdown_waiter_start(consumers: Arc<Mutex<Vec<Consumer>>>, mut receiver: UnboundedReceiver<u32>) {
        tokio::spawn(async move {
            while let Some(consumer_id_to_remove) = receiver.recv().await {
                let mut consumers_guard = consumers.lock().await;

                match consumers_guard.iter().position(|c| c.id == consumer_id_to_remove) {
                    Some(consumer_pos) => {
                        consumers_guard.remove(consumer_pos);
                        info!("consumer_id={} removed", consumer_id_to_remove)
                    }
                    None => {
                        info!("cannot remove consumer_id={}", consumer_id_to_remove)
                    }
                }
            }
        });
    }
}

pub struct Consumer {
    id: u32,
    channel: UnboundedSender<ConsumerItem>,
}

impl Consumer {
    pub fn new(id: u32, channel: UnboundedSender<ConsumerItem>) -> Self {
        Consumer {
            id,
            channel,
        }
    }
}

pub struct ConsumerHandle {
    pub id: u32,
    pub data_receiver: UnboundedReceiver<ConsumerItem>,
    pub shutdown_sender: UnboundedSender<u32>,
}