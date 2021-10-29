use std::sync::atomic::{AtomicUsize, Ordering, AtomicU32};
use bytes::Bytes;
use chashmap::{CHashMap};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use crate::storage::{Storage};
use crate::controller::{ConsumerItem, MesgConsumer};
use log::{info};
use std::fmt::{Debug};
use thiserror::Error;

pub struct MesgController {
    storage: Storage,
    router: ConsumerRouter,
    consumers: CHashMap<String, ConsumerBag>,
}

impl MesgController {
    pub fn new(storage: Storage) -> Self {
        MesgController {
            storage,
            router: ConsumerRouter::new(),
            consumers: CHashMap::new(),
        }
    }

    pub fn create_consumer(&self, queue: &str) -> MesgConsumer {
        let (sender, reciever) = unbounded_channel();

        match self.consumers.get_mut(queue) {
            Some(mut consumer) => {
                consumer.add_consumer(sender);
            }
            None => {
                let mut consumer = ConsumerBag::new();

                consumer.add_consumer(sender);

                self.consumers.insert(queue.into(), consumer);
            }
        }

        info!("consumer created for queue={}", queue);
        
        MesgConsumer::new(reciever)
    }

    pub async fn push(&self, queue: &str, data: Bytes, broadcast: bool) {
        let id = self.storage.push(queue, Bytes::clone(&data)).await;
        // TODO Move
        self.consume(queue, id, Bytes::clone(&data), broadcast).unwrap();
    }

    pub async fn commit(&self, queue: &str, id: i64, consumer_id: u32) {
        self.storage.commit(queue, id, consumer_id).await;
    }

    fn consume(&self, queue: &str, id: i64, data: Bytes, broadcast: bool) -> Result<(), ConsumingError> {
        let mut consumers_ids_to_remove = Vec::new();
    
        {
            if let Some(queue_consumer) = self.consumers.get(queue) {
                if broadcast {
                    for (consumer_id, item) in &queue_consumer.consumers {
                        let consumer_item = ConsumerItem {
                            id,
                            data: Bytes::clone(&data),
                            consumer_id: *consumer_id
                        };
    
                        match item.send(consumer_item) {
                            Ok(_) => {
                                info!("broadcast: message id={}, delivered success", id);
                            }
                            Err(_) => {
                                info!("broadcast: message id={}, delivery error", id);
                                consumers_ids_to_remove.push(*consumer_id);
                            }
                        }
                    }
                } else {                    
                    let next_consumer = self.router.get_next_consumer(&queue_consumer.consumers);
                    if next_consumer.is_none() {
                        return Err(ConsumingError::NoConsumers);
                    }

                    let (next_consumer_id, next_consumer_channel) = next_consumer.unwrap();

                    let consumer_item = ConsumerItem {
                        id,
                        data: Bytes::clone(&data),
                        consumer_id: *next_consumer_id
                    };
                    
                    match next_consumer_channel.send(consumer_item) {
                        Ok(_) => {
                            info!("direct: message id={}, delivered success", id);
                        }
                        Err(_) => {
                            info!("direct: message id={}, delivery error", id);
                            consumers_ids_to_remove.push(*next_consumer_id);
                        }
                    }
                }
            }
        }
    
        // Remove
        if !consumers_ids_to_remove.is_empty() {
            if let Some(mut consumer) = self.consumers.get_mut(queue) {
                consumer.remove_consumers(&consumers_ids_to_remove);
            }
        }
    
        Ok(())
    }
}

type Consumer = (u32, UnboundedSender<ConsumerItem>);

pub struct ConsumerBag {
    generator: AtomicU32,
    pub consumers: Vec<Consumer>,
}

impl ConsumerBag {
    pub fn new() -> Self {
        ConsumerBag {
            generator: AtomicU32::new(0),
            consumers: Vec::new(),
        }
    }

    pub fn add_consumer(&mut self, consumer: UnboundedSender<ConsumerItem>) {
        let id = self.generator.fetch_add(1, Ordering::SeqCst);
        self.consumers.push((id, consumer));
    }

    pub fn remove_consumers(&mut self, ids: &[u32]) {
        for id in ids {
            let idx = self.consumers.iter().position(|(c_id, _)| c_id == id).unwrap();
            self.consumers.remove(idx);
        }
    }
}

pub struct ConsumerRouter {
    current_consumer: AtomicUsize
}

impl ConsumerRouter {
    pub fn new() -> Self {
        ConsumerRouter {
            current_consumer: AtomicUsize::new(0)
        }
    }
    
    pub fn get_next_consumer<'c>(&self, consumers: &'c [Consumer]) -> Option<&'c Consumer> {
        if consumers.is_empty() {
            return None;
        }
        
        let mut next_idx = self.current_consumer.fetch_add(1, Ordering::SeqCst);
        if next_idx >= consumers.len() {
            next_idx = 0;
            self.current_consumer.store(0, Ordering::SeqCst);
        }

        Some(&consumers[next_idx])
    }
}

#[derive(Error, Debug)]
pub enum ConsumingError {
    #[error("data store disconnected")]
    NoConsumers,
}
