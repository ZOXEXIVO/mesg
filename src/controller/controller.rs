use std::sync::atomic::{AtomicUsize, Ordering, AtomicU32};
use bytes::Bytes;
use chashmap::{CHashMap};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use crate::storage::{Storage};
use crate::controller::{ConsumerItem, MesgConsumer};
use log::{info};
use std::error::Error;
use std::fmt::{Debug, Formatter, Display};

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
        
        match self.consume(queue, id, data, broadcast) {
            Ok(_) => {}
            Err(_) => {}
        }
    }

    pub async fn commit(&self, queue: &str, id: i64) {
        self.storage.commit(queue, id).await;
    }

    fn consume(&self, queue: &str, id: i64, data: Bytes, broadcast: bool) -> Result<(), ConsumeError> {
        let mut consumers_ids_to_remove = Vec::new();

        {
            if let Some(consumer) = self.consumers.get(queue) {
                if broadcast {
                    for (idx, (consumer_id, item)) in consumer.consumers.iter().enumerate() {
                        let consumer_item = ConsumerItem {
                            id,
                            data: Bytes::clone(&data),
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
                    let consumer_item = ConsumerItem {
                        id,
                        data: Bytes::clone(&data),
                    };
                    
                    let next_consumer_index = self.router.get_next_consumer_id(&consumer.consumers);
                    if next_consumer_index.is_none() {
                        return Err(ConsumeError::from("no_consumers"));
                    }
                    
                    let next_idx = next_consumer_index.unwrap();
                    
                    match consumer.consumers.get(next_idx) {
                        Some((_, cs)) => {
                            match cs.send(consumer_item) {
                                Ok(_) => {
                                    info!("direct: message id={}, delivered success", id);
                                }
                                Err(_) => {
                                    info!("direct: message id={}, delivery error", id);
                                    consumers_ids_to_remove.push(consumer.consumers[next_idx].0);
                                }
                            }
                        }
                        None => {
                            info!("direct: message id={}, delivery error: no consumers found", id);                            
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
    
    pub fn get_next_consumer_id(&self, consumers: &[Consumer]) -> Option<usize> {
        if consumers.is_empty() {
            return None;
        }
        
        let mut next_idx = self.current_consumer.fetch_add(1, Ordering::SeqCst);
        if next_idx >= consumers.len() {
            next_idx = 0;
            self.current_consumer.store(0, Ordering::SeqCst);
        }

        Some(next_idx)
    }
}

pub struct ConsumeError {
    error_message: String
}

impl Debug for ConsumeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Consuming error")
    }
}

impl Display for ConsumeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Consuming error")
    }
}

impl Error for ConsumeError {
    
}

impl From<&str> for ConsumeError {
    fn from(error_message: &str) -> Self {
        ConsumeError {
            error_message: error_message.into()
        }
    }
}