use std::sync::atomic::{AtomicUsize, Ordering};
use bytes::Bytes;
use chashmap::{CHashMap};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use crate::storage::{Storage};
use crate::controller::{ConsumerItem, MesgConsumer};
use log::{info};

pub struct MesgController {
    storage: Storage,
    consumers: CHashMap<String, ConsumerBag>,
}

impl MesgController {
    pub fn new(storage: Storage) -> Self {
        MesgController {
            storage,
            consumers: CHashMap::new(),
        }
    }

    pub fn create_consumer(&self, queue: &str) -> MesgConsumer {
        let (sender, reciever) = unbounded_channel();

        match self.consumers.get_mut(queue) {
            Some(mut consumer) => {
                consumer.add_consumer(sender);

                info!("create new consumer_list for queue={}", queue);
            }
            None => {
                let mut consumer = ConsumerBag::new();

                consumer.add_consumer(sender);

                self.consumers.insert(queue.into(), consumer);

                info!("use existing consumer_list for queue={}", queue);
            }
        }

        info!("consumer created for queue={}", queue);
        
        MesgConsumer::new(reciever)
    }

    pub async fn push(&self, queue: &str, data: Bytes, broadcast: bool) {
        let id = self.storage.push(queue, Bytes::clone(&data)).await;
        self.consume(queue, id, data, broadcast);
    }

    pub async fn commit(&self, queue: &str, id: i64) {
        self.storage.commit(queue, id).await;
    }

    fn consume(&self, queue: &str, id: i64, data: Bytes, broadcast: bool) {
        let mut consumers_to_remove = Vec::new();

        {
            if let Some(consumer) = self.consumers.get(queue) {
                if broadcast {
                    info!("use broadcast send. id={}", id);
                    for (idx, item) in consumer.consumers.iter().enumerate() {
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
                                consumers_to_remove.push(idx);
                            }
                        }
                    }
                } else {
                    info!("use direct send. id={}", id);
                    
                    let consumer_item = ConsumerItem {
                        id,
                        data: Bytes::clone(&data),
                    };

                    let next_consumer = consumer.next_consumer();

                    match consumer.consumers.get(next_consumer) {
                        Some(consumer) => {
                            match consumer.send(consumer_item) {
                                Ok(_) => {
                                    info!("direct: message id={}, delivered success", id);
                                }
                                Err(_) => {
                                    info!("direct: message id={}, delivery error", id);
                                    consumers_to_remove.push(next_consumer);
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
        if !consumers_to_remove.is_empty() {
            info!("consumers to remove non empty, queue={}", queue);
            
            if let Some(mut consumer) = self.consumers.get_mut(queue) {
                for consumer_to_remove_id in consumers_to_remove {
                    consumer.remove_consumer(consumer_to_remove_id);
                    info!("consumer removed, queue={}, idx={}", queue, consumer_to_remove_id);
                }
            }
        }
    }
}

pub struct ConsumerBag {
    current_consumer: AtomicUsize,
    pub consumers: Vec<UnboundedSender<ConsumerItem>>,
}

impl ConsumerBag {
    pub fn new() -> Self {
        ConsumerBag {
            current_consumer: AtomicUsize::new(0),
            consumers: Vec::new(),
        }
    }

    pub fn add_consumer(&mut self, consumer: UnboundedSender<ConsumerItem>) {
        self.consumers.push(consumer)
    }

    pub fn remove_consumer(&mut self, idx: usize) {
        self.consumers.remove(idx);
        self.current_consumer.store(0, Ordering::SeqCst)
    }

    pub fn next_consumer(&self) -> usize {
         self.current_consumer.fetch_add(1, Ordering::SeqCst) % self.consumers.len()        
    }
}


