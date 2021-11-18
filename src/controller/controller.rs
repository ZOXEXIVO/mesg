use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicU32};
use std::time::Duration;
use bytes::Bytes;
use tokio::sync::mpsc::{channel, UnboundedSender, UnboundedReceiver, Sender, Receiver};
use crate::storage::{Storage};
use crate::controller::{ConsumerCoordinator, ConsumerItem, MesgConsumer};
use log::{info};
use tokio::sync::{RwLock};

pub struct MesgController {
    storage: Arc<Storage>,
    consummers: ConsumerCollection,
}

impl MesgController {
    pub fn new(storage: Arc<Storage>) -> Self {
        MesgController {
            storage,
            consummers: ConsumerCollection::new(),
        }
    }

    pub async fn create_consumer(&self, queue: &str, application: &str) -> MesgConsumer {
        if !self.storage.is_application_queue_exists(queue, application).await {
            self.storage.create_application_queue(queue, application).await;
        }

        info!("consumer created for queue={}, application={}", queue, application);

        let storage = Arc::clone(&self.storage);

        let consumer_handle = self.consummers.add_consumer(storage, queue, application).await;

        MesgConsumer::from(consumer_handle)
    }

    pub async fn push(&self, queue: &str, data: Bytes) -> bool {
        self.storage.push(queue, Bytes::clone(&data)).await.unwrap()
    }

    pub async fn commit(&self, id: i64, queue: &str, application: &str) -> bool {
        self.storage.commit(id, queue, application).await
    }
}

pub struct ConsumerCollection {
    generator: AtomicU32,
    consumers: Arc<RwLock<Vec<Consumer>>>,
    shutdown_tx: UnboundedSender<u32>,
}

impl ConsumerCollection {
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::unbounded_channel();

        let consumers = ConsumerCollection {
            generator: AtomicU32::new(0),
            consumers: Arc::new(RwLock::new(Vec::new())),
            shutdown_tx,
        };

        let consumers_ref = Arc::clone(&consumers.consumers);

        tokio::spawn(async move {
            // Run consuming task
            ConsumerCoordinator::start(consumers_ref).await;
        });

        // Run shutdown waiter
        Self::wait_shutdown(Arc::clone(&consumers.consumers), shutdown_rx);

        consumers
    }

    pub async fn add_consumer(&self, storage: Arc<Storage>, queue: &str, application: &str) -> ConsumerHandle {
        let (consumer_data_tx, consumer_data_rx) = channel(1024);

        let consumer_id = self.generate_id();

        let mut consumers = self.consumers.write().await;

        let consumer = Consumer::new(consumer_id,
                                     Arc::clone(&storage),
                                     String::from(queue),
                                     String::from(application),
                                     consumer_data_tx,
        );

        consumers.push(consumer);

        ConsumerHandle {
            id: consumer_id,
            data_rx: consumer_data_rx,
            shutdown_tx: self.shutdown_tx.clone(),
        }
    }

    fn generate_id(&self) -> u32 {
        self.generator.fetch_add(1, Ordering::SeqCst)
    }

    // waiting consumers shutdown
    fn wait_shutdown(consumers: Arc<RwLock<Vec<Consumer>>>, mut shutdown_rx: UnboundedReceiver<u32>) {
        info!("shudown waiter started");

        tokio::spawn(async move {
            while let Some(consumer_id_to_remove) = shutdown_rx.recv().await {
                let mut consumers_guard = consumers.write().await;

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
    id: u32
}

impl Consumer {
    pub fn new(id: u32, storage: Arc<Storage>, queue: String, application: String, data_tx: Sender<ConsumerItem>) -> Self {
        let consumer = Consumer {
            id
        };

        info!("start consumer polling task");
        
        tokio::spawn(async move {
            loop {
                if let Some(messsage) = storage.pop(&queue, &application, 30000).await {
                    let consumer_item = ConsumerItem {
                        id: messsage.id,
                        data: Bytes::clone(&messsage.data)
                    };

                    info!("message polled");

                    data_tx.send(consumer_item).await;
                }else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
                
        });
        
        consumer
    }
}

pub struct ConsumerHandle {
    pub id: u32,
    pub data_rx: Receiver<ConsumerItem>,
    pub shutdown_tx: UnboundedSender<u32>,
}