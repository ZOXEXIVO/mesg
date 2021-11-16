use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicU32};
use bytes::Bytes;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use crate::storage::{Storage};
use crate::controller::{ConsumerCoordinator, ConsumerItem, MesgConsumer};
use log::{info};
use tokio::sync::Mutex;

pub struct MesgController {
    storage: Arc<Storage>,
    consummers: ConsumerCollection
}

impl MesgController {
    pub fn new(storage: Arc<Storage>) -> Self {
        MesgController {
            storage,
            consummers: ConsumerCollection::new()
        }
    }

    pub async fn create_consumer(&self, queue: &str, application: &str) -> MesgConsumer {
        if !self.storage.is_application_queue_exists(queue, application).await {
            self.storage.create_application_queue(queue, application).await;
        }
        
        info!("consumer created for queue={}, application={}", queue, application);
        
        let storage = Arc::clone(&self.storage);
        
        let consumer_handle = self.consummers.add_consumer(storage).await;       

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
    consumers: Arc<Mutex<Vec<Consumer>>>,
    coordinator: ConsumerCoordinator,
    shutdown_tx: UnboundedSender<u32>,
}

impl ConsumerCollection {
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::unbounded_channel();

        let consumers = ConsumerCollection {
            generator: AtomicU32::new(0),
            consumers: Arc::new(Mutex::new(Vec::new())),
            coordinator: ConsumerCoordinator::new(),
            shutdown_tx,
        };

        // Run shutdown waiter
        Self::wait_shutdown(Arc::clone(&consumers.consumers), shutdown_rx);

        let notification = ConsumerAddedNotification::new();

        // Run consuming task
        consumers.coordinator.start(Arc::clone(&consumers.consumers), notification);

        consumers
    }

    pub async fn add_consumer(&self, storage: Arc<Storage>) -> ConsumerHandle {
        let (consumer_data_tx, consumer_data_rx) = unbounded_channel();

        let consumer_id = self.generate_id();

        let mut consumers = self.consumers.lock().await;

        consumers.push(Consumer::new(consumer_id,
                                     Arc::clone(&storage),
                                     consumer_data_tx)
        );

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
    fn wait_shutdown(consumers: Arc<Mutex<Vec<Consumer>>>, mut shutdown_rx: UnboundedReceiver<u32>) {
        info!("shudown waiter started");
        
        tokio::spawn(async move {
            while let Some(consumer_id_to_remove) = shutdown_rx.recv().await {
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
    storage: Arc<Storage>,
    data_tx: UnboundedSender<ConsumerItem>,
    
}

impl Consumer {
    pub fn new(id: u32, storage: Arc<Storage>, data_tx: UnboundedSender<ConsumerItem>) -> Self {
        Consumer {
            id,
            storage,
            data_tx,
        }
    }
}

pub struct ConsumerHandle {
    pub id: u32,
    pub data_rx: UnboundedReceiver<ConsumerItem>,
    pub shutdown_tx: UnboundedSender<u32>,
}

pub struct ConsumerAddedNotification {}

impl ConsumerAddedNotification {
    pub fn new() -> Self {
        ConsumerAddedNotification {}
    }
}