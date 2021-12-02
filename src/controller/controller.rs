use crate::controller::{
    Consumer, ConsumerCoordinator, ConsumerItem, ConsumersShutdownWaiter, MesgConsumer,
};
use crate::storage::Storage;
use bytes::Bytes;
use log::info;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, UnboundedSender};
use tokio::sync::RwLock;

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

    pub async fn create_consumer(
        &self,
        queue: &str,
        application: &str,
        invisibility_timeout: u32,
    ) -> MesgConsumer {
        self.storage
            .create_application_queue(queue, application)
            .await;

        info!(
            "consumer created for queue={}, application={}",
            queue, application
        );

        let storage = Arc::clone(&self.storage);

        let consumer_handle = self
            .consummers
            .add_consumer(storage, queue, application, invisibility_timeout)
            .await;

        MesgConsumer::from(consumer_handle)
    }

    pub async fn push(&self, queue: &str, data: Bytes) -> bool {
        self.storage.push(queue, Bytes::clone(&data)).await.unwrap()
    }

    pub async fn commit(&self, id: i64, queue: &str, application: &str, success: bool) -> bool {
        self.storage.commit(id, queue, application, success).await
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

        ConsumerCoordinator::start(Arc::clone(&consumers.consumers));

        // Run shutdown waiter
        ConsumersShutdownWaiter::wait(Arc::clone(&consumers.consumers), shutdown_rx);

        consumers
    }

    pub async fn add_consumer(
        &self,
        storage: Arc<Storage>,
        queue: &str,
        application: &str,
        invisibility_timeout: u32,
    ) -> ConsumerHandle {
        let (consumer_data_tx, consumer_data_rx) = channel(1024);

        let consumer_id = self.generate_id();

        let mut consumers = self.consumers.write().await;

        let consumer = Consumer::new(
            consumer_id,
            Arc::clone(&storage),
            String::from(queue),
            String::from(application),
            invisibility_timeout,
            consumer_data_tx,
        );

        consumers.push(consumer);

        ConsumerHandle {
            id: consumer_id,
            queue: queue.into(),
            data_rx: consumer_data_rx,
            shutdown_tx: self.shutdown_tx.clone(),
        }
    }

    fn generate_id(&self) -> u32 {
        self.generator.fetch_add(1, Ordering::SeqCst)
    }
}

pub struct ConsumerHandle {
    pub id: u32,
    pub queue: String,
    pub data_rx: Receiver<ConsumerItem>,
    pub shutdown_tx: UnboundedSender<u32>,
}
