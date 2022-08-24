use crate::controller::jobs::BackgroundJobs;
use crate::controller::{Consumer, ConsumerItem, ConsumersShutdownWaiter, MesgConsumer};
use crate::storage::Storage;
use bytes::Bytes;
use log::info;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, UnboundedSender};
use tokio::sync::RwLock;

pub struct MesgController {
    storage: Arc<Storage>,
    consumers: ConsumerCollection,
    background_jobs: BackgroundJobs,
}

impl MesgController {
    pub fn new(storage: Arc<Storage>) -> Self {
        MesgController {
            storage: Arc::clone(&storage),
            consumers: ConsumerCollection::new(),
            background_jobs: BackgroundJobs::new(Arc::clone(&storage)),
        }
    }

    pub fn start_jobs(&self) {
        self.background_jobs.start();
    }

    pub async fn create_consumer(
        &self,
        queue: &str,
        application: &str,
        invisibility_timeout: u32,
    ) -> MesgConsumer {
        let storage = Arc::clone(&self.storage);

        // add consumer
        let consumer_handle = self
            .consumers
            .add_consumer(storage, queue, application, invisibility_timeout)
            .await;

        MesgConsumer::from(consumer_handle)
    }

    pub async fn push(&self, queue: &str, data: Bytes, is_broadcast: bool) -> bool {
        self.storage
            .push(queue, Bytes::clone(&data), is_broadcast)
            .await
            .unwrap()
    }

    pub async fn commit(&self, id: u64, queue: &str, application: &str, success: bool) -> bool {
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

        info!(
            "consumer created, consumer_id={}, queue={}, application={}",
            consumer_id, queue, application
        );

        ConsumerHandle {
            id: consumer_id,
            queue: queue.into(),
            application: String::from(application),
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
    pub application: String,
    pub data_rx: Receiver<ConsumerItem>,
    pub shutdown_tx: UnboundedSender<u32>,
}
