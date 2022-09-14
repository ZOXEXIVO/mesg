use crate::consumer::{Consumer, ConsumerHandle, ConsumersShutdownWaiter};
use crate::storage::Storage;
use log::info;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{channel, UnboundedSender};
use tokio::sync::RwLock;

pub struct ConsumerCollection {
    id_generator: AtomicU32,
    consumers: Arc<RwLock<Vec<Consumer>>>,
    shutdown_tx: UnboundedSender<u32>,
}

impl ConsumerCollection {
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::unbounded_channel();

        let consumers = ConsumerCollection {
            id_generator: AtomicU32::new(0),
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
        invisibility_timeout_ms: i32,
    ) -> ConsumerHandle {
        let (consumer_data_tx, consumer_data_rx) = channel(4096);

        let consumer_id = self.generate_id();

        let mut consumers = self.consumers.write().await;

        let consumer = Consumer::new(
            consumer_id,
            Arc::clone(&storage),
            String::from(queue),
            String::from(application),
            invisibility_timeout_ms,
            consumer_data_tx,
        );

        consumers.push(consumer);

        info!(
            "consumer[id={}] created, queue={}, application={}",
            consumer_id, queue, application
        );

        ConsumerHandle {
            id: consumer_id,
            queue: String::from(queue),
            application: String::from(application),
            data_rx: consumer_data_rx,
            shutdown_tx: self.shutdown_tx.clone(),
        }
    }

    fn generate_id(&self) -> u32 {
        self.id_generator.fetch_add(1, Ordering::SeqCst)
    }
}
