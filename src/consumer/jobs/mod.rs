use crate::consumer::ConsumerDto;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use crate::storage::MesgStorage;

pub struct ConsumerJobsCollection {
    storage: Arc<MesgStorage>,
    config: ConsumerConfig,
    data_tx: Sender<ConsumerDto>,
    jobs_handles: Vec<JoinHandle<()>>,
}

impl ConsumerJobsCollection {
    pub fn new(
        storage: Arc<MesgStorage>,
        config: ConsumerConfig,
        data_tx: Sender<ConsumerDto>,
    ) -> Self {
        ConsumerJobsCollection {
            storage,
            config,
            data_tx,
            jobs_handles: Vec::new(),
        }
    }

    pub fn start(&mut self) {
        let consume_wakeup_task = Arc::new(Notify::new());
    }

    pub fn shutdown(&self) {
        for handle in &self.jobs_handles {
            handle.abort()
        }
    }
}

#[derive(Clone)]
pub struct ConsumerConfig {
    consumer_id: u32,
    queue: String,
    application: String,
    invisibility_timeout: i32,
}

impl ConsumerConfig {
    pub fn new(
        consumer_id: u32,
        queue: String,
        application: String,
        invisibility_timeout: i32,
    ) -> Self {
        ConsumerConfig {
            consumer_id,
            queue,
            application,
            invisibility_timeout,
        }
    }
}
