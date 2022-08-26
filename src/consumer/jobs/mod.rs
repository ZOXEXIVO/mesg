use crate::consumer::jobs::new_events_watcher::NewEventsWatcher;
use crate::consumer::jobs::stale_events_watcher::StaleEventsWatcher;
use crate::consumer::ConsumerDto;
use crate::storage::Storage;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::Notify;

mod new_events_watcher;
mod stale_events_watcher;

pub struct ConsumerJobsCollection {
    storage: Arc<Storage>,
    config: ConsumerConfig,
    data_tx: Sender<ConsumerDto>,
}

impl ConsumerJobsCollection {
    pub fn new(
        storage: Arc<Storage>,
        config: ConsumerConfig,
        data_tx: Sender<ConsumerDto>,
    ) -> Self {
        ConsumerJobsCollection {
            storage,
            config,
            data_tx,
        }
    }

    pub fn start(&self) {
        let consume_wakeup_task = Arc::new(Notify::new());

        NewEventsWatcher::start(
            Arc::clone(&self.storage),
            ConsumerConfig::clone(&self.config),
            Arc::clone(&consume_wakeup_task),
        );

        StaleEventsWatcher::start(
            Arc::clone(&self.storage),
            ConsumerConfig::clone(&self.config),
            Arc::clone(&consume_wakeup_task),
            self.data_tx.clone(),
        );
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
