use crate::consumer::jobs::new_events_watcher::NewEventsWatcher;
use crate::storage::Storage;
use std::sync::Arc;

mod new_events_watcher;
mod stale_events_watcher;

pub struct ConsumerJobsCollection {
    storage: Arc<Storage>,
    config: ConsumerConfig,
}

impl ConsumerJobsCollection {
    pub fn new(storage: Arc<Storage>, config: ConsumerConfig) -> Self {
        ConsumerJobsCollection { storage, config }
    }

    pub fn start(&self) {
        NewEventsWatcher::st
    }
}

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
