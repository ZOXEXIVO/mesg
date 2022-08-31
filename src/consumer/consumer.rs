use crate::consumer::{ConsumerConfig, ConsumerDto, ConsumerJobsCollection};
use crate::storage::Storage;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub struct Consumer {
    pub id: u32,
    jobs: ConsumerJobsCollection,
}

impl Consumer {
    pub fn new(
        id: u32,
        storage: Arc<Storage>,
        queue: String,
        application: String,
        invisibility_timeout: i32,
        data_tx: Sender<ConsumerDto>,
    ) -> Self {
        let config = ConsumerConfig::new(id, queue, application, invisibility_timeout);

        let mut jobs = ConsumerJobsCollection::new(Arc::clone(&storage), config, data_tx);

        jobs.start();

        Consumer { id, jobs }
    }

    pub async fn shutdown(&self) {
        self.jobs.shutdown();
    }
}
