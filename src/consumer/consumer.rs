use crate::consumer::{ConsumerConfig, ConsumerDto, ConsumerJobsCollection};
use crate::storage::Storage;
use log::{error, info};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::Notify;

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
        let consume_wakeup_task = Arc::new(Notify::new());

        let config = ConsumerConfig::new(id, queue, application, invisibility_timeout);

        let mut jobs = ConsumerJobsCollection::new(Arc::clone(&storage), config);

        jobs.start();

        Consumer { id, jobs }
    }
}
