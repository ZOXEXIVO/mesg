use crate::consumer::{ConsumerCollection, ConsumerDto, RawConsumer};
use crate::controller::jobs::BackgroundJobs;
use crate::storage::Storage;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, UnboundedSender};

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

    pub async fn create_consumer(
        &self,
        queue: &str,
        application: &str,
        invisibility_timeout: i32,
    ) -> RawConsumer {
        let storage = Arc::clone(&self.storage);

        // add consumer
        let consumer_handle = self
            .consumers
            .add_consumer(storage, queue, application, invisibility_timeout)
            .await;

        RawConsumer::from(consumer_handle)
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

    pub fn start_jobs(&self) {
        self.background_jobs.start();
    }
}
