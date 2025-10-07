use crate::consumer::{ConsumerCollection, RawConsumer};
use crate::controller::jobs::BackgroundJobs;
use crate::storage::{MesgStorage};
use bytes::Bytes;
use std::sync::Arc;
use uuid::Uuid;

pub struct MesgController {
    storage: Arc<MesgStorage>,
    consumers: ConsumerCollection,
    background_jobs: BackgroundJobs,
}

impl MesgController {
    pub fn new(storage: Arc<MesgStorage>) -> Self {
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

    pub async fn commit(&self, id: Uuid, queue: &str, application: &str) -> bool {
        self.storage.commit(id, queue, application).await.unwrap()
    }

    pub async fn rollback(&self, id: Uuid, queue: &str, application: &str) -> bool {
        self.storage.rollback(id, queue, application).await.unwrap()
    }

    pub fn start_jobs(&self) {
        self.background_jobs.start();
    }
}
