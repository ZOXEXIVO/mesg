use crate::storage::{MesgInnerStorage, MesgStorageError};
use bytes::Bytes;
use std::path::Path;
use crate::storage::raw::RawFileStorage;

pub type MesgStorage = Storage<RawFileStorage>;

pub struct Storage<S: MesgInnerStorage> {
    inner_storage: S,
}

impl<S: MesgInnerStorage> Storage<S> {
    pub async fn new<P: AsRef<Path>>(base_path: P) -> Self {
        Storage {
            inner_storage: S::create(base_path).await,
        }
    }

    pub async fn create_consumer_queue(&self, queue: &str, application: &str) {
        self.inner_storage
            .create_consumer_queue(queue, application)
            .await
    }

    pub async fn push(
        &self,
        queue: &str,
        data: Bytes,
        is_broadcast: bool,
    ) -> Result<bool, MesgStorageError> {
        self.inner_storage.push(queue, data, is_broadcast).await
    }

    pub async fn commit(
        &self,
        id: u64,
        queue: &str,
        application: &str,
        success: bool,
    ) -> Result<bool, MesgStorageError> {
        self.inner_storage
            .commit(id, queue, application, success)
            .await
    }
}
