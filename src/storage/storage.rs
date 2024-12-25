use crate::storage::raw::RawFileStorage;
use crate::storage::{MesgInnerStorage, MesgStorageError, Message};
use bytes::Bytes;
use std::path::Path;
use uuid7::Uuid;

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

    pub async fn push(
        &self,
        queue: &str,
        data: Bytes,
        is_broadcast: bool,
    ) -> Result<bool, MesgStorageError> {
        self.inner_storage.push(queue, data, is_broadcast).await
    }

    pub async fn pop(
        &self,
        queue: &str,
        application: &str,
        invisibility_timeout_ms: i32,
    ) -> Result<Option<Message>, MesgStorageError> {
        self.inner_storage
            .pop(queue, application, invisibility_timeout_ms)
            .await
    }

    pub async fn commit(
        &self,
        id: Uuid,
        queue: &str,
        application: &str,
        success: bool,
    ) -> Result<bool, MesgStorageError> {
        self.inner_storage
            .commit(id, queue, application, success)
            .await
    }

    pub async fn revert(
        &self,
        id: Uuid,
        queue: &str,
        application: &str,
    ) -> Result<bool, MesgStorageError> {
        self.inner_storage
            .revert(id, queue, application)
            .await
    }
}
