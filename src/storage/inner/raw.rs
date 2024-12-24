use std::path::Path;
use bytes::Bytes;

use crate::storage::{MesgInnerStorage, MesgStorageError};

pub struct RawFileStorage {

}

impl MesgInnerStorage for RawFileStorage {
    async fn create<P: AsRef<Path>>(path: P) -> Self {
        RawFileStorage {
            
        }
    }

    async fn create_consumer_queue(&self, queue: &str, application: &str) {

    }

    async fn push(&self, queue: &str, data: Bytes, is_broadcast: bool) -> Result<bool, MesgStorageError> {
        Ok(true)
    }

    async fn commit(&self, id: u64, queue: &str, application: &str, success: bool) -> Result<bool, MesgStorageError> {
        Ok(true)
    }
}