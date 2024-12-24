use std::path::Path;
use bytes::Bytes;

use crate::storage::{MesgInnerStorage, MesgStorageError, Message};

pub struct RawFileStorage {

}

impl MesgInnerStorage for RawFileStorage {
    async fn create<P: AsRef<Path>>(path: P) -> Self {
        RawFileStorage {
            
        }
    }

    async fn push(&self, queue: &str, data: Bytes, is_broadcast: bool) -> Result<bool, MesgStorageError> {
        Ok(true)
    }

    async fn pop(
        &self,
        queue: &str,
        application: &str,
        invisibility_timeout_ms: i32,
    ) -> Result<Option<Message>, MesgStorageError> {
        Ok(None)
    }
    
    
    async fn commit(&self, id: u64, queue: &str, application: &str, success: bool) -> Result<bool, MesgStorageError> {
        Ok(true)
    }
}