pub mod raw;

use bytes::Bytes;
use std::path::Path;
use uuid::Uuid;
use crate::storage::Message;

pub trait MesgInnerStorage {
    async fn create<P: AsRef<Path>>(path: P) -> Self;

    async fn push(
        &self,
        queue: &str,
        data: Bytes,
        is_broadcast: bool,
    ) -> Result<bool, MesgStorageError>;

    async fn pop(
        &self,
        queue: &str,
        application: &str,
        invisibility_timeout_ms: i32,
    ) -> Result<Option<Message>, MesgStorageError>;

    async fn commit(
        &self,
        id: Uuid,
        queue: &str,
        application: &str
    ) -> Result<bool, MesgStorageError>;

    async fn rollback(
        &self,
        id: Uuid,
        queue: &str,
        application: &str,
    ) -> Result<bool, MesgStorageError>;
}

#[derive(Debug)]
pub struct MesgStorageError {
    message: String
}

