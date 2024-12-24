pub mod raw;

use bytes::Bytes;
use std::path::Path;
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
        id: u64,
        queue: &str,
        application: &str,
        success: bool,
    ) -> Result<bool, MesgStorageError>;
}

#[derive(Debug)]
pub enum MesgStorageError {}
