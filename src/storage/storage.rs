use self::sequence_generator::SequenceGenerator;
use crate::storage::message::Message;
use crate::storage::message_store::{MessageStorageError, MessageStore};
use bytes::Bytes;
use chashmap::CHashMap;
use std::sync::Arc;
use thiserror::Error;

pub struct Storage {
    store: Arc<CHashMap<String, MessageStore>>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            store: Arc::new(CHashMap::new()),
        }
    }

    pub async fn push(&self, queue: &str, data: Bytes) -> Result<bool, StorageError> {
        let message_id = SequenceGenerator::generate();

        let message = Message::new(message_id, data);

        match self.store.get_mut(queue) {
            Some(mut item) => Ok(item.push(message).await?),
            None => Ok(false),
        }
    }

    pub async fn pop(
        &self,
        queue: &str,
        application: &str,
        invisibility_timeout: u32,
    ) -> Option<Message> {
        if let Some(mut store) = self.store.get_mut(queue) {
            if let Ok(msg) = store.pop(application, invisibility_timeout).await {
                return msg;
            }
        }

        None
    }

    pub async fn commit(&self, id: i64, queue: &str, application: &str, success: bool) -> bool {
        match success {
            true => self.commit_inner(id, queue, application).await,
            false => self.uncommit_inner(id, queue, application).await,
        }
    }

    pub async fn commit_inner(&self, id: i64, queue: &str, application: &str) -> bool {
        if let Some(mut store) = self.store.get_mut(queue) {
            if let Ok(msg) = store.commit(application, id).await {
                return msg;
            }
        }

        false
    }

    pub async fn uncommit_inner(&self, id: i64, queue: &str, application: &str) -> bool {
        if let Some(mut store) = self.store.get_mut(queue) {
            if let Ok(res) = store.uncommit(application, id).await {
                return res;
            }
        }

        false
    }

    async fn is_application_queue_exists(&self, queue: &str, application: &str) -> bool {
        if let Some(store) = self.store.get(queue) {
            return store.is_application_exists(application).await;
        }

        false
    }

    pub async fn create_application_queue(&self, queue: &str, application: &str) -> bool {
        if self.is_application_queue_exists(queue, application).await {
            return false;
        }

        match self.store.get_mut(queue) {
            Some(store) => {
                return store.create_application_queue(application).await;
            }
            None => {
                let message_store = MessageStore::new();

                let result = message_store.create_application_queue(application).await;

                self.store.insert(queue.into(), message_store);

                result
            }
        }
    }
}

impl Clone for Storage {
    fn clone(&self) -> Self {
        Storage {
            store: Arc::clone(&self.store),
        }
    }
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Storage lock error")]
    LockError(#[from] MessageStorageError),
}

mod sequence_generator {
    use std::sync::atomic::{AtomicI64, Ordering};

    static CURRENT_SEQUENCE_VALUE: AtomicI64 = AtomicI64::new(0);

    pub struct SequenceGenerator;

    impl SequenceGenerator {
        pub fn generate() -> i64 {
            CURRENT_SEQUENCE_VALUE.fetch_add(1, Ordering::Relaxed)
        }
    }
}
