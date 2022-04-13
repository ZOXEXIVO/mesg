use self::sequence_generator::SequenceGenerator;
use crate::storage::message::Message;
use bytes::Bytes;
use chashmap::CHashMap;
use sled::{Db, IVec};
use std::sync::Arc;
use thiserror::Error;

pub struct Storage {
    store: Arc<CHashMap<String, Db>>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            store: Arc::new(CHashMap::new()),
        }
    }

    pub async fn push(&self, queue: &str, data: Bytes) -> Result<bool, StorageError> {
        let message = Message::new(message_id, data);

        match self.store.get_mut(queue) {
            Some(mut item) => {
                
            },
            None => { 
                
            },
        }
    }
    
    fn get_identity(&self, db: Db) -> u64 {
        const IDENTITY_KEY: &str = "identity";
        
        let identity_value = db.get(IDENTITY_KEY).unwrap();
        
        match identity_value {
            Some(identity) => {
                let updated_val = db
                    .fetch_and_update(IDENTITY_KEY, increment)
                    .unwrap().unwrap();
                
                return 0;
            },
            None => {
                let initial_id: u64 = 0;
                
                db.insert(IDENTITY_KEY, initial_id.to_ne_bytes());
                
                return initial_id;
            }
        }

        fn u64_to_ivec(number: u64) -> IVec {
            IVec::from(number.to_be_bytes().to_vec())
        }
        
        fn increment(old: Option<&[u8]>) -> Option<Vec<u8>> {
            let number = match old {
                Some(bytes) => {
                    let array: [u8; 8] = bytes.try_into().unwrap();
                    let number = u64::from_be_bytes(array);
                    number + 1
                }
                None => 0,
            };

            Some(number.to_be_bytes().to_vec())
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
                let message_store = MessageStore::from_filename(queue.into()).await;

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
    // #[error("Storage lock error")]
    // LockError(#[from] MessageStorageError),
}