use crate::storage::message::Message;
use bytes::Bytes;
use chashmap::CHashMap;
use sled::{Db, IVec};
use std::sync::Arc;
use structopt::clap::App;
use thiserror::Error;
use crate::storage::NameUtils;

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
        return match self.store.get_mut(queue) {
            Some(db) => {
                let (identity_val, identity_vec) = get_identity(&db);
                let message = Message::new(identity_val, data);

                for tree in &db.tree_names() {
                    db.open_tree(tree)
                        .unwrap()
                        .insert(&identity_vec, message.clone().data.to_vec())
                        .unwrap();
                }

                Ok(true)
            }
            None => Ok(true),
        };

        fn get_identity(db: &Db) -> (u64, IVec) {
            const IDENTITY_KEY: &str = "identity";

            let mut current_value = 0;

            let new_value = db
                .fetch_and_update(IDENTITY_KEY, |old| {
                    let number = match old {
                        Some(bytes) => {
                            let array: [u8; 8] = bytes.try_into().unwrap();
                            let number = u64::from_be_bytes(array);
                            number + 1
                        }
                        None => 0,
                    };

                    current_value = number;

                    Some(number.to_be_bytes().to_vec())
                })
                .unwrap()
                .unwrap();

            (current_value, new_value)
        }
    }

    fn u64_to_ivec(number: u64) -> IVec {
        IVec::from(number.to_be_bytes().to_vec())
    }

    pub async fn pop(&self, queue: &str, application: &str) -> Option<Message> {
        if let Some(db) = self.store.get_mut(queue) {
            let queue_names = NameUtils::application(application);
            
            let tree = db.open_tree(queue_names.default()).unwrap();
            let uncommitted_tree = db.open_tree(queue_names.uncommited()).unwrap();

            if let Ok(popped_item) = tree.pop_min() {
                if let Some((k, v)) = popped_item {
                    uncommitted_tree.insert(k.clone(), v.clone()).unwrap();

                    let key_bytes: Vec<u8> = k.to_vec();
                    let val_bytes: Vec<u8> = v.to_vec();

                    let id = u64::from_be_bytes(key_bytes.try_into().unwrap());

                    return Some(Message::new(id, Bytes::from(val_bytes)));
                }
            }
        }

        None
    }

    pub async fn commit(&self, id: u64, queue: &str, application: &str, success: bool) -> bool {
        match success {
            true => self.commit_inner(id, queue, application).await,
            false => self.uncommit_inner(id, queue, application).await,
        }
    }

    pub async fn commit_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        if let Some(db) = self.store.get_mut(queue) {
            let queue_names = NameUtils::application(application);

            let uncommitted_tree = db.open_tree(queue_names.uncommited()).unwrap();

            let id_vec = Self::u64_to_ivec(id);

            if let Ok(removed) = uncommitted_tree.remove(id_vec) {
                if removed.is_some() {
                    return true;
                }
            }
        }

        false
    }

    pub async fn uncommit_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        if let Some(db) = self.store.get_mut(queue) {
            let queue_names = NameUtils::application(application);
            
            let tree = db.open_tree(queue_names.default()).unwrap();
            let uncommitted_tree = db.open_tree(queue_names.uncommited()).unwrap();

            let id_vec = Self::u64_to_ivec(id);

            if let Ok(removed) = uncommitted_tree.remove(id_vec.clone()) {
                if let Some(removed_message) = removed {
                    tree.insert(id_vec, removed_message).unwrap();
                }
            }
        }

        false
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
