use crate::storage::message::Message;
use crate::storage::NameUtils;
use bytes::Bytes;
use chashmap::CHashMap;
use sled::{Db, IVec, Subscriber};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

pub struct Storage {
    store: Arc<CHashMap<String, Db>>,
    write_mutex: Arc<Mutex<()>>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            store: Arc::new(CHashMap::new()),
            write_mutex: Arc::new(Mutex::new(())),
        }
    }

    pub async fn subscribe(&self, queue: &str, application: &str) -> Option<Subscriber> {
        if let Some(db) = self.store.get_mut(queue) {
            let queue_names = NameUtils::application(application);

            let tree = db.open_tree(queue_names.default()).unwrap();

            return Some(tree.watch_prefix(vec![]));
        }

        None
    }

    pub async fn push(&self, queue: &str, data: Bytes) -> Result<bool, StorageError> {
        match self.store.get(queue) {
            Some(db) => {
                push_internal(&db, data).await;
            }
            None => {
                let _ = self.write_mutex.lock().await;

                if let Some(db) = self.store.get(queue) {
                    push_internal(&db, data).await;
                    return Ok(true);
                }

                if let Ok(db) = sled::open(format!("{queue}.mesg")) {
                    push_internal(&db, data).await;
                    self.store.insert(String::from(queue), db);
                } else if let Some(db) = self.store.get(queue) {
                    push_internal(&db, data).await;
                    return Ok(true);
                }
            }
        };

        return Ok(true);

        async fn push_internal(db: &Db, data: Bytes) {
            let (identity_val, identity_vec) = get_identity(db);
            let message = Message::new(identity_val, data);

            for tree in &db.tree_names() {
                db.open_tree(tree)
                    .unwrap()
                    .insert(&identity_vec, message.clone().data.to_vec())
                    .unwrap();
            }
        }

        fn get_identity(db: &sled::Db) -> (u64, IVec) {
            const IDENTITY_KEY: &str = "identity";

            let mut current_value = 0;

            let identity_value = db
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
                .unwrap();

            match identity_value {
                Some(identity_val) => (current_value, identity_val),
                None => (
                    current_value,
                    IVec::from(current_value.to_be_bytes().to_vec()),
                ),
            }
        }
    }

    pub async fn pop(&self, queue: &str, application: &str) -> Option<Message> {
        return match self.store.get_mut(queue) {
            Some(db) => {
                let queue_names = NameUtils::application(application);

                let tree = db.open_tree(queue_names.default()).unwrap();
                let uncommitted_tree = db.open_tree(queue_names.uncommited()).unwrap();

                if let Ok(Some((k, v))) = tree.pop_min() {
                    uncommitted_tree.insert(k.clone(), v.clone()).unwrap();

                    let key_bytes: Vec<u8> = k.to_vec();
                    let val_bytes: Vec<u8> = v.to_vec();

                    let id = u64::from_be_bytes(key_bytes.try_into().unwrap());

                    return Some(Message::new(id, Bytes::from(val_bytes)));
                }

                return None;
            }
            None => None,
        };
    }

    pub async fn commit(&self, id: u64, queue: &str, application: &str, success: bool) -> bool {
        match success {
            true => self.commit_inner(id, queue, application).await,
            false => self.uncommit_inner(id, queue, application).await,
        }
    }

    pub async fn commit_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        match self.store.get_mut(queue) {
            Some(db) => {
                let queue_names = NameUtils::application(application);

                let uncommitted_tree = db.open_tree(queue_names.uncommited()).unwrap();

                let id_vec = IVec::from(id.to_be_bytes().to_vec());

                if let Ok(removed) = uncommitted_tree.remove(id_vec) {
                    if removed.is_some() {
                        return true;
                    }
                }

                false
            }
            None => false,
        }
    }

    pub async fn uncommit_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        match self.store.get_mut(queue) {
            Some(db) => {
                let queue_names = NameUtils::application(application);

                let tree = db.open_tree(queue_names.default()).unwrap();
                let uncommitted_tree = db.open_tree(queue_names.uncommited()).unwrap();

                let id_vec = IVec::from(id.to_be_bytes().to_vec());

                if let Ok(Some(removed_message)) = uncommitted_tree.remove(id_vec.clone()) {
                    tree.insert(id_vec, removed_message).unwrap();
                }

                false
            }
            None => false,
        }
    }
}

impl Clone for Storage {
    fn clone(&self) -> Self {
        Storage {
            store: Arc::clone(&self.store),
            write_mutex: Arc::clone(&self.write_mutex),
        }
    }
}

#[derive(Error, Debug)]
pub enum StorageError {
    // #[error("Storage lock error")]
// LockError(#[from] MessageStorageError),
}
