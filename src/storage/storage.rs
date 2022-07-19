use crate::storage::identity::Identity;
use crate::storage::message::Message;
use crate::storage::{NameUtils, QueueNames};
use bytes::Bytes;
use log::{error, info};
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use sled::{Db, IVec, Subscriber};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

pub struct Storage {
    store: Arc<RwLock<StorageData>>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            store: Arc::new(RwLock::new(StorageData::new())),
        }
    }

    pub async fn subscribe(&self, queue: &str, application: &str) -> Option<Subscriber> {
        let result = self
            .execute_in_context(queue, move |db| {
                let queue_names = NameUtils::application(application);

                db.open_tree(queue_names.default())
                    .unwrap()
                    .watch_prefix(vec![])
            })
            .await;

        if let Ok(res) = result {
            return Some(res);
        }

        None
    }

    pub async fn push(
        &self,
        queue: &str,
        data: Bytes,
        is_broadcast: bool,
    ) -> Result<bool, StorageError> {
        let result = self
            .execute_in_context(queue, move |db| {
                let (identity_val, identity_vec) = Identity::get(db, queue);
                let message = Message::new(identity_val, Bytes::clone(&data));

                match is_broadcast {
                    true => broadcast_send(db, identity_vec, message),
                    false => direct_send(db, identity_vec, message),
                }
            })
            .await;

        return match result {
            Ok(res) => Ok(res),
            Err(msg) => Err(StorageError::Generic(msg)),
        };

        fn broadcast_send(db: &Db, identity_vec: IVec, message: Message) -> bool {
            let mut pushed = false;

            for queue_name in queue_names(db) {
                let queue_names = NameUtils::application(&queue_name);

                db.open_tree(queue_names.default())
                    .unwrap()
                    .insert(&identity_vec, Message::clone(&message).data.to_vec())
                    .unwrap();

                pushed = true;
            }

            pushed
        }

        fn direct_send(db: &Db, identity_vec: IVec, message: Message) -> bool {
            let queues_name = random_queue_name(db);
            let queue_names = NameUtils::application(&queues_name);

            db.open_tree(queue_names.default())
                .unwrap()
                .insert(&identity_vec, Message::clone(&message).data.to_vec())
                .unwrap();

            true
        }

        fn queue_names(db: &Db) -> Vec<String> {
            db.tree_names()
                .into_iter()
                .filter(|n| n != b"__sled__default")
                .map(|q| String::from_utf8(q.to_vec()).unwrap())
                .filter(|q| !QueueNames::is_unacked(q))
                .collect()
        }

        fn random_queue_name(db: &Db) -> String {
            let items: Vec<String> = db
                .tree_names()
                .into_iter()
                .filter(|n| n != b"__sled__default")
                .map(|q| String::from_utf8(q.to_vec()).unwrap())
                .filter(|q| !QueueNames::is_unacked(q))
                .collect();

            let mut rng = thread_rng();

            let n = rng.gen_range(0..items.len());

            items[n].clone()
        }
    }

    pub async fn pop(&self, queue: &str, application: &str) -> Option<Message> {
        let result = self
            .execute_in_context(queue, move |db| {
                let queue_names = NameUtils::application(application);

                let original_queue = db.open_tree(queue_names.default()).unwrap();
                let unacked_queue = db.open_tree(queue_names.unacked()).unwrap();

                if let Ok(Some((k, v))) = original_queue.pop_min() {
                    unacked_queue.insert(k.clone(), v.clone()).unwrap();

                    let key_bytes: Vec<u8> = k.to_vec();
                    let val_bytes: Vec<u8> = v.to_vec();

                    let id = u64::from_be_bytes(key_bytes.try_into().unwrap());
                    let value = Bytes::from(val_bytes);

                    return Some(Message::new(id, value));
                }

                None
            })
            .await;

        if let Ok(res) = result {
            return res;
        }

        None
    }

    pub async fn ack(&self, id: u64, queue: &str, application: &str, success: bool) -> bool {
        match success {
            true => self.ack_inner(id, queue, application).await,
            false => self.unack_inner(id, queue, application).await,
        }
    }

    pub async fn ack_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        let result = self
            .execute_in_context(queue, move |db| {
                let queue_names = NameUtils::application(application);

                let unacked_queue = db.open_tree(queue_names.unacked()).unwrap();

                let id_vec = IVec::from(id.to_be_bytes().to_vec());

                if let Ok(removed) = unacked_queue.remove(id_vec) {
                    if removed.is_some() {
                        return true;
                    }
                }

                false
            })
            .await;

        result.unwrap_or(false)
    }

    pub async fn unack_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        let result = self
            .execute_in_context(queue, move |db| {
                let queue_names = NameUtils::application(application);

                let unacked_queue = db.open_tree(queue_names.unacked()).unwrap();

                let id_vec = IVec::from(id.to_be_bytes().to_vec());

                if let Ok(Some(removed_message)) = unacked_queue.remove(id_vec.clone()) {
                    let original_queue = db.open_tree(queue_names.default()).unwrap();

                    original_queue.insert(id_vec, removed_message).unwrap();

                    return true;
                }

                false
            })
            .await;

        result.unwrap_or(false)
    }

    async fn execute_in_context<F: Fn(&Db) -> R, R>(
        &self,
        queue: &str,
        action: F,
    ) -> Result<R, String> {
        let read_lock = self.store.read().await;

        if let Some(db) = read_lock.map.get(queue) {
            let result = action(db);

            flush(db).await;

            return Ok(result);
        } else {
            drop(read_lock);

            let mut write_lock = self.store.write().await;

            if let Ok(db) = sled::open(format!("{queue}.mesg")) {
                let result = action(&db);

                flush(&db).await;

                write_lock.map.insert(String::from(queue), db);

                return Ok(result);
            } else {
                drop(write_lock);

                let read_lock = self.store.read().await;

                if let Some(db) = read_lock.map.get(queue) {
                    let result = action(db);

                    flush(db).await;

                    return Ok(result);
                }
            }
        }

        return Err(format!("Cannot create queue={}", queue));

        async fn flush(db: &Db) {
            if let Err(error) = db.flush_async().await {
                error!("flush error: {}", error)
            }
        }
    }

    pub async fn create_application_queue(&self, queue: &str, application: &str) -> bool {
        let result = self
            .execute_in_context(queue, move |db| {
                let queue_names = NameUtils::application(application);

                db.open_tree(queue_names.default()).unwrap();
                db.open_tree(queue_names.unacked()).unwrap();

                info!(
                    "application queue created, queue={}, application={}",
                    queue, application
                );

                true
            })
            .await;

        result.unwrap_or(false)
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
    #[error("Storage generic error")]
    Generic(String),
}

struct StorageData {
    map: HashMap<String, Db>,
}

impl StorageData {
    pub fn new() -> Self {
        StorageData {
            map: HashMap::new(),
        }
    }
}
