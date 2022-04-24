use crate::storage::identity::Identity;
use crate::storage::message::Message;
use crate::storage::{NameUtils, QueueNames};
use bytes::Bytes;
use chashmap::CHashMap;
use log::{error, info};
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
            let default_queue_name = IVec::from(queue_names.default());

            if db.tree_names().contains(&default_queue_name) {
                let original_queue = db.open_tree(queue_names.default()).unwrap();
                return Some(original_queue.watch_prefix(vec![]));
            }
        }

        None
    }

    pub async fn push(&self, queue: &str, data: Bytes) -> Result<bool, StorageError> {
        let result = self
            .execute_in_context(queue, move |db| {
                let (identity_val, identity_vec) = Identity::get(db, queue);
                let message = Message::new(identity_val, Bytes::clone(&data));

                let original_queue_names: Vec<IVec> = db
                    .tree_names()
                    .into_iter()
                    .filter(|n| n != b"__sled__default")
                    .collect();

                let mut pushed = false;

                for queue_name in original_queue_names
                    .iter()
                    .map(|qn| String::from_utf8(qn.to_vec()).unwrap())
                {
                    if QueueNames::is_unacked(&queue_name) {
                        continue;
                    }

                    let queue_name_copy = queue_name.clone();

                    let original_queue = db.open_tree(queue_name).unwrap();

                    original_queue
                        .insert(&identity_vec, message.clone().data.to_vec())
                        .unwrap();

                    info!(
                        "message pushed, message_id={}, queue={}, application={}, remaining_items={}, sum={}",
                        message.id,
                        queue,
                        queue_name_copy,
                        original_queue.len(),
                        original_queue.checksum().unwrap()
                    );

                    pushed = true;
                }

                pushed
            })
            .await;

        match result {
            Ok(res) => Ok(res),
            Err(msg) => Err(StorageError::Generic(msg)),
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

                    info!(
                        "try pop success, message_id={}, queue={}, application={}, remaining_items={}",
                        id, queue, application, original_queue.len()
                    );

                    return Some(Message::new(id, Bytes::from(val_bytes)));
                }

                info!(
                    "try pop failed, queue={}, application={}, remaining_items={}",
                    queue, application, original_queue.len()
                );

                None
            })
            .await;

        match result {
            Ok(res) => res,
            Err(_) => None,
        }
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
                        info!(
                            "removed form unacked queue, message_id={}, queue={}, application={}",
                            id, queue, application
                        );
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

                let original_queue = db.open_tree(queue_names.default()).unwrap();
                let unacked_queue = db.open_tree(queue_names.unacked()).unwrap();

                let id_vec = IVec::from(id.to_be_bytes().to_vec());

                if let Ok(Some(removed_message)) = unacked_queue.remove(id_vec.clone()) {
                    original_queue.insert(id_vec, removed_message).unwrap();
                    info!(
                        "returned to original_queue, message_id={}, queue={}, application={}",
                        id, queue, application
                    );
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
        if let Some(db) = self.store.get(queue) {
            let result = action(&db);

            if let Err(error) = db.flush_async().await {
                error!("flush error: queue: {}, Error={}", queue, error)
            }

            return Ok(result);
        }

        let _ = self.write_mutex.lock().await;

        // double check
        if let Some(db) = self.store.get(queue) {
            let result = action(&db);

            flush(&db).await;

            return Ok(result);
        }

        if let Ok(db) = sled::open(format!("{queue}.mesg")) {
            let result = action(&db);

            flush(&db).await;

            self.store.insert(String::from(queue), db);

            return Ok(result);
        } else if let Some(db) = self.store.get(queue) {
            let result = action(&db);

            flush(&db).await;

            return Ok(result);
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

                let _ = db.open_tree(queue_names.default()).unwrap();
                let _ = db.open_tree(queue_names.unacked()).unwrap();

                info!(
                    "application queue created, queue={}, appplication={}",
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
            write_mutex: Arc::clone(&self.write_mutex),
        }
    }
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Storage generic error")]
    Generic(String),
}
