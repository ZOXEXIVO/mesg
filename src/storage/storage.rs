use crate::storage::identity::Identity;
use crate::storage::message::Message;
use crate::storage::QueueNames;
use bytes::Bytes;
use chrono::Utc;
use log::{error, info};
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
                let queues = QueueNames::from(queue, application);

                info!("subscribed to queue={}", queues.ready());

                db.open_tree(queues.ready()).unwrap().watch_prefix(vec![])
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
                // generate message id
                let (identity_val, identity_vec) = Identity::get(db, queue);

                // store data
                db.open_tree(queue)
                    .unwrap()
                    .insert(&identity_vec, data.to_vec())
                    .unwrap();

                info!("store message data, id={}, queue={}", identity_val, queue);

                // store message ids
                match is_broadcast {
                    true => broadcast_send(db, identity_val, identity_vec),
                    false => direct_send(db, identity_val, identity_vec),
                }
            })
            .await;

        return match result {
            Ok(res) => Ok(res),
            Err(msg) => Err(StorageError::Generic(msg)),
        };

        fn broadcast_send(db: &Db, identity_val: u64, identity_vec: IVec) -> bool {
            let mut pushed = false;

            for queue in ready_queue_names(db) {
                info!(
                    "broadcast store message, id={}, queue={}",
                    identity_val, queue
                );

                db.open_tree(queue)
                    .unwrap()
                    .insert(&identity_vec, vec![])
                    .unwrap();

                pushed = true;
            }

            pushed
        }

        fn direct_send(db: &Db, identity_val: u64, identity_vec: IVec) -> bool {
            let queue = random_queue_name(db);

            info!("direct store message, id={}, queue={}", identity_val, queue);

            db.open_tree(queue)
                .unwrap()
                .insert(&identity_vec, vec![])
                .unwrap();

            true
        }

        fn get_user_queues(db: &Db) -> Vec<String> {
            db.tree_names()
                .into_iter()
                .filter(|n| n != b"__sled__default")
                .map(|q| String::from_utf8(q.to_vec()).unwrap())
                .filter(|q| QueueNames::is_ready(q))
                .collect()
        }

        fn ready_queue_names(db: &Db) -> Vec<String> {
            get_user_queues(db)
        }

        fn random_queue_name(db: &Db) -> String {
            let items = get_user_queues(db);

            let mut rng = thread_rng();

            let n = rng.gen_range(0..items.len());

            items[n].clone()
        }
    }

    pub async fn pop(
        &self,
        queue: &str,
        application: &str,
        invisibility_timeout: u32,
    ) -> Option<Message> {
        let result = self
            .execute_in_context(queue, move |db| {
                let queue_names = QueueNames::from(queue, application);

                let ready_queue = db.open_tree(queue_names.ready()).unwrap();

                if let Ok(Some((k, _))) = ready_queue.pop_min() {
                    // calculate item expire time
                    let now_millis = Utc::now().timestamp_millis();
                    let expire_time_millis = now_millis + invisibility_timeout as i64 * 1000;

                    let unack_queue = db.open_tree(queue_names.unack()).unwrap();

                    unack_queue
                        .insert(
                            k.clone(),
                            IVec::from(expire_time_millis.to_be_bytes().to_vec()),
                        )
                        .unwrap();

                    let message_data = db.open_tree(queue).unwrap();

                    let key_bytes: Vec<u8> = k.to_vec();

                    let message_data = message_data.get(k).unwrap().unwrap();
                    let val_bytes: Vec<u8> = message_data.to_vec();

                    let id = u64::from_be_bytes(key_bytes.try_into().unwrap());

                    info!("pop: get message data, id={}, queue={}", id, queue);

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

    pub async fn commit(&self, id: u64, queue: &str, application: &str, success: bool) -> bool {
        match success {
            true => self.commit_inner(id, queue, application).await,
            false => self.revert_inner(id, queue, application).await,
        }
    }

    pub async fn commit_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        let result = self
            .execute_in_context(queue, move |db| {
                let queue_names = QueueNames::from(queue, application);

                let unack_queue = db.open_tree(queue_names.unack()).unwrap();

                let id_vec = IVec::from(id.to_be_bytes().to_vec());

                if let Ok(removed) = unack_queue.remove(id_vec.clone()) {
                    if removed.is_some() {
                        info!(
                            "commit: message removed, id={}, queue={}",
                            id,
                            queue_names.unack()
                        );

                        let data_queue = db.open_tree(queue).unwrap();

                        data_queue.remove(id_vec).unwrap();

                        info!("commit: message data removed, id={}, queue={}", id, queue);

                        return true;
                    }
                }

                false
            })
            .await;

        result.unwrap_or(false)
    }

    pub async fn revert_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        let result = self
            .execute_in_context(queue, move |db| {
                let queue_names = QueueNames::from(queue, application);

                let unack_queue = db.open_tree(queue_names.unack()).unwrap();

                let id_vec = IVec::from(id.to_be_bytes().to_vec());

                if let Ok(Some(removed_message)) = unack_queue.remove(id_vec.clone()) {
                    info!(
                        "revert: message removed, id={}, queue={}",
                        id,
                        queue_names.unack()
                    );

                    let ready_queue = db.open_tree(queue_names.ready()).unwrap();

                    ready_queue.insert(id_vec, removed_message).unwrap();

                    info!(
                        "revert: message id inserted, id={}, queue={}",
                        id,
                        queue_names.ready()
                    );

                    return true;
                }

                false
            })
            .await;

        result.unwrap_or(false)
    }

    pub async fn try_revert(&self, queue: &str, application: &str) -> bool {
        false
        //self.execute_in_context(queue, move |db| db.transaction(|tx| tx.generate_id()))
    }

    async fn execute_in_context<F: Fn(&Db) -> R, R>(
        &self,
        queue: &str,
        action: F,
    ) -> Result<R, String> {
        let read_lock = self.store.read().await;

        if let Some(db) = read_lock.map.get(queue) {
            let result = action(db);
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
                let queue_names = QueueNames::from(queue, application);

                db.open_tree(queue_names.data()).unwrap();
                db.open_tree(queue_names.unack()).unwrap();
                db.open_tree(queue_names.ready()).unwrap();

                info!(
                    "application queue created, queue={}, unack={}, ready={}, application={}",
                    queue_names.data(),
                    queue_names.unack(),
                    queue_names.ready(),
                    application
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
