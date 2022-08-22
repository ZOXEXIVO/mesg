use crate::storage::identity::Identity;
use crate::storage::message::Message;
use crate::storage::QueueNames;
use bytes::Bytes;
use chrono::Utc;
use log::{error, info};
use rand::{thread_rng, Rng};
use sled::{Db, IVec, Subscriber};
use std::sync::Arc;
use thiserror::Error;

pub struct Storage {
    store: Arc<Db>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            store: Arc::new(sled::open(format!("data.mesg")).unwrap()),
        }
    }

    pub async fn subscribe(&self, queue: &str, application: &str) -> Subscriber {
        let queues = QueueNames::from_application(application);

        info!("subscribed to queue={}", queues.ready());

        self.store
            .open_tree(queues.ready())
            .unwrap()
            .watch_prefix(vec![])
    }

    pub async fn push(
        &self,
        queue: &str,
        data: Bytes,
        is_broadcast: bool,
    ) -> Result<bool, StorageError> {
        // generate message id
        let (identity_val, identity_vec) = Identity::get(&self.store, queue);

        // store data
        self.store
            .open_tree(queue)
            .unwrap()
            .insert(&identity_vec, data.to_vec())
            .unwrap();

        info!("store message data, id={}, queue={}", identity_val, queue);

        // store message ids
        if is_broadcast {
            let mut pushed = false;

            for queue in ready_queue_names(&self.store) {
                info!(
                    "broadcast store message, id={}, queue={}",
                    identity_val, queue
                );

                self.store
                    .open_tree(queue)
                    .unwrap()
                    .insert(&identity_vec, vec![])
                    .unwrap();

                pushed = true;
            }

            return Ok(pushed);
        } else {
            let queue = random_queue_name(&self.store);

            info!("direct store message, id={}, queue={}", identity_val, queue);

            self.store
                .open_tree(queue)
                .unwrap()
                .insert(&identity_vec, vec![])
                .unwrap();

            return Ok(true);
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
        let queue_names = QueueNames::from_application(application);

        let ready_queue = self.store.open_tree(queue_names.ready()).unwrap();

        info!("pop, queue_names.ready()={}", queue_names.ready());

        if let Ok(Some((k, _))) = ready_queue.pop_min() {
            // calculate item expire time
            let now_millis = Utc::now().timestamp_millis();
            let expire_time_millis = now_millis + invisibility_timeout as i64;

            let unack_queue = self.store.open_tree(queue_names.unack()).unwrap();

            // store {expire_time, message_id} to unack queue
            unack_queue
                .insert(
                    IVec::from(expire_time_millis.to_be_bytes().to_vec()),
                    k.clone(),
                )
                .unwrap();

            let message_data = self.store.open_tree(queue).unwrap();

            let key_bytes: Vec<u8> = k.to_vec();

            let message_data = message_data.get(k).unwrap().unwrap();
            let val_bytes: Vec<u8> = message_data.to_vec();

            let id = u64::from_be_bytes(key_bytes.try_into().unwrap());

            let value = Bytes::from(val_bytes);

            return Some(Message::new(id, value));
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
        let queue_names = QueueNames::from_application(application);

        let unack_queue = self.store.open_tree(queue_names.unack()).unwrap();

        let id_vec = IVec::from(id.to_be_bytes().to_vec());

        if let Ok(removed) = unack_queue.remove(id_vec.clone()) {
            if removed.is_some() {
                info!(
                    "commit: message removed, id={}, queue={}",
                    id,
                    queue_names.unack()
                );

                let data_queue = self.store.open_tree(queue).unwrap();

                data_queue.remove(id_vec).unwrap();

                info!("commit: message data removed, id={}, queue={}", id, queue);

                return true;
            }
        }

        false
    }

    pub async fn revert_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        let queue_names = QueueNames::from_application(application);

        let unack_queue = self.store.open_tree(queue_names.unack()).unwrap();

        let id_vec = IVec::from(id.to_be_bytes().to_vec());

        if let Ok(Some(removed_message)) = unack_queue.remove(id_vec.clone()) {
            info!(
                "revert: message removed, id={}, queue={}",
                id,
                queue_names.unack()
            );

            let ready_queue = self.store.open_tree(queue_names.ready()).unwrap();

            ready_queue.insert(id_vec, removed_message).unwrap();

            info!(
                "revert: message id inserted, id={}, queue={}",
                id,
                queue_names.ready()
            );

            return true;
        }

        false
    }

    pub async fn try_restore(&self, queue: &str, application: &str) -> bool {
        let queue_names = QueueNames::from_application(application);

        let unack_queue = self.store.open_tree(queue_names.unack()).unwrap();

        info!("try_restore, queue_names.unack()={}", queue_names.unack());

        if let Ok(Some((k, v))) = unack_queue.pop_min() {
            let now_millis = Utc::now().timestamp_millis();

            let expire_millis = i64::from_be_bytes(k.to_vec().try_into().unwrap());
            let message_id = u64::from_be_bytes(v.to_vec().try_into().unwrap());

            info!(
                "check message expiration, id={}, queue={}",
                message_id, queue
            );

            if now_millis >= expire_millis {
                info!(
                    "unack message expired, restoring, id={}, queue={}",
                    message_id, queue
                );

                if let Ok(Some(removed_message)) = unack_queue.remove(v.clone()) {
                    info!(
                        "restore: message removed, id={}, queue={}",
                        message_id,
                        queue_names.unack()
                    );

                    let ready_queue = self.store.open_tree(queue_names.ready()).unwrap();
                    info!("try_restore, queue_names.ready()={}", queue_names.ready());

                    ready_queue.insert(v, removed_message).unwrap();

                    info!(
                        "restore: message id inserted, id={}, queue={}",
                        message_id,
                        queue_names.ready()
                    );
                }

                return true;
            } else {
                unack_queue.insert(k, v).unwrap();
                return false;
            }
        }

        false
    }

    pub async fn get_unack_queues(&self) -> Vec<(String, String)> {
        self.store
            .tree_names()
            .into_iter()
            .filter(|n| n != b"__sled__default")
            .map(|q| String::from_utf8(q.to_vec()).unwrap())
            .filter(|q| QueueNames::is_unack(q))
            .map(|q| (QueueNames::get_unack_queue_name(&q).to_owned(), q))
            .collect()
    }

    pub async fn create_application_queue(&self, queue: &str, application: &str) {
        let queue_names = QueueNames::from_application(application);

        self.store.open_tree(queue_names.data()).unwrap();
        self.store.open_tree(queue_names.unack()).unwrap();
        self.store.open_tree(queue_names.ready()).unwrap();

        info!(
            "application queue created, queue={}, unack={}, ready={}, application={}",
            queue_names.data(),
            queue_names.unack(),
            queue_names.ready(),
            application
        );
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
