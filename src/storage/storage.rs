use crate::storage::message::Message;
use crate::storage::{IdPair, InnerStorage, QueueNames};
use bytes::Bytes;
use chrono::Utc;
use log::{error, info, warn};
use sled::{IVec, Subscriber};
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

pub struct Storage {
    inner: InnerStorage,
}

impl Storage {
    pub fn new<T: AsRef<Path>>(path: T) -> Self {
        Storage {
            inner: InnerStorage::new(path),
        }
    }

    pub async fn push(
        &self,
        queue: &str,
        data: Bytes,
        is_broadcast: bool,
    ) -> Result<bool, StorageError> {
        // generate message id
        let id = self.inner.generate_id(queue).await;

        self.inner.store_data(&id, queue, data);

        info!("store message data, id={}, queue={}", id.value, queue);

        if is_broadcast {
            // push id to all queue-reciever
            Ok(self.inner.broadcast_store(queue, id))
        } else {
            // push id to random receiver queue
            Ok(self.inner.direct_store(queue, id))
        }
    }

    pub async fn pop(
        &self,
        queue: &str,
        application: &str,
        invisibility_timeout: u32,
    ) -> Option<Message> {
        match self.inner.pop(queue, application) {
            Some(popped_id) => {
                // store id to unack queue
                self.inner
                    .store_unack(&popped_id, queue, application, invisibility_timeout);
                // get message data from data queue
                self.inner.get_data(&popped_id, queue)
            }
            None => None,
        }
    }

    pub async fn subscribe(&self, queue: &str, application: &str) -> Subscriber {
        let subscriber = self.inner.subscribe_to_receiver(queue, application);

        info!(
            "subscribed for new messages in queue={}, application={}",
            queue, application
        );

        subscriber
    }

    pub async fn commit(&self, id: u64, queue: &str, application: &str, success: bool) -> bool {
        match success {
            true => self.commit_inner(id, queue, application).await,
            false => self.revert_inner(id, queue, application).await,
        }
    }

    pub async fn commit_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        // Remove data from unack queue
        let removed_id = self.inner.remove_unack(id, queue, application);
        if removed_id.is_none() {
            warn!(
                "commit: not found id in unack queue, id={}, queue={}",
                id, queue
            );
            return false;
        }

        // Remove data
        if !self
            .inner
            .remove_data(IdPair::new(id, removed_id.unwrap()), queue)
        {
            warn!(
                "commit: data not found in data_queue, id={}, queue={}",
                id, queue
            );

            return false;
        }

        true
    }

    pub async fn revert_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        // Remove data from unack queue
        let removed_id = self.inner.remove_unack(id, queue, application);
        if removed_id.is_none() {
            warn!(
                "revert: not found id in unack queue, id={}, queue={}",
                id, queue
            );
            return false;
        }

        self.inner
            .store_ready(IdPair::new(id, removed_id.unwrap()), queue, application);

        true
    }

    pub async fn try_restore(&self, queue: &str, application: &str) -> bool {
        false
        // let queue_names = QueueNames::new(queue, application);
        //
        // let unack_queue = self.inner.open_tree(queue_names.unack()).unwrap();
        //
        // if let Ok(Some((k, v))) = unack_queue.pop_min() {
        //     let now_millis = Utc::now().timestamp_millis();
        //
        //     let expire_millis = i64::from_be_bytes(k.to_vec().try_into().unwrap());
        //     let message_id = u64::from_be_bytes(v.to_vec().try_into().unwrap());
        //
        //     info!(
        //         "check message expiration, id={}, queue={}",
        //         message_id, queue
        //     );
        //
        //     if now_millis >= expire_millis {
        //         info!(
        //             "unack message expired, restoring, id={}, queue={}",
        //             message_id, queue
        //         );
        //
        //         if let Ok(Some(removed_message)) = unack_queue.remove(v.clone()) {
        //             info!(
        //                 "restore: message removed, id={}, queue={}",
        //                 message_id,
        //                 queue_names.unack()
        //             );
        //
        //             let ready_queue = self.inner.open_tree(queue_names.ready()).unwrap();
        //
        //             ready_queue.insert(v, removed_message).unwrap();
        //         }
        //
        //         return true;
        //     } else {
        //         unack_queue.insert(k, v).unwrap();
        //         return false;
        //     }
        // }
        //
        // false
    }
}
//
// impl Clone for Storage {
//     fn clone(&self) -> Self {
//         Storage {
//             inner: Arc::clone(&self.inner),
//         }
//     }
// }

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Storage generic error")]
    Generic(String),
}
