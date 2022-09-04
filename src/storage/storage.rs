use crate::storage::message::Message;
use crate::storage::{IdPair, InnerStorage};
use bytes::Bytes;
use log::{debug, error, info, warn};
use sled::Subscriber;
use std::path::Path;
use thiserror::Error;

pub struct Storage {
    inner: InnerStorage,
}

impl Storage {
    pub async fn new<T: AsRef<Path>>(path: T) -> Self {
        Storage {
            inner: InnerStorage::new(path).await,
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

        if is_broadcast {
            // push id to all queue-reciever
            let (success, affected_consumers) = self.inner.broadcast_store(queue, &id);

            if success {
                self.inner.store_data_usages(queue, &id, affected_consumers);
            }

            Ok(success)
        } else {
            // push id to random receiver queue
            let success = self.inner.direct_store(queue, &id);

            if success {
                self.inner.store_data_usages(queue, &id, 1);
            }

            Ok(success)
        }
    }

    pub async fn pop(
        &self,
        queue: &str,
        application: &str,
        invisibility_timeout_ms: i32,
    ) -> Option<Message> {
        match self.inner.pop(queue, application) {
            Some(popped_id) => {
                // store id to unack queue
                self.inner
                    .store_unack(&popped_id, queue, application, invisibility_timeout_ms)
                    .await;
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
        let id_pair = IdPair::from_value(id);

        debug!(
            "commit_inner, id={}, queue={}, application={}",
            id, queue, application
        );

        // Remove data from unack queue
        if !self.inner.remove_unack(&id_pair, queue, application) {
            debug!(
                "commit_inner remove_unack error, id={}, queue={}, application={}",
                id, queue, application
            );

            return false;
        }

        debug!(
            "commit_inner remove_unack success, id={}, queue={}, application={}",
            id, queue, application
        );

        if let Some(data_usage) = self.inner.decrement_data_usage(queue, &id_pair) {
            if data_usage == 0 {
                if !self.inner.remove_data(queue, &id_pair) {
                    warn!(
                        "commit_inner: remove_data error, id={}, queue={}",
                        id, queue
                    );

                    return false;
                }

                debug!(
                    "commit_inner remove_data success, id={}, queue={}, application={}",
                    id, queue, application
                );
            }
        }

        true
    }

    pub async fn revert_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        let id_pair = IdPair::from_value(id);

        // Remove data from unack queue
        if !self.inner.remove_unack(&id_pair, queue, application) {
            warn!(
                "revert_inner remove_unack error: not found id in unack queue, id={}, queue={}",
                id, queue
            );

            return false;
        }

        debug!(
            "revert_inner remove_unack success, id={}, queue={}, application={}",
            id, queue, application
        );

        self.inner.store_ready(&id_pair, queue, application);

        true
    }

    pub async fn try_restore(&self, queue: &str, application: &str) -> Option<Vec<u64>> {
        // get expired id from unack_order
        if let Some(expired_unacks) = self.inner.pop_expired_unacks(queue, application).await {
            for expired_item in &expired_unacks {
                // check for data
                if !self.inner.has_data(&expired_item, queue) {
                    debug!(
                        "try_restore: data not found, queue={}, application={}",
                        queue, application
                    );
                }

                // remove unack
                if self.inner.remove_unack(&expired_item, queue, application) {
                    debug!(
                        "try_restore: remove_unack success, id={}, queue={}, application={}",
                        expired_item.value(),
                        queue,
                        application
                    );

                    // add id to ready
                    self.inner.store_ready(&expired_item, queue, application);
                } else {
                    debug!(
                        "try_restore: remove_unack error, id={}, queue={}, application={}",
                        expired_item.value(),
                        queue,
                        application
                    );
                }
            }

            let result: Vec<u64> = expired_unacks.into_iter().map(|e| e.value()).collect();
            return Some(result);
        }

        return None;
    }

    pub fn get_unack_queues(&self) -> Vec<String> {
        self.inner.get_unack_queues()
    }
}

#[derive(Error, Debug)]
pub enum StorageError {}
