use crate::storage::message::Message;
use crate::storage::{IdPair, InnerStorage};
use bytes::Bytes;
use log::{error, info, warn};
use sled::Subscriber;
use std::path::Path;
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

    pub async fn try_restore(&self, queue: &str, application: &str) -> Option<u64> {
        // TODO Transaction

        // get expired id from unack_order
        let expired_unack_id = self.inner.get_expired_unack_id(queue, application);
        expired_unack_id?;

        let (expired_message_id, expired_at) = expired_unack_id.unwrap();

        if !self.inner.has_data(expired_message_id, queue) {
            return None;
        }

        // remove from uack queue
        if let Some(removed_id) = self
            .inner
            .remove_unack(expired_message_id, queue, application)
        {
            // add id to ready
            self.inner.store_ready(
                IdPair::new(expired_message_id, removed_id),
                queue,
                application,
            );

            Some(expired_message_id)
        } else {
            // revert unack_order
            self.inner
                .store_unack_order(expired_message_id, expired_at, queue, application);

            None
        }
    }

    pub fn get_unack_queues(&self) -> Vec<String> {
        self.inner.get_unack_queues()
    }
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Storage generic error")]
    Generic(String),
}
