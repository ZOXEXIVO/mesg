use crate::storage::message::Message;
use crate::storage::{DebugUtils, IdPair, InnerStorage, QueueNames};
use bytes::Bytes;
use log::{debug, error, info, warn};
use sled::transaction::{ConflictableTransactionError, TransactionalTree};
use sled::Subscriber;
use sled::Transactional;
use std::path::Path;
use structopt::clap::App;
use thiserror::Error;

#[derive(Debug, PartialEq)]
struct MyBullshitError;

pub struct Storage {
    inner: InnerStorage,
}

impl Storage {
    pub async fn new<T: AsRef<Path>>(path: T) -> Self {
        Storage {
            inner: InnerStorage::new(path).await,
        }
    }

    #[allow(dead_code)]
    pub fn from_inner(inner: InnerStorage) -> Self {
        Storage { inner }
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

        let result = if is_broadcast {
            // push id to all queue-receiver
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
        };

        // remove data if no consumers
        if let Ok(success) = result {
            if !success {
                self.inner.remove_data(queue, &id);
            }
        }

        result
    }

    pub async fn pop(
        &self,
        queue: &str,
        application: &str,
        invisibility_timeout_ms: i32,
    ) -> Option<Message> {
        match self.inner.pop_ready_minimal(queue, application) {
            Some(minimal_popped_id) => {
                // store id to unack queue
                self.inner
                    .store_unack(
                        &minimal_popped_id,
                        queue,
                        application,
                        invisibility_timeout_ms,
                    )
                    .await;
                // get message data from data queue
                self.inner.get_data(&minimal_popped_id, queue)
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

    async fn commit_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        let id_pair = IdPair::from_value(id);

        // Remove data from unack queue
        if !self.inner.remove_unack(&id_pair, queue, application) {
            debug!(
                "commit_inner remove_unack error, id={}, queue={}, application={}",
                id, queue, application
            );

            return false;
        }

        if let Some(data_usage) = self.inner.decrement_data_usage(queue, &id_pair) {
            if data_usage == 0 {
                if self.inner.remove_data(queue, &id_pair) {
                    debug!(
                        "commit_inner: data removed (usage=0), id={}, queue={}",
                        id, queue
                    );
                } else {
                    warn!(
                        "commit_inner: remove_data error, id={}, queue={}",
                        id, queue
                    );
                }
            }
        }

        true
    }

    pub async fn revert_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        let id_pair = IdPair::from_value(id);

        // Remove data from unack queue
        if !self.inner.remove_unack(&id_pair, queue, application) {
            warn!(
                "revert_inner remove_unack error: not found id in unack queue, id={}, queue={}, application={}",
                id, queue, application
            );

            return false;
        }

        self.inner.store_ready(&id_pair, queue, application);

        debug!(
            "revert_inner: ready item stored, id={}, queue={}, application={}",
            id, queue, application
        );

        true
    }

    pub async fn try_restore(&self, queue: &str, application: &str) -> Option<Vec<u64>> {
        if let Some(expired_unacks) = self.inner.pop_expired_unacks(queue, application).await {
            info!(
                "finded expired items, ids=[{}] in queue={}, application={}",
                DebugUtils::render_pair_values(&expired_unacks),
                queue,
                application
            );

            let mut resored_items = Vec::with_capacity(expired_unacks.len());

            // process expired items
            for expired_item in &expired_unacks {
                if process_expired_item(&self.inner, expired_item, queue, application) {
                    resored_items.push(expired_item.value());
                }
            }

            return Some(resored_items);
        }

        return None;

        fn process_expired_item(
            inner: &InnerStorage,
            expired_item: &IdPair,
            queue: &str,
            application: &str,
        ) -> bool {
            // check for data
            if !inner.data_exists(expired_item, queue) {
                debug!(
                    "try_restore: data not found, id={}, queue={}, application={}",
                    expired_item.value(),
                    queue,
                    application
                );

                return false;
            }

            // remove unack
            if !inner.remove_unack(expired_item, queue, application) {
                debug!(
                    "try_restore: remove_unack error, id={}, queue={}, application={}",
                    expired_item.value(),
                    queue,
                    application
                );

                return false;
            }

            debug!(
                "try_restore: remove_unack success, id={}, queue={}, application={}",
                expired_item.value(),
                queue,
                application
            );

            // add id to ready
            inner.store_ready(expired_item, queue, application);

            true
        }
    }

    pub fn get_unack_queues(&self) -> Vec<String> {
        self.inner.get_unack_queues()
    }

    pub async fn create_application_queue(&self, queue: &str, application: &str) {
        self.inner
            .create_application_queue(queue, application)
            .await;
    }
}

#[derive(Error, Debug)]
pub enum StorageError {}

#[cfg(test)]
mod tests {
    use crate::storage::{IdPair, InnerStorage, Storage};
    use bytes::Bytes;
    use sled::Config;

    #[tokio::test]
    async fn push_broadcast_no_consumer_queues_return_false() {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        let inner_storage = InnerStorage::from_db(&db);
        let storage = Storage::from_inner(inner_storage.clone());

        let data = Bytes::from(vec![1, 2, 3]);

        // act

        let result = storage.push("queue1", data, true).await.unwrap();

        // assert

        assert!(!result);
        assert!(!inner_storage.data_exists(&IdPair::from_value(0), "queue1"));
    }

    #[tokio::test]
    async fn push_broadcast_pop_each_once_is_correct() {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        let inner_storage = InnerStorage::from_db(&db);

        let queue = String::from("queue1");

        inner_storage.create_application_queue(&queue, "app1").await;
        inner_storage.create_application_queue(&queue, "app2").await;

        let storage = Storage::from_inner(inner_storage);

        let data = Bytes::from(vec![1, 2, 3]);

        //act

        let result = storage.push("queue1", data, true).await.unwrap();

        // assert

        assert!(result);

        let app1_data = storage.pop(&queue, "app1", 1000).await;
        let app2_data = storage.pop(&queue, "app2", 1000).await;

        assert!(app1_data.is_some());
        assert!(app2_data.is_some());

        let app1_data = storage.pop(&queue, "app1", 1000).await;
        let app2_data = storage.pop(&queue, "app2", 1000).await;

        assert!(app1_data.is_none());
        assert!(app2_data.is_none());
    }

    #[tokio::test]
    async fn push_direct_no_consumer_queues_return_false() {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        let inner_storage = InnerStorage::from_db(&db);
        let storage = Storage::from_inner(inner_storage);

        let data = Bytes::from(vec![1, 2, 3]);

        let result = storage.push("queue1", data, false).await.unwrap();

        //act

        let has_data = InnerStorage::from_db(&db).data_exists(&IdPair::from_value(0), "queue1");

        // assert

        assert!(!result);
        assert!(!has_data);
    }

    #[tokio::test]
    async fn push_direct_pop_one_once_is_correct() {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        let inner_storage = InnerStorage::from_db(&db);

        let queue = String::from("queue1");

        inner_storage.create_application_queue(&queue, "app1").await;
        inner_storage.create_application_queue(&queue, "app2").await;

        let storage = Storage::from_inner(inner_storage);

        let data = Bytes::from(vec![1, 2, 3]);

        // act

        let result = storage.push("queue1", data, false).await.unwrap();

        // assert

        assert!(result);

        let app1_data = storage.pop(&queue, "app1", 1000).await;
        let app2_data = storage.pop(&queue, "app2", 1000).await;

        assert!(app1_data.is_some() || app2_data.is_some());

        let app1_data = storage.pop(&queue, "app1", 1000).await;
        let app2_data = storage.pop(&queue, "app2", 1000).await;

        assert!(app1_data.is_none() && app2_data.is_none());
    }

    #[tokio::test]
    async fn pop_empty_return_none() {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        let inner_storage = InnerStorage::from_db(&db);

        let queue = String::from("queue1");

        inner_storage.create_application_queue(&queue, "app1").await;

        let storage = Storage::from_inner(inner_storage);

        // act

        let pop_result = storage.pop(&queue, "app1", 1000).await;

        // assert

        assert!(pop_result.is_none());
    }

    #[tokio::test]
    async fn pop_unack_exists() {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        let inner_storage = InnerStorage::from_db(&db);

        let queue = String::from("queue1");
        let application = String::from("app1");

        inner_storage
            .create_application_queue(&queue, &application)
            .await;

        let storage = Storage::from_inner(inner_storage.clone());

        let data = Bytes::from(vec![1, 2, 3]);

        storage.push(&queue, data, false).await.unwrap();

        // act

        let message = storage.pop(&queue, &application, 1000).await;

        // assert

        assert!(message.is_some());

        assert!(inner_storage.unack_exists(&IdPair::from_value(0), &queue, &application));
        assert!(inner_storage.data_exists(&IdPair::from_value(0), &queue));
    }

    #[tokio::test]
    async fn commit_success_unack_removed() {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        let inner_storage = InnerStorage::from_db(&db);

        let queue = String::from("queue1");
        let application = String::from("app1");

        inner_storage
            .create_application_queue(&queue, &application)
            .await;

        let storage = Storage::from_inner(inner_storage.clone());

        let data = Bytes::from(vec![1, 2, 3]);

        storage.push(&queue, data, false).await.unwrap();

        let message = storage.pop(&queue, &application, 1000).await.unwrap();

        // act

        let commit_result = storage.commit(message.id, &queue, &application, true).await;

        // assert

        assert!(commit_result);
        assert!(!inner_storage.unack_exists(&IdPair::from_value(message.id), &queue, &application));
    }

    #[tokio::test]
    async fn commit_success_one_data_copy_removed() {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        let inner_storage = InnerStorage::from_db(&db);

        let queue = String::from("queue1");
        let application = String::from("app1");

        inner_storage
            .create_application_queue(&queue, &application)
            .await;

        let storage = Storage::from_inner(inner_storage.clone());

        let data = Bytes::from(vec![1, 2, 3]);

        storage.push(&queue, data, false).await.unwrap();

        let message = storage.pop(&queue, &application, 1000).await.unwrap();

        // act

        let commit_result = storage.commit(message.id, &queue, &application, true).await;

        // assert

        assert!(commit_result);
        assert!(!inner_storage.data_exists(&IdPair::from_value(message.id), &queue));
    }

    #[tokio::test]
    async fn commit_success_two_data_usages_one_pop_not_removed() {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        let inner_storage = InnerStorage::from_db(&db);

        let queue = String::from("queue1");

        let application1 = String::from("app1");
        let application2 = String::from("app2");

        inner_storage
            .create_application_queue(&queue, &application1)
            .await;

        inner_storage
            .create_application_queue(&queue, &application2)
            .await;

        let storage = Storage::from_inner(inner_storage.clone());

        let data = Bytes::from(vec![1, 2, 3]);

        storage.push(&queue, data, true).await.unwrap();

        let message = storage.pop(&queue, &application1, 1000).await.unwrap();

        // act

        let commit_result = storage
            .commit(message.id, &queue, &application1, true)
            .await;

        // assert

        assert!(commit_result);
        assert!(inner_storage.data_exists(&IdPair::from_value(message.id), &queue));
    }

    #[tokio::test]
    async fn commit_success_two_data_usages_two_pop_removed() {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        let inner_storage = InnerStorage::from_db(&db);

        let queue = String::from("queue1");

        let application1 = String::from("app1");
        let application2 = String::from("app2");

        inner_storage
            .create_application_queue(&queue, &application1)
            .await;

        inner_storage
            .create_application_queue(&queue, &application2)
            .await;

        let storage = Storage::from_inner(inner_storage.clone());

        let data = Bytes::from(vec![1, 2, 3]);

        storage.push(&queue, data, true).await.unwrap();

        let message1 = storage.pop(&queue, &application1, 1000).await.unwrap();
        let message2 = storage.pop(&queue, &application2, 1000).await.unwrap();

        //act

        let commit1_result = storage
            .commit(message1.id, &queue, &application1, true)
            .await;

        assert!(commit1_result);

        let commit2_result = storage
            .commit(message2.id, &queue, &application2, true)
            .await;

        assert!(commit2_result);

        // assert

        assert!(!inner_storage.data_exists(&IdPair::from_value(message1.id), &queue));
    }

    // revert

    #[tokio::test]
    async fn commit_error_unack_removed_and_ready_added() {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        let inner_storage = InnerStorage::from_db(&db);

        let queue = String::from("queue1");
        let application = String::from("app1");

        inner_storage
            .create_application_queue(&queue, &application)
            .await;

        let storage = Storage::from_inner(inner_storage.clone());

        let data = Bytes::from(vec![1, 2, 3]);

        storage.push(&queue, data, false).await.unwrap();

        let message1 = storage.pop(&queue, &application, 1000).await.unwrap();
        let message2 = storage.pop(&queue, &application, 1000).await;

        // act

        let commit_result = storage
            .commit(message1.id, &queue, &application, false)
            .await;

        // assert

        assert!(commit_result);
        assert!(message2.is_none());

        assert!(!inner_storage.unack_exists(
            &IdPair::from_value(message1.id),
            &queue,
            &application
        ));

        // check for ready exists

        let message3 = storage.pop(&queue, &application, 1000).await;
        assert!(message3.is_some());
    }
}
