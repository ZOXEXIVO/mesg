use crate::storage::collections::InMemoryStructures;
use crate::storage::{IdPair, Identity, Message, QueueNames, QueueUtils};
use bytes::Bytes;
use chrono::Utc;
use log::{debug, error};
use sled::{Db, IVec, Subscriber, Tree};
use std::path::Path;
use std::sync::Arc;

pub struct InnerStorage {
    store: Arc<Db>,
    memory_data: InMemoryStructures,
}

impl InnerStorage {
    pub async fn new<T: AsRef<Path>>(_: T) -> Self {
        let storage = sled::open("data.mesg").unwrap();
        let memory_data = InMemoryStructures::from_db(&storage);

        InnerStorage {
            store: Arc::new(storage),
            memory_data,
        }
    }

    #[allow(dead_code)]
    pub fn from_db(db: &Db) -> Self {
        let storage = db.clone();

        InnerStorage {
            store: Arc::new(storage.clone()),
            memory_data: InMemoryStructures::from_db(&storage),
        }
    }

    pub async fn generate_id(&self, queue: &str) -> IdPair {
        Identity::generate(&self.store, queue).await
    }

    pub fn store_data(&self, id: &IdPair, queue: &str, data: Bytes) {
        // store data
        self.store
            .open_tree(queue)
            .unwrap()
            .insert(&id.vector(), data.to_vec())
            .unwrap();
    }

    pub fn get_data(&self, id: &IdPair, queue: &str) -> Option<Message> {
        let message_data_queue = self.store.open_tree(queue).unwrap();

        if let Ok(Some(message_data)) = message_data_queue.get(&id.vector()) {
            let value = Bytes::from(message_data.to_vec());

            return Some(Message::new(id.value(), value));
        }

        error!("data [id={}] not found, queue={}", id.value(), queue);

        None
    }

    pub fn store_data_usages(&self, queue: &str, id: &IdPair, usages_count: u32) {
        let data_usage_key = QueueNames::data_usage(queue);

        self.store
            .open_tree(data_usage_key)
            .unwrap()
            .insert(id.vector(), IdPair::from(usages_count))
            .unwrap();
    }

    pub fn decrement_data_usage(&self, queue: &str, id: &IdPair) -> Option<u32> {
        let data_queue_name = QueueNames::data_usage(queue);

        let mut current_value = 0;

        let _ = self
            .store
            .open_tree(data_queue_name)
            .unwrap()
            .fetch_and_update(id.vector(), |old| {
                let number = match old {
                    Some(bytes) => {
                        let array: [u8; 4] = bytes.try_into().unwrap();
                        let number = u32::from_be_bytes(array);
                        number - 1
                    }
                    None => 0,
                };

                current_value = number;

                Some(IVec::from(&number.to_be_bytes()))
            })
            .unwrap();

        Some(current_value)
    }

    pub fn remove_data(&self, queue: &str, id: &IdPair) -> bool {
        let message_data_queue = self.store.open_tree(queue).unwrap();

        // remove shared data
        if let Ok(None) = message_data_queue.remove(id.vector()) {
            debug!("remove data [id={}] none, queue={}", id.value(), queue);
            return false;
        }
        true
    }

    pub fn pop_ready_minimal(&self, queue: &str, application: &str) -> Option<IdPair> {
        let queue_names = QueueNames::new(queue, application);

        let ready_queue = self.store.open_tree(queue_names.ready()).unwrap();

        // pop minimal element
        if let Ok(Some((k, _))) = ready_queue.pop_min() {
            return Some(IdPair::from_vector(k));
        }

        None
    }

    pub async fn store_unack(
        &self,
        id: &IdPair,
        queue: &str,
        application: &str,
        invisibility_timeout_ms: i32,
    ) {
        let queue_names = QueueNames::new(queue, application);

        let now_millis = Utc::now().timestamp_millis();
        let expire_time_millis = now_millis + invisibility_timeout_ms as i64;

        // store id to unack queue
        self.store
            .open_tree(queue_names.unack())
            .unwrap()
            .insert(&id.vector(), vec![])
            .unwrap();

        // store expiration data to in_memory queue
        self.memory_data
            .unack_order_data
            .add(queue_names.unack_order(), id.value(), expire_time_millis)
            .await;

        // store { message_id, expire_time } to unack_order queue
        let unack_order = self.store.open_tree(queue_names.unack_order()).unwrap();

        let expire_vector = IdPair::from(expire_time_millis);

        unack_order
            .insert(&id.vector(), expire_vector.vector())
            .unwrap();

        debug!(
            "stored id={} to unack, queue={}, application={}",
            id.value(),
            queue,
            application
        );
    }

    pub fn remove_unack(&self, id: &IdPair, queue: &str, application: &str) -> bool {
        let queue_names = QueueNames::new(queue, application);

        let unack_queue = self.store.open_tree(queue_names.unack()).unwrap();

        matches!(unack_queue.remove(id.vector()), Ok(Some(_)))
    }

    pub async fn pop_expired_unacks(&self, queue: &str, application: &str) -> Option<Vec<IdPair>> {
        let now = Utc::now().timestamp_millis();

        let queue_names = QueueNames::new(queue, application);

        let unack_queue = self.store.open_tree(queue_names.unack()).unwrap();
        let unack_order_queue = self.store.open_tree(queue_names.unack_order()).unwrap();

        // store expiration data to in_memory queue
        let expired_data = self
            .memory_data
            .unack_order_data
            .get_expired(queue_names.unack_order(), now)
            .await;

        if let Some(expired_items) = expired_data {
            let mut result = Vec::new();

            for expired_id in &expired_items {
                if let Ok(Some(_)) = unack_order_queue.remove(IdPair::from(*expired_id).vector()) {
                    let id = IdPair::from_value(*expired_id);

                    if unack_queue.contains_key(id.vector()).unwrap() {
                        // if we succesfully drop item, use it value
                        result.push(id);
                    }
                }
            }

            Some(result)
        } else {
            None
        }
    }

    pub fn store_ready(&self, id: &IdPair, queue: &str, application: &str) {
        let queue_names = QueueNames::new(queue, application);

        self.store
            .open_tree(queue_names.ready())
            .unwrap()
            .insert(id.vector(), vec![])
            .unwrap();

        debug!(
            "stored [id={}] to ready queue, queue={}, application={}",
            id.value(),
            queue,
            application
        );
    }

    // Storage
    pub fn broadcast_store(&self, queue: &str, id: &IdPair) -> (bool, u32) {
        let mut pushed = false;

        let ready_queues = QueueUtils::get_ready_queues(&self.store, queue);

        for ready_queue in &ready_queues {
            self.store
                .open_tree(ready_queue)
                .unwrap()
                .insert(id.vector(), vec![])
                .unwrap();

            debug!(
                "broadcast stored [id={}] to queue, queue={}, application={}",
                id.value(),
                queue,
                &ready_queue
            );

            pushed = true;
        }

        (pushed, ready_queues.len() as u32)
    }

    pub fn direct_store(&self, queue: &str, id: &IdPair) -> bool {
        match QueueUtils::random_ready_queue_name(&self.store, queue) {
            Some(random_queue_name) => {
                self.store
                    .open_tree(&random_queue_name)
                    .unwrap()
                    .insert(&id.vector(), vec![])
                    .unwrap();

                debug!(
                    "direct stored [id={}] to queue, queue={}, application={}",
                    id.value(),
                    queue,
                    &random_queue_name
                );

                true
            }
            None => false,
        }
    }

    pub fn subscribe_to_receiver(&self, queue: &str, application: &str) -> Subscriber {
        let queue_names = QueueNames::new(queue, application);

        debug!("subscribed to queue, queue={}", queue_names.ready());

        self.store
            .open_tree(queue_names.ready())
            .unwrap()
            .watch_prefix(vec![])
    }

    pub fn get_unack_queues(&self) -> Vec<String> {
        QueueUtils::get_unack_queues(&self.store)
    }

    pub fn data_exists(&self, id: &IdPair, queue: &str) -> bool {
        let message_data_queue = self.store.open_tree(queue).unwrap();

        message_data_queue.contains_key(id.vector()).unwrap()
    }

    #[allow(dead_code)]
    pub fn unack_exists(&self, id: &IdPair, queue: &str, application: &str) -> bool {
        let queue_names = QueueNames::new(queue, application);

        let unack_queue = self.store.open_tree(queue_names.unack()).unwrap();

        unack_queue.contains_key(id.vector()).unwrap()
    }

    pub async fn create_application_queue(&self, queue: &str, application: &str) {
        let queue_names = QueueNames::new(queue, application);

        self.store.open_tree(queue_names.ready()).unwrap();
    }
}

impl Clone for InnerStorage {
    fn clone(&self) -> Self {
        InnerStorage {
            store: Arc::clone(&self.store),
            memory_data: InMemoryStructures::from_db(&Arc::clone(&self.store)),
        }
    }
}
