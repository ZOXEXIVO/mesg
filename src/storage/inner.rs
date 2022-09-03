use crate::storage::{DebugUtils, IdPair, Identity, Message, QueueNames};
use bytes::Bytes;
use chrono::Utc;
use log::{debug, warn};
use rand::{thread_rng, Rng};
use sled::{Db, IVec, Subscriber};
use std::path::Path;
use std::sync::Arc;

pub struct InnerStorage {
    store: Arc<Db>,
}

impl InnerStorage {
    pub fn new<T: AsRef<Path>>(_: T) -> Self {
        InnerStorage {
            store: Arc::new(sled::open("data.mesg").unwrap()),
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

        debug!("data stored, message_id={}, queue={}", id.value(), queue);
    }

    pub fn get_data(&self, id: &IdPair, queue: &str) -> Option<Message> {
        let message_data_queue = self.store.open_tree(queue).unwrap();

        if let Ok(Some(message_data)) = message_data_queue.get(&id.vector()) {
            let val_bytes: Vec<u8> = message_data.to_vec();

            let value = Bytes::from(val_bytes);

            return Some(Message::new(id.value(), value));
        }

        debug!("get_data none, message_id={}, queue={}", id.value(), queue);

        None
    }

    pub fn store_data_usages(&self, queue: &str, id: &IdPair, usages_count: u32) {
        let data_usage_key = QueueNames::data_usage(queue);

        self.store
            .open_tree(data_usage_key)
            .unwrap()
            .insert(id.vector(), IdPair::convert_u32_to_vec(usages_count))
            .unwrap();
    }

    // TODO
    pub fn decrement_data_usage(&self, queue: &str, id: &IdPair) -> Option<u64> {
        let data_usage_key = QueueNames::data_usage(queue);

        let mut current_value = 0;

        let _ = self
            .store
            .fetch_and_update(data_usage_key, |old| {
                let number = match old {
                    Some(bytes) => {
                        let array: [u8; 8] = bytes.try_into().unwrap();
                        let number = u64::from_be_bytes(array);
                        number - 1
                    }
                    None => 0,
                };

                current_value = number - 1;

                Some(IVec::from(&number.to_be_bytes()))
            })
            .unwrap();

        Some(current_value)
    }

    pub fn remove_data(&self, queue: &str, id: &IdPair) -> bool {
        let message_data_queue = self.store.open_tree(queue).unwrap();

        if let Ok(Some(_)) = message_data_queue.remove(id.vector()) {
            debug!(
                "remove_data success, message_id={}, queue={}",
                id.value(),
                queue
            );

            return true;
        }

        debug!(
            "remove_data none, message_id={}, queue={}",
            id.value(),
            queue
        );

        false
    }

    pub fn pop(&self, queue: &str, application: &str) -> Option<IdPair> {
        let queue_names = QueueNames::new(queue, application);

        let ready_queue = self.store.open_tree(queue_names.ready()).unwrap();

        if let Ok(Some((k, _))) = ready_queue.pop_min() {
            let vector = IdPair::from_vector(k);

            debug!(
                "pop_min success, message_id={}, queue={}, application={}",
                vector.value(),
                queue,
                application
            );

            return Some(vector);
        }

        debug!("pop_min none, queue={}, application={}", queue, application);

        None
    }

    pub fn store_unack(
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

        // store { expire_time, message_id } to unack_order queue
        let unack_order = self.store.open_tree(queue_names.unack_order()).unwrap();

        let expire_vector = IdPair::convert_i64_to_vec(expire_time_millis);
        let id_vector = id.vector();

        let mut result_vector: Vec<u8> = Vec::with_capacity(expire_vector.len() + id_vector.len());

        result_vector.append(&mut expire_vector.to_vec());
        result_vector.append(&mut id_vector.to_vec());

        unack_order
            .insert(IVec::from(result_vector), id.vector())
            .unwrap();

        debug!(
            "message stored to unack, unack_order queue, message_id={}, queue={}, application={}",
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

    pub fn pop_expired_unack(&self, queue: &str, application: &str) -> Option<(IdPair, i64)> {
        let queue_names = QueueNames::new(queue, application);

        let unack_order_queue = self.store.open_tree(queue_names.unack_order()).unwrap();
        let unack_queue = self.store.open_tree(queue_names.unack()).unwrap();
        let ready_queue = self.store.open_tree(queue_names.ready()).unwrap();

        #[cfg(debug_assertions)]
        DebugUtils::print_keys_tree(&ready_queue, &format!("ready: {} {}", queue, application));
        #[cfg(debug_assertions)]
        DebugUtils::print_keys_tree(&unack_queue, &format!("unack: {} {}", queue, application));
        #[cfg(debug_assertions)]
        DebugUtils::print_values_tree(
            &unack_order_queue,
            &format!("unack_order: {} {}", queue, application),
        );

        let now = IdPair::convert_i64_to_vec(Utc::now().timestamp_millis());

        // try get
        if let Ok(Some((k, v))) = unack_order_queue.get_gt(now) {
            let vector = k.to_vec().as_slice()[0..8].to_vec();

            let expire_millis = i64::from_be_bytes(vector.try_into().unwrap());

            if unack_order_queue.remove(&k).is_err() {
                let id = IdPair::from_vector(k);

                warn!(
                    "unack remove error: not found id={}, queue={}, application={}",
                    id.value(),
                    queue,
                    application
                )
            }

            return Some((IdPair::from_vector(v), expire_millis));
        }

        None
    }

    pub fn store_ready(&self, id: &IdPair, queue: &str, application: &str) {
        let queue_names = QueueNames::new(queue, application);

        self.store
            .open_tree(queue_names.ready())
            .unwrap()
            .insert(id.vector(), vec![])
            .unwrap();

        debug!(
            "message stored to ready queue, message_id={}, queue={}, application={}",
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
                .open_tree(&ready_queue)
                .unwrap()
                .insert(id.vector(), vec![])
                .unwrap();

            debug!(
                "broadcast message stored to queue, message_id={}, queue={}, application={}",
                id.value(),
                queue,
                &ready_queue
            );

            pushed = true;
        }

        (pushed, ready_queues.len() as u32)
    }

    pub fn direct_store(&self, queue: &str, id: &IdPair) -> bool {
        let random_queue_name = QueueUtils::random_queue_name(&self.store, queue);

        self.store
            .open_tree(&random_queue_name)
            .unwrap()
            .insert(&id.vector(), vec![])
            .unwrap();

        debug!(
            "direct message stored to queue, message_id={}, queue={}, application={}",
            id.value(),
            queue,
            &random_queue_name
        );

        true
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

    pub fn has_data(&self, id: &IdPair, queue: &str) -> bool {
        let message_data_queue = self.store.open_tree(queue).unwrap();

        message_data_queue.contains_key(id.vector()).unwrap()
    }
}

// QueueNames helper
pub struct QueueUtils;

impl QueueUtils {
    pub fn get_ready_queues(db: &Db, queue: &str) -> Vec<String> {
        db.tree_names()
            .into_iter()
            .filter(|n| n != b"__sled__default")
            .map(|q| String::from_utf8(q.to_vec()).unwrap())
            .filter(|db_queue| QueueNames::is_ready(db_queue, queue))
            .collect()
    }

    pub fn get_unack_queues(db: &Db) -> Vec<String> {
        db.tree_names()
            .into_iter()
            .filter(|n| n != b"__sled__default")
            .map(|q| String::from_utf8(q.to_vec()).unwrap())
            .filter(|q| QueueNames::is_unack(q))
            .collect()
    }

    pub fn random_queue_name(db: &Db, queue: &str) -> String {
        let items = Self::get_ready_queues(db, queue);

        let mut rng = thread_rng();

        let n = rng.gen_range(0..items.len());

        items[n].clone()
    }
}
