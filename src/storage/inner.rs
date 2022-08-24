use crate::storage::{IdPair, Identity, Message, QueueNames};
use bytes::Bytes;
use chrono::Utc;
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
            .insert(&id.vec, data.to_vec())
            .unwrap();
    }

    pub fn get_data(&self, id: &IdPair, queue: &str) -> Option<Message> {
        let message_data_queue = self.store.open_tree(queue).unwrap();

        if let Ok(Some(message_data)) = message_data_queue.get(&id.vec) {
            let val_bytes: Vec<u8> = message_data.to_vec();

            let value = Bytes::from(val_bytes);

            return Some(Message::new(id.value, value));
        }

        None
    }

    pub fn remove_data(&self, id: IdPair, queue: &str) -> bool {
        let message_data_queue = self.store.open_tree(queue).unwrap();

        if let Ok(Some(_)) = message_data_queue.remove(id.vec) {
            return true;
        }

        false
    }

    pub fn pop(&self, queue: &str, application: &str) -> Option<IdPair> {
        let queue_names = QueueNames::new(queue, application);

        let ready_queue = self.store.open_tree(queue_names.ready()).unwrap();

        if let Ok(Some((k, _))) = ready_queue.pop_min() {
            return Some(IdPair::from_vec(k));
        }

        None
    }

    pub fn store_unack(
        &self,
        id: &IdPair,
        queue: &str,
        application: &str,
        invisibility_timeout: u32,
    ) {
        let queue_names = QueueNames::new(queue, application);

        let now_millis = Utc::now().timestamp_millis();
        let expire_time_millis = now_millis + invisibility_timeout as i64;

        // store id to unack queue
        self.store
            .open_tree(queue_names.unack())
            .unwrap()
            .insert(&id.vec, vec![])
            .unwrap();

        // store { expire_time, message_id } to unack queue
        self.store
            .open_tree(queue_names.unack_order())
            .unwrap()
            .insert(
                IdPair::convert_i64_to_vec(expire_time_millis),
                IdPair::convert_u64_to_vec(id.value),
            )
            .unwrap();
    }

    pub fn remove_unack(&self, id: u64, queue: &str, application: &str) -> Option<IVec> {
        let queue_names = QueueNames::new(queue, application);

        let unack_queue = self.store.open_tree(queue_names.unack()).unwrap();

        let id_vec = IdPair::convert_u64_to_vec(id);

        if let Ok(Some(vec)) = unack_queue.remove(id_vec) {
            return Some(vec);
        }

        None
    }

    pub fn get_expired_unack_id(&self, queue: &str, application: &str) -> Option<(u64, i64)> {
        let queue_names = QueueNames::new(queue, application);

        let unack_order_queue = self.store.open_tree(queue_names.unack_order()).unwrap();

        if let Ok(Some((k, v))) = unack_order_queue.pop_min() {
            let now_millis = Utc::now().timestamp_millis();
            let expire_millis = i64::from_be_bytes(k.to_vec().try_into().unwrap());

            if now_millis >= expire_millis {
                let expired_id = u64::from_be_bytes(v.to_vec().try_into().unwrap());
                return Some((expired_id, expire_millis));
            } else {
                unack_order_queue.insert(k, v).unwrap();
            }
        }

        None
    }

    pub fn store_unack_order(&self, id: u64, expired_at: i64, queue: &str, application: &str) {
        let queue_names = QueueNames::new(queue, application);

        self.store
            .open_tree(queue_names.unack_order())
            .unwrap()
            .insert(
                IdPair::convert_i64_to_vec(expired_at),
                IdPair::convert_u64_to_vec(id),
            )
            .unwrap();
    }

    pub fn store_ready(&self, id: IdPair, queue: &str, application: &str) {
        let queue_names = QueueNames::new(queue, application);

        self.store
            .open_tree(queue_names.ready())
            .unwrap()
            .insert(id.vec, vec![])
            .unwrap();
    }

    // Storage
    pub fn broadcast_store(&self, queue: &str, id: IdPair) -> bool {
        let mut pushed = false;

        for queue in QueueUtils::get_ready_queues(&self.store, queue) {
            self.store
                .open_tree(queue)
                .unwrap()
                .insert(&id.vec, vec![])
                .unwrap();

            pushed = true;
        }

        pushed
    }

    pub fn direct_store(&self, queue: &str, id: IdPair) -> bool {
        let random_queue_name = QueueUtils::random_queue_name(&self.store, queue);

        self.store
            .open_tree(random_queue_name)
            .unwrap()
            .insert(&id.vec, vec![])
            .unwrap();

        true
    }

    pub fn subscribe_to_receiver(&self, queue: &str, application: &str) -> Subscriber {
        let queue_names = QueueNames::new(queue, application);

        self.store
            .open_tree(queue_names.ready())
            .unwrap()
            .watch_prefix(vec![])
    }

    pub fn get_unack_queues(&self) -> Vec<String> {
        QueueUtils::get_unack_queues(&self.store)
    }

    pub fn has_data(&self, id: u64, queue: &str) -> bool {
        let message_data_queue = self.store.open_tree(queue).unwrap();

        message_data_queue
            .contains_key(IdPair::convert_u64_to_vec(id))
            .unwrap()
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
            .filter(|db_queue| QueueNames::is_ready_for_queue(db_queue, queue))
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
