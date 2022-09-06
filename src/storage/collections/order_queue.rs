use crate::storage::{IdPair, QueueNames, QueueUtils};
use dashmap::DashMap;
use log::info;
use sled::{Db, Tree};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use tokio::sync::Mutex;

pub struct UnackOrderData {
    data: DashMap<String, UnackOrderQueue>,
}

impl UnackOrderData {
    pub fn from_db(db: &Db) -> Self {
        let result = DashMap::new();

        for unack_order_queue in QueueUtils::get_unack_order_queues(db) {
            let (queue, application) = QueueNames::parse_queue_application(&unack_order_queue);

            let unack_order_tree = db.open_tree(&unack_order_queue).unwrap();

            let queue_name = QueueNames::new(queue, application).base();

            result.insert(
                queue_name,
                UnackOrderQueue::from_data(get_date(&unack_order_tree)),
            );
        }

        return UnackOrderData { data: result };

        // helpers
        fn get_date(tree: &Tree) -> Vec<(u64, i64)> {
            let mut queue_data: Vec<(u64, i64)> = Vec::with_capacity(tree.len());

            for tree_item in tree.into_iter() {
                let (k, v) = tree_item.unwrap();

                let id = IdPair::from_vector(k);

                queue_data.push((id.value(), IdPair::convert_vec_to_i64(v)));
            }

            queue_data
        }
    }

    pub async fn add(&self, queue_name: String, message_id: u64, expire_at: i64) {
        self.data
            .entry(queue_name)
            .or_insert(UnackOrderQueue::new())
            .add(message_id, expire_at)
            .await;
    }

    pub async fn get_expired(&self, queue_name: String, now: i64) -> Option<Vec<u64>> {
        if let Some(data) = self.data.get(&queue_name) {
            return data.get_expired(now).await;
        }

        None
    }
}

type MinHeap<T> = BinaryHeap<Reverse<T>>;

pub struct UnackOrderQueue {
    data: Mutex<UnackOrderQueueInner>,
}

impl UnackOrderQueue {
    pub fn new() -> Self {
        UnackOrderQueue {
            data: Mutex::new(UnackOrderQueueInner::with_capacity(1024)),
        }
    }

    pub fn from_data(data: Vec<(u64, i64)>) -> Self {
        let data_len = data.len();

        let mut order_queue = UnackOrderQueueInner::with_capacity(data_len);

        for (id, expire_at) in data {
            order_queue.add(id, expire_at);
        }

        info!("order_queue init with {} records", data_len);

        UnackOrderQueue {
            data: Mutex::new(order_queue),
        }
    }

    pub async fn add(&self, message_id: u64, expire_at: i64) {
        let mut data_guard = self.data.lock().await;

        data_guard.add(message_id, expire_at);
    }

    pub async fn get_expired(&self, now: i64) -> Option<Vec<u64>> {
        let mut data_guard = self.data.lock().await;

        data_guard.get_expired(now)
    }
}

pub struct UnackOrderQueueInner {
    min_heap: MinHeap<i64>,
    expiration_group: HashMap<i64, Vec<u64>>,
}

impl UnackOrderQueueInner {
    pub fn with_capacity(capacity: usize) -> Self {
        UnackOrderQueueInner {
            min_heap: MinHeap::with_capacity(capacity),
            expiration_group: HashMap::new(),
        }
    }

    pub fn add(&mut self, message_id: u64, expire_at: i64) {
        // push to heap
        self.min_heap.push(Reverse(expire_at));

        // push to hashmap
        self.expiration_group
            .entry(expire_at)
            .or_insert(Vec::new())
            .push(message_id);
    }

    pub fn get_expired(&mut self, now: i64) -> Option<Vec<u64>> {
        let mut min_element = -1;

        let min_element_expired = match self.min_heap.peek() {
            Some(Reverse(min_val)) => {
                min_element = *min_val;
                now >= *min_val
            }
            None => false,
        };

        if min_element_expired {
            let _ = self.min_heap.pop();

            return self.expiration_group.remove(&min_element);
        }

        None
    }
}
