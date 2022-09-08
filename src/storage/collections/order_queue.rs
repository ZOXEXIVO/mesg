use crate::storage::{IdPair, QueueNames, QueueUtils};
use dashmap::DashMap;
use log::info;
use sled::{Db, Tree};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
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
    min_heap: BinaryHeap<OrderQueueItem>,
}

impl UnackOrderQueueInner {
    pub fn with_capacity(capacity: usize) -> Self {
        UnackOrderQueueInner {
            min_heap: BinaryHeap::with_capacity(capacity),
        }
    }

    pub fn add(&mut self, message_id: u64, expire_at: i64) {
        // push to heap
        self.min_heap.push(OrderQueueItem {
            id: message_id,
            expired_at: Reverse(expire_at),
        });
    }

    pub fn get_expired(&mut self, now: i64) -> Option<Vec<u64>> {
        let min_element_expired = match self.min_heap.peek() {
            Some(min_val) => now >= min_val.expired_at.0,
            None => false,
        };

        if min_element_expired {
            let element = self.min_heap.pop().unwrap();
            return Some(vec![element.id]);
        }

        None
    }
}

pub struct OrderQueueItem {
    pub id: u64,
    pub expired_at: Reverse<i64>,
}

impl Eq for OrderQueueItem {}

impl PartialEq<Self> for OrderQueueItem {
    fn eq(&self, other: &Self) -> bool {
        self.expired_at.eq(&other.expired_at)
    }
}

impl PartialOrd<Self> for OrderQueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.expired_at.partial_cmp(&other.expired_at)
    }
}

impl Ord for OrderQueueItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.expired_at.cmp(&other.expired_at)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::collections::order_queue::UnackOrderQueueInner;

    #[test]
    fn get_expired_once_is_correct() {
        let mut queue = UnackOrderQueueInner::with_capacity(1);

        queue.add(5, 5);
        queue.add(4, 4);
        queue.add(3, 3);
        queue.add(2, 2);
        queue.add(1, 1);

        assert!(queue.get_expired(0).is_none());

        assert_eq!(1u64, queue.get_expired(10).unwrap()[0]);
        assert_eq!(2u64, queue.get_expired(10).unwrap()[0]);
        assert_eq!(3u64, queue.get_expired(10).unwrap()[0]);
        assert_eq!(4u64, queue.get_expired(10).unwrap()[0]);
        assert_eq!(5u64, queue.get_expired(10).unwrap()[0]);

        assert!(queue.get_expired(10).is_none());
    }
}
