use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use tokio::sync::Mutex;

type BinaryMinHeap<T> = BinaryHeap<Reverse<T>>;

pub struct UnackOrderQueue {
    heap: Mutex<BinaryMinHeap<Reverse<OrderQueueItem>>>,
}

impl UnackOrderQueue {
    pub fn with_capacity(capacity: usize) -> Self {
        UnackOrderQueue {
            heap: Mutex::new(BinaryMinHeap::with_capacity(capacity)),
        }
    }

    pub async fn push(&self, expiration_ms: i64, message_id: u64) {
        let guard = self.heap.lock().await;
    }
}

pub struct OrderQueueItem {
    pub expiration_ms: i64,
    pub messages: Vec<u64>,
}

impl Eq for OrderQueueItem {}

impl PartialEq<Self> for OrderQueueItem {
    fn eq(&self, other: &Self) -> bool {
        self.expiration_ms == other.expiration_ms
    }
}

impl PartialOrd<Self> for OrderQueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.expiration_ms.cmp(&other.expiration_ms))
    }
}

impl Ord for OrderQueueItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.expiration_ms.cmp(&other.expiration_ms)
    }
}
