use crate::storage::QueueNames;
use rand::{thread_rng, Rng};
use sled::Db;

// QueueNames helper
pub struct QueueUtils;

impl QueueUtils {
    fn get_user_queues(db: &Db) -> impl Iterator<Item = String> {
        db.tree_names()
            .into_iter()
            .filter(|n| n != b"__sled__default")
            .map(|q| String::from_utf8(q.to_vec()).unwrap())
    }

    pub fn get_ready_queues(db: &Db, base_queue_name: &str) -> Vec<String> {
        Self::get_user_queues(db)
            .filter(|db_queue| QueueNames::is_ready(db_queue, base_queue_name))
            .collect()
    }

    pub fn get_unack_queues(db: &Db) -> Vec<String> {
        Self::get_user_queues(db)
            .filter(|q| QueueNames::is_unack(q))
            .collect()
    }

    pub fn get_unack_order_queues(db: &Db) -> Vec<String> {
        Self::get_user_queues(db)
            .filter(|q| QueueNames::is_unack_order(q))
            .collect()
    }

    pub fn random_ready_queue_name(db: &Db, queue: &str) -> Option<String> {
        let items = Self::get_ready_queues(db, queue);

        if items.is_empty() {
            return None;
        }

        let mut rng = thread_rng();

        let n = rng.gen_range(0..items.len());

        Some(items[n].clone())
    }
}
