use rand::{Rng, thread_rng};
use sled::Db;
use crate::storage::QueueNames;

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

    pub fn random_queue_name(db: &Db, queue: &str) -> String {
        let items = Self::get_ready_queues(db, queue);
        
        let mut rng = thread_rng();

        let n = rng.gen_range(0..items.len());

        items[n].clone()
    }
}