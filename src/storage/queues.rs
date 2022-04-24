use crate::storage::QueueNames;
use chashmap::CHashMap;
use log::info;
use sled::{Db, Tree};

pub struct QueueCollection {
    queues: CHashMap<String, Tree>,
}

impl QueueCollection {
    pub fn new() -> Self {
        QueueCollection {
            queues: CHashMap::new(),
        }
    }

    pub fn tree_names(&self) -> Vec<String> {
        self.queues
            .clone()
            .into_iter()
            .map(|(k, _)| k)
            .filter(|q| !QueueNames::is_unacked(q))
            .collect()
    }

    pub fn execute<F: Fn(&Tree) -> R, R>(&self, db: &Db, queue: &str, action: F) -> R {
        let mut insert_result = None;
        let mut update_result = None;

        self.queues.upsert(
            queue.to_owned(),
            || {
                let opened_tree = db.open_tree(queue).unwrap();

                insert_result = Some(action(&opened_tree));

                opened_tree
            },
            |tree| {
                update_result = Some(action(tree));
            },
        );

        if let Some(..) = insert_result {
            return insert_result.unwrap();
        }

        update_result.unwrap()
    }
}
