use sled::{Db, Tree};
use std::collections::HashMap;
use tokio::sync::RwLock;

pub struct QueueCollection {
    trees: RwLock<HashMap<String, Tree>>,
}

impl QueueCollection {
    pub fn new() -> Self {
        QueueCollection {
            trees: RwLock::new(HashMap::new()),
        }
    }

    pub fn tree_names(&self) -> Vec<String> {
        let read_lock = self.trees.blocking_read();
        read_lock.keys().map(|k| k.clone()).collect()
    }

    pub fn execute_in_queue<F: Fn(&Tree) -> R, R>(&self, db: &Db, queue: &str, action: F) -> R {
        let read_lock = self.trees.blocking_read();

        if let Some(tree) = read_lock.get(queue) {
            return action(tree);
        } else {
            drop(read_lock);

            let mut write_lock = self.trees.blocking_write();

            let tree = db.open_tree(queue).unwrap();

            let result = action(&tree);

            write_lock.insert(String::from(queue), tree);

            result
        }
    }
}
