pub mod order_queue;

use order_queue::*;
use sled::Db;

pub struct InMemoryStructures {
    pub unack_order_data: UnackOrderData,
}

impl InMemoryStructures {
    pub fn from_db(db: &Db) -> Self {
        InMemoryStructures {
            unack_order_data: UnackOrderData::from_db(db),
        }
    }
}
