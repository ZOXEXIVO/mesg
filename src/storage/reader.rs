use crate::storage::{MessageStorage};
use std::sync::{Arc};
use parking_lot::{Condvar};
use chashmap::CHashMap;

pub struct StorageReader {
    pub queue_name: String,
    pub storage: Arc<CHashMap<String, MessageStorage>>,
    pub condvar: Arc<Condvar>
}