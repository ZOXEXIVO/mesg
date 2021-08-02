use crate::storage::{MessageStorage};
use std::collections::HashMap;
use std::sync::{Arc};
use parking_lot::{Condvar, Mutex};

pub struct StorageReader {
    pub queue_name: String,
    pub queue_storages: Arc<Mutex<HashMap<String, MessageStorage>>>,
    pub condvar: Arc<Condvar>
}