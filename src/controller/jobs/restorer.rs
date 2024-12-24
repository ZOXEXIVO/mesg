use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::RwLock;
use crate::controller::jobs::BackgroundJob;
use crate::storage::MesgStorage;

pub struct ExpiredMessageRestorerJob {
    watched_queues: Arc<RwLock<HashSet<String>>>,
}

impl ExpiredMessageRestorerJob {
    pub fn new() -> Self {
        ExpiredMessageRestorerJob {
            watched_queues: Arc::new(RwLock::new(HashSet::new())),
        }
    }
}

impl BackgroundJob for ExpiredMessageRestorerJob {
    fn start(&self, storage: Arc<MesgStorage>) {
        
    }
}