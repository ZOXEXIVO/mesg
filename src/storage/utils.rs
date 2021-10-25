use std::sync::atomic::{AtomicI64, Ordering};

static CURRENT: AtomicI64 = AtomicI64::new(0);

pub struct StorageIdGenerator;

impl StorageIdGenerator {
    pub fn generate() -> i64 {
        CURRENT.fetch_add(1, Ordering::SeqCst)
    }
}