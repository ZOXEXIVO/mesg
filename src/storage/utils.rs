use core::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

static CURRENT: AtomicU64 = AtomicU64::new(0);

pub struct StorageIdGenerator;

impl StorageIdGenerator {
    pub fn generate() -> u64 {
        CURRENT.fetch_add(1, Ordering::SeqCst)
    }
}