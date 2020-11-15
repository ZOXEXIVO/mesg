use core::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

pub struct StorageIdGenerator {
    current: AtomicU64
}

impl StorageIdGenerator {
    pub fn new() -> Self {
        StorageIdGenerator {
            current: AtomicU64::new(0)
        }    
    }
    
    pub fn generate(&self) -> u64 {
        self.current.fetch_add(1, Ordering::SeqCst)
    }
}