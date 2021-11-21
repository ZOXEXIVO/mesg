use std::sync::atomic::{AtomicI64, Ordering};

static CURRENT_SEQUENCE_VALUE: AtomicI64 = AtomicI64::new(0);

pub struct SequenceGenerator;

impl SequenceGenerator {
    pub fn generate() -> i64 {
        CURRENT_SEQUENCE_VALUE.fetch_add(1, Ordering::SeqCst)
    }
}
