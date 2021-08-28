use std::sync::atomic::{AtomicU64, Ordering};
use std::fmt::Write;


static PUSH_METRIC: AtomicU64 = AtomicU64::new(0);
static CONSUMERS_COUNT_METRIC: AtomicU64 = AtomicU64::new(0);
static COMMIT_METRIC: AtomicU64 = AtomicU64::new(0);
static QUEUES_COUNT_METRIC: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct MetricsWriter {
}

impl MetricsWriter {
    pub fn inc_push_metric() {
        PUSH_METRIC.fetch_add(1, Ordering::SeqCst);
    }
    pub fn inc_consumers_count_metric() {
        CONSUMERS_COUNT_METRIC.fetch_add(1, Ordering::SeqCst);
    }
    pub fn decr_consumers_count_metric() {
        CONSUMERS_COUNT_METRIC.fetch_sub(1, Ordering::SeqCst);
    }
    
    pub fn inc_commit_metric() {
        COMMIT_METRIC.fetch_add(1, Ordering::SeqCst);
    }
    pub fn inc_queues_count_metric() {
        QUEUES_COUNT_METRIC.fetch_add(1, Ordering::SeqCst);
    }
    
    pub fn write(result: &mut String){
        result.reserve(100);
       
        result.write_fmt(format_args!("mesg_push_ops {}\r\n", PUSH_METRIC.load(Ordering::SeqCst))).unwrap();
        result.write_fmt(format_args!("mesg_consumers_count {}\r\n", CONSUMERS_COUNT_METRIC.load(Ordering::SeqCst))).unwrap();
        result.write_fmt(format_args!("mesg_commit_ops {}\r\n", COMMIT_METRIC.load(Ordering::SeqCst))).unwrap();
        result.write_fmt(format_args!("mesg_queues_count {}\r\n", QUEUES_COUNT_METRIC.load(Ordering::SeqCst))).unwrap();        
    }
}
