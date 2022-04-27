use chashmap::CHashMap;
use std::fmt::Write;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

extern crate lazy_static;
use lazy_static::lazy_static;

lazy_static! {
    static ref PUSH_METRIC: CHashMap<String, Arc<AtomicU64>> = CHashMap::new();
    static ref CONSUMERS_COUNT_METRIC: CHashMap<String, Arc<AtomicU64>> = CHashMap::new();
    static ref COMMIT_METRIC: CHashMap<String, Arc<AtomicU64>> = CHashMap::new();
}

#[derive(Clone)]
pub struct StaticMetricsWriter;

impl StaticMetricsWriter {
    pub fn inc_push_metric(queue: &str) {
        PUSH_METRIC.upsert(
            queue.into(),
            || Arc::new(AtomicU64::new(1)),
            |val| {
                val.fetch_add(1, Ordering::Relaxed);
            },
        );
    }
    pub fn inc_consumers_count_metric(queue: &str) {
        CONSUMERS_COUNT_METRIC.upsert(
            queue.into(),
            || Arc::new(AtomicU64::new(1)),
            |val| {
                val.fetch_add(1, Ordering::Relaxed);
            },
        );
    }
    pub fn decr_consumers_count_metric(queue: &str) {
        CONSUMERS_COUNT_METRIC.upsert(
            queue.into(),
            || Arc::new(AtomicU64::new(0)),
            |val| {
                val.fetch_sub(1, Ordering::Relaxed);
            },
        );
    }

    pub fn inc_commit_metric(queue: &str) {
        COMMIT_METRIC.upsert(
            queue.into(),
            || Arc::new(AtomicU64::new(1)),
            |val| {
                val.fetch_add(1, Ordering::Relaxed);
            },
        );
    }

    pub fn write(result: &mut String) {
        // mesg_push_ops
        writeln!(result, "# HELP mesg_push_ops Number of push operations").unwrap();
        writeln!(result, "# TYPE mesg_push_ops histogram").unwrap();
        Self::write_map(result, "mesg_push_ops", PUSH_METRIC.clone());

        writeln!(result).unwrap();

        // mesg_commit_ops
        writeln!(result, "# HELP mesg_commit_ops Number of commit operations").unwrap();
        writeln!(result, "# TYPE mesg_commit_ops histogram").unwrap();
        Self::write_map(result, "mesg_commit_ops", COMMIT_METRIC.clone());

        writeln!(result).unwrap();

        // mesg_consumers_count
        writeln!(
            result,
            "# HELP mesg_consumers_count Number of active consumers"
        )
        .unwrap();
        writeln!(result, "# TYPE mesg_consumers_count gauge").unwrap();
        Self::write_map(
            result,
            "mesg_consumers_count",
            CONSUMERS_COUNT_METRIC.clone(),
        );

        writeln!(result).unwrap();
    }

    fn write_map(result: &mut String, title: &str, map: CHashMap<String, Arc<AtomicU64>>) {
        for (queue, val) in map.into_iter() {
            let val = val.load(SeqCst);

            writeln!(result, "{} {{ queue=\"{}\" }} {}", title, queue, val).unwrap();
        }
    }
}
