use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

static PUSH_METRIC: AtomicU64 = AtomicU64::new(0);
static CONSUMERS_COUNT_METRIC: AtomicU64 = AtomicU64::new(0);
static COMMIT_METRIC: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct StaticMetricsWriter;

impl StaticMetricsWriter {
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

    pub fn write(result: &mut String) {
        // mesg_push_ops
        writeln!(result, "# HELP mesg_push_ops Number of push operations").unwrap();
        writeln!(result, "# TYPE mesg_push_ops histogram").unwrap();
        writeln!(
            result,
            "mesg_push_ops {}",
            PUSH_METRIC.load(Ordering::SeqCst)
        )
        .unwrap();

        writeln!(result).unwrap();

        // mesg_commit_ops
        writeln!(result, "# HELP mesg_commit_ops Number of commit operations").unwrap();
        writeln!(result, "# TYPE mesg_commit_ops histogram").unwrap();
        writeln!(
            result,
            "mesg_commit_ops {}",
            COMMIT_METRIC.load(Ordering::SeqCst)
        )
        .unwrap();

        writeln!(result).unwrap();

        // mesg_consumers_count
        writeln!(
            result,
            "# HELP mesg_consumers_count Number of active consumers"
        )
        .unwrap();
        writeln!(result, "# TYPE mesg_consumers_count gauge").unwrap();
        writeln!(
            result,
            "mesg_consumers_count {}",
            CONSUMERS_COUNT_METRIC.load(Ordering::SeqCst)
        )
        .unwrap();
    }
}
