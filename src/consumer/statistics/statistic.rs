use chrono::Utc;
use dashmap::DashMap;

pub struct ConsumerStatistics {
    statistics: DashMap<u32, u64>,
}

impl ConsumerStatistics {
    pub fn consumed(id: u32) {
        let now = Utc::now().timestamp_millis();
    }
}
