use dashmap::DashMap;

pub struct ConsumerStatistics {
    statistics: DashMap<u32, u64>,
}

impl ConsumerStatistics {
    pub fn consumed(_id: u32) {
        //let now = Utc::now().timestamp_millis();
    }
}
