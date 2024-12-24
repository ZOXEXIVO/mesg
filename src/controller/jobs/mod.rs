use crate::controller::jobs::restorer::ExpiredMessageRestorerJob;
use crate::storage::{MesgStorage};
use std::sync::Arc;

mod restorer;

pub struct BackgroundJobs {
    storage: Arc<MesgStorage>,
    expited_message_restorer_job: ExpiredMessageRestorerJob,
}

impl BackgroundJobs {
    pub fn new(storage: Arc<MesgStorage>) -> Self {
        BackgroundJobs {
            storage,
            expited_message_restorer_job: ExpiredMessageRestorerJob::new(),
        }
    }

    pub fn start(&self) {
        self.expited_message_restorer_job
            .start(Arc::clone(&self.storage));
    }
}
