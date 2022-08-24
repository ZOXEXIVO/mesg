use crate::controller::jobs::restorer::ExpiredMessageRestorerJob;
use crate::storage::Storage;
use std::sync::Arc;

mod restorer;

pub struct BackgroundJobs {
    storage: Arc<Storage>,
    expited_message_restorer_job: ExpiredMessageRestorerJob,
}

impl BackgroundJobs {
    pub fn new(storage: Arc<Storage>) -> Self {
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
