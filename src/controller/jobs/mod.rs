use crate::storage::{MesgStorage};
use std::sync::Arc;

mod restorer;

pub struct BackgroundJobs {
    storage: Arc<MesgStorage>,
    jobs: Vec<Box<dyn BackgroundJob + Sync + Send + 'static>>,
}

impl BackgroundJobs {
    pub fn new(storage: Arc<MesgStorage>) -> Self {
        BackgroundJobs {
            storage,
            jobs: Vec::new(),
        }
    }

    pub fn add_job(&mut self, job: Box<dyn BackgroundJob + Sync + Send + 'static>) {
        self.jobs.push(job);
    }


    pub fn start(&self) {
        for job in &self.jobs {
            job.start(Arc::clone(&self.storage));
        }
    }
}


pub trait BackgroundJob {
    fn start(&self, storage: Arc<MesgStorage>);
}