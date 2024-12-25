mod events_watcher;

use crate::consumer::jobs::events_watcher::EventsWatcher;
use crate::consumer::ConsumerDto;
use crate::storage::MesgStorage;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::Notify;

pub struct ConsumerJobsCollection {
    storage: Arc<MesgStorage>,
    config: ConsumerConfig,
    data_tx: Sender<ConsumerDto>,
    jobs: Vec<Box<dyn ConsumerBackgroundJob + Sync + Send>>,
}

impl ConsumerJobsCollection {
    pub fn new(
        storage: Arc<MesgStorage>,
        config: ConsumerConfig,
        data_tx: Sender<ConsumerDto>,
    ) -> Self {
        ConsumerJobsCollection {
            storage,
            config,
            data_tx,
            jobs: vec![Box::new(EventsWatcher::new())],
        }
    }

    pub fn add_job(&mut self, job: Box<dyn ConsumerBackgroundJob + Sync + Send>) {
        self.jobs.push(job);
    }

    pub fn start(&mut self) {
        for job in &mut self.jobs {
            job.start(
                Arc::clone(&self.storage),
                self.config.clone(),
                Arc::new(Notify::new()),
                self.data_tx.clone(),
            );
        }
    }

    pub fn shutdown(&mut self) {
        for handle in &mut self.jobs {
            handle.stop()
        }
    }
}

#[derive(Clone)]
pub struct ConsumerConfig {
    consumer_id: u32,
    queue: String,
    application: String,
    invisibility_timeout: i32,
}

impl ConsumerConfig {
    pub fn new(
        consumer_id: u32,
        queue: String,
        application: String,
        invisibility_timeout: i32,
    ) -> Self {
        ConsumerConfig {
            consumer_id,
            queue,
            application,
            invisibility_timeout,
        }
    }
}

pub trait ConsumerBackgroundJob {
    fn start(
        &mut self,
        storage: Arc<MesgStorage>,
        config: ConsumerConfig,
        notify: Arc<Notify>,
        data_tx: Sender<ConsumerDto>,
    );

    fn stop(&mut self);
}
