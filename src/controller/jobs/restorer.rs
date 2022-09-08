use crate::storage::{DebugUtils, QueueNames, Storage};
use log::info;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub struct ExpiredMessageRestorerJob {
    watched_queues: Arc<RwLock<HashSet<String>>>,
}

impl ExpiredMessageRestorerJob {
    pub fn new() -> Self {
        ExpiredMessageRestorerJob {
            watched_queues: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub fn start(&self, storage: Arc<Storage>) {
        self.start_new_queues_watcher(storage);
    }

    fn start_new_queues_watcher(&self, storage: Arc<Storage>) {
        let watched_queues = Arc::clone(&self.watched_queues);

        tokio::spawn(async move {
            loop {
                let unack_queues = storage.get_unack_queues();

                let mut new_queues_to_watch = Vec::with_capacity(unack_queues.len());

                {
                    let watched_queue_read_guard = watched_queues.read().await;

                    for unack_queue in unack_queues {
                        if !watched_queue_read_guard.contains(&unack_queue) {
                            new_queues_to_watch.push(unack_queue);
                        }
                    }
                }

                if !new_queues_to_watch.is_empty() {
                    let mut watched_queue_write_guard = watched_queues.write().await;

                    for unack_queue in new_queues_to_watch {
                        watched_queue_write_guard.insert(unack_queue.clone());

                        let (queue_name, application_name) =
                            QueueNames::parse_queue_application(&unack_queue);

                        Self::start_message_restorer_for_queue(
                            Arc::clone(&storage),
                            queue_name.to_owned(),
                            application_name.to_owned(),
                        );
                    }
                }

                tokio::time::sleep(Duration::from_millis(5000)).await;
            }
        });
    }

    fn start_message_restorer_for_queue(storage: Arc<Storage>, queue: String, application: String) {
        tokio::spawn(async move {
            let mut attempt: u16 = 0;

            loop {
                if let Some(restored_unacks) = storage.try_restore(&queue, &application).await {
                    if !restored_unacks.is_empty() {
                        info!(
                            "message restored, ids=[{}] in queue={}, application={}",
                            DebugUtils::render_values(&restored_unacks),
                            queue,
                            application
                        );
                    }
                } else {
                    attempt += 1;

                    if attempt > 50 {
                        attempt = 0;
                    }

                    let sleep_time_ms = 100 * attempt;

                    tokio::time::sleep(Duration::from_millis(sleep_time_ms as u64)).await;
                }
            }
        });
    }
}
