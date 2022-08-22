use crate::storage::Storage;
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
                let current_queues = storage.get_unack_queues().await;

                let mut queues_to_add = Vec::new();

                {
                    let watched_queue_read_guard = watched_queues.read().await;

                    for (current_queue, current_application) in current_queues {
                        let full_queue_name = format!("{}{}", current_queue, current_application);

                        if !watched_queue_read_guard.contains(&full_queue_name) {
                            queues_to_add.push((
                                full_queue_name.to_owned(),
                                (current_queue, current_application),
                            ));
                        }
                    }
                }

                if !queues_to_add.is_empty() {
                    let mut watched_queue_write_guard = watched_queues.write().await;

                    for (full_queue_path, (queue, application)) in queues_to_add {
                        info!(
                            "add new queue to watch, queue={}, application={}",
                            queue, application
                        );

                        watched_queue_write_guard.insert(full_queue_path);

                        Self::start_message_restorer_for_queue(
                            Arc::clone(&storage),
                            queue.clone(),
                            application.clone(),
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
                if !storage.try_restore(&queue, &application).await {
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
