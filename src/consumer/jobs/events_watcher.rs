use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use log::{debug, error, warn};
use tokio::sync::mpsc::Sender;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use uuid::Uuid;
use crate::consumer::{ConsumerBackgroundJob, ConsumerConfig, ConsumerDto, ConsumerStatistics};
use crate::storage::MesgStorage;

pub struct EventsWatcher {
    handle: Option<JoinHandle<()>>
}

impl EventsWatcher {
    pub fn new() -> Self {
        EventsWatcher {
            handle: None
        }
    }
}

impl ConsumerBackgroundJob for EventsWatcher {
    fn start(&mut self, storage: Arc<MesgStorage>, config: ConsumerConfig, notify: Arc<Notify>, data_tx: Sender<ConsumerDto>) {
        self.handle = Some(tokio::spawn(async move {
            // Start with very short polling interval
            let mut poll_interval_ms = 10u64;
            const MIN_POLL_INTERVAL_MS: u64 = 10;
            const MAX_POLL_INTERVAL_MS: u64 = 500;
            const BACKOFF_MULTIPLIER: u64 = 2;

            debug!(
                "consumer[id={}] events_watcher started, queue={}, application={}",
                config.consumer_id, &config.queue, &config.application
            );

            loop {
                // Try to pop a message
                match storage.pop(
                    &config.queue,
                    &config.application,
                    config.invisibility_timeout,
                ).await {
                    Ok(Some(message)) => {
                        // Successfully got a message - reset poll interval
                        poll_interval_ms = MIN_POLL_INTERVAL_MS;

                        ConsumerStatistics::consumed(config.consumer_id);

                        let id = message.id.clone();

                        debug!(
                            "consumer[id={}] received message[id={}], queue={}, application={}",
                            config.consumer_id, &id, &config.queue, &config.application
                        );

                        // Try to send message to consumer
                        match data_tx.send(ConsumerDto::from(message)).await {
                            Ok(()) => {
                                // Successfully sent - give other consumers a chance
                                // Small delay to prevent one consumer from grabbing all messages
                                tokio::time::sleep(Duration::from_millis(5)).await;
                                tokio::task::yield_now().await;
                                poll_interval_ms = MIN_POLL_INTERVAL_MS; // Reset backoff
                                continue;
                            }
                            Err(err) => {
                                // Consumer disconnected or channel closed
                                warn!(
                                    "consumer[id={}] send data[id={}] error, queue={}, application={}, err={}",
                                    config.consumer_id, id, &config.queue, &config.application, err
                                );

                                // Try to rollback the message
                                let revert_result = storage
                                    .rollback(Uuid::from_str(&id).unwrap(), &config.queue, &config.application)
                                    .await;

                                if revert_result.is_err() {
                                    error!(
                                        "rollback error consumer_id={}, id={}, queue={}, application={}, err={:?}",
                                        config.consumer_id, id, &config.queue, &config.application, revert_result
                                    );
                                }

                                warn!(
                                    "consumer[id={}] events_watcher exited due to channel error",
                                    config.consumer_id
                                );

                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        // No message available - use exponential backoff with max limit
                        debug!(
                            "consumer[id={}] no messages available, waiting {}ms, queue={}, application={}",
                            config.consumer_id, poll_interval_ms, &config.queue, &config.application
                        );

                        // Wait with timeout, but also listen for notifications
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(poll_interval_ms)) => {
                                // Timeout - increase backoff for next iteration
                                poll_interval_ms = (poll_interval_ms * BACKOFF_MULTIPLIER).min(MAX_POLL_INTERVAL_MS);
                            }
                            _ = notify.notified() => {
                                // Got notified - reset to fast polling
                                debug!(
                                    "consumer[id={}] notified, resetting poll interval, queue={}, application={}",
                                    config.consumer_id, &config.queue, &config.application
                                );
                                poll_interval_ms = MIN_POLL_INTERVAL_MS;
                            }
                        }

                        // Yield to allow other tasks to run
                        tokio::task::yield_now().await;
                    }
                    Err(err) => {
                        // Storage error - log and continue with backoff
                        error!(
                            "consumer[id={}] storage.pop error, queue={}, application={}, err={:?}",
                            config.consumer_id, &config.queue, &config.application, err
                        );

                        // Wait before retrying
                        tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
                        poll_interval_ms = (poll_interval_ms * BACKOFF_MULTIPLIER).min(MAX_POLL_INTERVAL_MS);

                        // Yield to allow other tasks to run
                        tokio::task::yield_now().await;
                    }
                }
            }

            debug!(
                "consumer[id={}] events_watcher stopped, queue={}, application={}",
                config.consumer_id, &config.queue, &config.application
            );
        }));
    }

    fn stop(&mut self) {
        if let Some(hdl) = self.handle.as_ref() {
            hdl.abort();
        }
    }
}