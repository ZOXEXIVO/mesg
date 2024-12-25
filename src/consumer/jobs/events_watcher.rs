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
        EventsWatcher{
            handle: None
        }
    }
}

impl ConsumerBackgroundJob for EventsWatcher {
    fn start(&mut self, storage: Arc<MesgStorage>, config: ConsumerConfig, notify: Arc<Notify>, data_tx: Sender<ConsumerDto>) {
        self.handle = Some(tokio::spawn(async move {
            loop {
                let notified_task = notify.notified();

                let mut attempt: u16 = 0;

                if let Ok(Some(message)) = storage.pop(
                    &config.queue,
                    &config.application,
                    config.invisibility_timeout,
                ).await
                {
                    ConsumerStatistics::consumed(config.consumer_id);

                    let id = message.id.clone();

                    match data_tx.send(ConsumerDto::from(message)).await {
                        Ok(()) => continue,
                        Err(err) => {
                            warn!(
                                "consumer[id={}] send data[id={}] error, queue={}, application={}, err={}",
                                config.consumer_id, id, &config.queue, &config.application, err
                            );

                            let revert_result = storage
                                .revert(Uuid::from_str(&id).unwrap(), &config.queue, &config.application)
                                .await;

                            if revert_result.is_err()
                            {
                                error!(
                                "revert_inner error consumer_id={}, id={}, queue={}, application={}, err={}",
                                config.consumer_id, id, &config.queue, &config.application, err);
                            }

                            warn!(
                                "consumer[id={}] stale_events_watcher exited",
                                config.consumer_id
                            );

                            break;
                        }
                    }
                } else if attempt > 5 {
                    attempt = 0;

                    debug!(
                        "consumer parked to queue={}, application={}, consumer_id={}",
                        &config.queue, &config.application, config.consumer_id
                    );

                    notified_task.await;

                    debug!(
                        "consumer notified, queue={}, application={}",
                        &config.queue, &config.application
                    );
                } else {
                    attempt += 1;

                    let sleep_time_ms = 200 * attempt;

                    tokio::time::sleep(Duration::from_millis(sleep_time_ms as u64)).await;
                }
            }
        }));
    }

    fn stop(&mut self) {
        if let Some(hdl) = self.handle.as_ref() {
            hdl.abort();
        }
    }
}