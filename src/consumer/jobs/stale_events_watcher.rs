use crate::consumer::{ConsumerConfig, ConsumerDto, ConsumerStatistics};
use crate::storage::Storage;
use log::{debug, error, warn};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

pub struct StaleEventsWatcher;

impl StaleEventsWatcher {
    pub fn start(
        storage: Arc<Storage>,
        config: ConsumerConfig,
        notify: Arc<Notify>,
        data_tx: Sender<ConsumerDto>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut attempt: u16 = 0;

            loop {
                let notified_task = notify.notified();

                if let Some(message) = storage
                    .pop(
                        &config.queue,
                        &config.application,
                        config.invisibility_timeout,
                    )
                    .await
                {
                    ConsumerStatistics::consumed(config.consumer_id);

                    let id = message.id;

                    match data_tx.send(ConsumerDto::from(message)).await {
                        Ok(()) => continue,
                        Err(err) => {
                            warn!(
                                "consumer[id={}] send data[id={}] error, queue={}, application={}, err={}",
                                config.consumer_id, id, &config.queue, &config.application, err
                            );

                            if !storage
                                .revert_inner(id, &config.queue, &config.application)
                                .await
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
        })
    }
}
