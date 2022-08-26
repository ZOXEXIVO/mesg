use crate::consumer::{ConsumerConfig, ConsumerDto};
use crate::storage::Storage;
use log::{error, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

pub struct StaleEventsWatcher;

impl StaleEventsWatcher {
    fn start(storage: Arc<Storage>, config: ConsumerConfig, notify: Arc<Notify>) -> JoinHandle<()> {
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
                    let id = message.id;
                    let item = ConsumerDto::from(message);

                    if let Err(err) = data_tx.send(item).await {
                        if !storage
                            .revert_inner(id, &config.queue, &config.application)
                            .await
                        {
                            error!(
                                "revert_inner error consumer_id={}, id={}, queue={}, application={}, err={}",
                                config.consumer_id, id, &config.queue, &config.application, err
                            );
                        }
                    }
                } else if attempt > 50 {
                    attempt = 0;

                    info!(
                        "consumer parked to queue={}, application={}, consumer_id={}",
                        &config.queue, &config.application, config.consumer_id
                    );

                    notified_task.await;
                } else {
                    attempt += 1;

                    let sleep_time_ms = 100 * attempt;

                    tokio::time::sleep(Duration::from_millis(sleep_time_ms as u64)).await;
                }
            }
        })
    }
}
