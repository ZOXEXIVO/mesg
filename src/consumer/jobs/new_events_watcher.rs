use crate::consumer::ConsumerConfig;
use crate::storage::Storage;
use log::{debug, info};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

pub struct NewEventsWatcher;

impl NewEventsWatcher {
    pub fn start(
        storage: Arc<Storage>,
        config: ConsumerConfig,
        notify: Arc<Notify>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut subscriber = storage.subscribe(&config.queue, &config.application).await;

            info!(
                "subscribed to queue={}, application={}, consumer_id={}",
                &config.queue, &config.application, config.consumer_id
            );

            while let Some(event) = (&mut subscriber).await {
                if let sled::Event::Insert { key: _, value: _ } = event {
                    notify.notify_one();
                }
            }
        })
    }
}
