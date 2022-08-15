use crate::controller::ConsumerHandle;
use crate::metrics::StaticMetricsWriter;
use crate::storage::{Message, Storage};
use bytes::Bytes;
use log::{error, info};
use sled::Subscriber;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::Duration;

pub struct Consumer {
    pub id: u32,

    queue_watcher_task: JoinHandle<()>,
    stale_events_watcher_task: JoinHandle<()>,
    revert_restorer_task: JoinHandle<()>,

    invisibility_timeout: u32,
}

impl Consumer {
    pub fn new(
        id: u32,
        storage: Arc<Storage>,
        queue: String,
        application: String,
        invisibility_timeout: u32,
        data_tx: Sender<ConsumerItem>,
    ) -> Self {
        let consume_wakeup_task = Arc::new(Notify::new());

        Consumer {
            id,
            queue_watcher_task: Consumer::start_queue_events_watcher(
                id,
                Arc::clone(&storage),
                queue.clone(),
                application.clone(),
                consume_wakeup_task.clone(),
            ),
            stale_events_watcher_task: Consumer::start_stale_events_watcher(
                id,
                Arc::clone(&storage),
                queue.clone(),
                application.clone(),
                data_tx,
                consume_wakeup_task,
                invisibility_timeout,
            ),
            revert_restorer_task: Consumer::start_revert_restorer(
                Arc::clone(&storage),
                queue,
                application,
            ),
            invisibility_timeout,
        }
    }

    fn start_stale_events_watcher(
        consumer_id: u32,
        storage: Arc<Storage>,
        queue: String,
        application: String,
        data_tx: Sender<ConsumerItem>,
        notify: Arc<Notify>,
        invisibility_timeout: u32,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut attempt: u16 = 0;

            loop {
                let notified_task = notify.notified();

                if let Some(message) = storage
                    .pop(&queue, &application, invisibility_timeout)
                    .await
                {
                    let id = message.id;
                    let item = ConsumerItem::from(message);

                    if let Err(err) = data_tx.send(item).await {
                        if !storage.revert_inner(id, &queue, &application).await {
                            error!(
                                "uncommit error consumer_id={}, id={}, queue={}, application={}, err={}",
                                consumer_id, id, &queue, &application, err
                            );
                        }
                    }
                } else if attempt > 50 {
                    attempt = 0;

                    info!(
                        "consumer parked to queue={}, application={}, consumer_id={}",
                        &queue, &application, consumer_id
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

    fn start_queue_events_watcher(
        consumer_id: u32,
        storage: Arc<Storage>,
        queue: String,
        application: String,
        notify: Arc<Notify>,
    ) -> JoinHandle<()> {
        const SUBSCRIPTION_DELAY: u64 = 1000_u64;

        tokio::spawn(async move {
            let mut subscriber: Option<Subscriber>;

            loop {
                subscriber = storage.subscribe(&queue, &application).await;

                if subscriber.is_some() {
                    info!(
                        "subscribed to queue={}, application={}, consumer_id={}",
                        &queue, &application, consumer_id
                    );
                    break;
                }

                tokio::time::sleep(Duration::from_millis(SUBSCRIPTION_DELAY)).await;
            }

            let mut data_subscription = subscriber.as_mut().unwrap();

            while let Some(event) = (&mut data_subscription).await {
                if let sled::Event::Insert { key: _, value: _ } = event {
                    notify.notify_one();
                }
            }
        })
    }

    fn start_revert_restorer(
        storage: Arc<Storage>,
        queue: String,
        application: String,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut attempt: u16 = 0;

            loop {
                if !storage.try_revert(&queue, &application).await {
                    attempt += 1;

                    if attempt > 50 {
                        attempt = 0;
                    }

                    let sleep_time_ms = 100 * attempt;

                    tokio::time::sleep(Duration::from_millis(sleep_time_ms as u64)).await;
                }
            }
        })
    }

    pub async fn shutdown(&self) {
        self.queue_watcher_task.abort();
        self.stale_events_watcher_task.abort();
        self.revert_restorer_task.abort();
    }
}

pub struct MesgConsumer {
    pub id: u32,
    pub queue: String,
    pub application: String,
    pub receiver: Receiver<ConsumerItem>,
    pub shutdown_channel: UnboundedSender<u32>,
}

impl MesgConsumer {
    pub fn new(
        id: u32,
        queue: String,
        application: String,
        receiver: Receiver<ConsumerItem>,
        shutdown_channel: UnboundedSender<u32>,
    ) -> Self {
        MesgConsumer {
            id,
            queue,
            application,
            receiver,
            shutdown_channel,
        }
    }
}

impl Future for MesgConsumer {
    type Output = ConsumerItem;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(item) => {
                if let Some(item) = item {
                    Poll::Ready(item)
                } else {
                    Poll::Pending
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl From<ConsumerHandle> for MesgConsumer {
    fn from(handle: ConsumerHandle) -> Self {
        MesgConsumer::new(
            handle.id,
            handle.queue,
            handle.application,
            handle.data_rx,
            handle.shutdown_tx,
        )
    }
}

pub struct ConsumerItem {
    pub id: u64,
    pub data: Bytes,
}

impl Clone for ConsumerItem {
    fn clone(&self) -> Self {
        ConsumerItem {
            id: self.id,
            data: Bytes::clone(&self.data),
        }
    }
}

impl From<Message> for ConsumerItem {
    fn from(message: Message) -> Self {
        ConsumerItem {
            id: message.id,
            data: Bytes::clone(&message.data),
        }
    }
}

impl Drop for MesgConsumer {
    fn drop(&mut self) {
        StaticMetricsWriter::decr_consumers_count_metric(&self.queue);

        info!(
            "send shutdown message for consumer_id={}, queue={}, application={}",
            self.id, &self.queue, &self.application
        );

        if let Err(err) = self.shutdown_channel.send(self.id) {
            error!(
                "error sending shutdown message to consumer_id={}, queue={}, error={}",
                self.id, &self.queue, err
            );
        }

        info!("consumer disconnected, consumer_id={}", self.id);
    }
}

pub struct ConsumersShutdownWaiter;

impl ConsumersShutdownWaiter {
    // waiting consumers shutdown
    pub fn wait(consumers: Arc<RwLock<Vec<Consumer>>>, mut shutdown_rx: UnboundedReceiver<u32>) {
        tokio::spawn(async move {
            while let Some(consumer_id_to_remove) = shutdown_rx.recv().await {
                let mut consumers_guard = consumers.write().await;

                match consumers_guard
                    .iter()
                    .position(|c| c.id == consumer_id_to_remove)
                {
                    Some(consumer_pos) => {
                        let consumer = consumers_guard.remove(consumer_pos);

                        consumer.shutdown().await;

                        info!("consumer_id={} removed", consumer_id_to_remove)
                    }
                    None => {
                        info!("cannot remove consumer_id={}", consumer_id_to_remove)
                    }
                }
            }
        });
    }
}
