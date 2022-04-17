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
use tokio::sync::RwLock;
use tokio::time::Duration;

pub struct Consumer {
    pub id: u32,
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
        let consumer = Consumer { id };

        let storage = Arc::clone(&storage);

        const SUBSCRIBTION_DELAY: u64 = 100_u64;

        tokio::spawn(async move {
            let mut subscriber: Option<Subscriber>;

            loop {
                subscriber = storage.subscribe(&queue, &application);

                if subscriber.is_some() {
                    info!(
                        "subscribed to queue={}, application={}, consumer_id={}",
                        &queue, &application, consumer.id
                    );
                    break;
                }

                tokio::time::sleep(Duration::from_millis(SUBSCRIBTION_DELAY)).await;

                info!(
                    "waiting for data subscriber {} ms, consumer_id={}",
                    SUBSCRIBTION_DELAY, consumer.id
                );
            }

            let mut data_subscribtion = subscriber.as_mut().unwrap();

            while let Some(event) = (&mut data_subscribtion).await {
                if let sled::Event::Insert { key: _, value: _ } = event {
                    if let Some(message) = storage.pop(&queue, &application).await {
                        let id = message.id;
                        let item = ConsumerItem::from(message);

                        if let Err(err) = data_tx.send(item).await {
                            if !storage.uncommit_inner(id, &queue, &application).await {
                                error!(
                                    "uncommit error id={}, queue={}, application={}, err={}",
                                    id, &queue, &application, err
                                );
                            }
                        }
                    }
                }
            }
        });

        consumer
    }
}

pub struct MesgConsumer {
    pub id: u32,
    pub queue: String,
    pub reciever: Receiver<ConsumerItem>,
    pub shudown_channel: UnboundedSender<u32>,
}

impl MesgConsumer {
    pub fn new(
        id: u32,
        queue: String,
        reciever: Receiver<ConsumerItem>,
        shudown_channel: UnboundedSender<u32>,
    ) -> Self {
        MesgConsumer {
            id,
            queue,
            reciever,
            shudown_channel,
        }
    }
}

impl Future for MesgConsumer {
    type Output = ConsumerItem;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.reciever.poll_recv(cx) {
            Poll::Ready(citem) => {
                if let Some(item) = citem {
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
        MesgConsumer::new(handle.id, handle.queue, handle.data_rx, handle.shutdown_tx)
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

        info!("send shutdown message for consumer_id={}", self.id);

        if let Err(err) = self.shudown_channel.send(self.id) {
            error!(
                "error sending shutdown message to consumer_id={}, error={}",
                self.id, err
            );
        }

        info!("consumer disconnected");
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
                        consumers_guard.remove(consumer_pos);
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
