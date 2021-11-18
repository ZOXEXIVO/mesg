use crate::controller::ConsumerHandle;
use crate::metrics::MetricsWriter;
use crate::storage::{Storage, Message};
use bytes::Bytes;
use log::{error, info};
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

        tokio::spawn(async move {
            loop {
                info!("consumer {} try pop", consumer.id);
                
                let mut attempt: u8 = 1;
                
                if let Some(messsage) = storage
                    .pop(&queue, &application, invisibility_timeout)
                    .await
                {
                    let item = ConsumerItem::from(messsage);

                    data_tx.send(item).await;

                    attempt = 1;
                } else {
                    // 100, 300, 500, 700, 900
                    
                    if attempt < 10 {
                        attempt = attempt + 1;
                    }
                    
                    let sleep_time_ms = 100 * attempt;
                    
                    tokio::time::sleep(Duration::from_millis(sleep_time_ms as u64)).await;
                }
            }
        });

        consumer
    }
}

pub struct MesgConsumer {
    pub id: u32,
    pub reciever: Receiver<ConsumerItem>,
    pub shudown_channel: UnboundedSender<u32>,
}

impl MesgConsumer {
    pub fn new(
        id: u32,
        reciever: Receiver<ConsumerItem>,
        shudown_channel: UnboundedSender<u32>,
    ) -> Self {
        MesgConsumer {
            id,
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
        MesgConsumer::new(handle.id, handle.data_rx, handle.shutdown_tx)
    }
}

pub struct ConsumerItem {
    pub id: i64,
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
            data: Bytes::clone(&message.data)
        }
    }
}

impl Drop for MesgConsumer {
    fn drop(&mut self) {
        MetricsWriter::decr_consumers_count_metric();

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
