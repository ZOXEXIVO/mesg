use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::Bytes;
use crate::metrics::MetricsWriter;
use log::{info, error};
use tokio::sync::mpsc::{Receiver, UnboundedSender};
use crate::controller::ConsumerHandle;

pub struct MesgConsumer {
    pub id: u32,
    pub reciever: Receiver<ConsumerItem>,
    pub shudown_channel: UnboundedSender<u32>
}

impl MesgConsumer {
    pub fn new(id: u32, reciever: Receiver<ConsumerItem>, shudown_channel: UnboundedSender<u32>) -> Self {
        MesgConsumer {
            id,
            reciever,
            shudown_channel
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
            Poll::Pending => Poll::Pending
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
    pub data: Bytes
}

impl Clone for ConsumerItem {
    fn clone(&self) -> Self {
        ConsumerItem {
            id: self.id,
            data: Bytes::clone(&self.data),
        }
    }
}

impl Drop for MesgConsumer {
    fn drop(&mut self) {
        MetricsWriter::decr_consumers_count_metric();

        info!("send shutdown message for consumer_id={}", self.id);
        
        if let Err(err) = self.shudown_channel.send(self.id) {
            error!("error sending shutdown message to consumer_id={}, error={}", self.id, err);
        }

        info!("consumer disconnected");
    }
}
