use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::Bytes;
use crate::metrics::MetricsWriter;
use log::{info};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::Sender;

pub struct MesgConsumer {
    pub reciever: UnboundedReceiver<ConsumerItem>,
    pub shudown_channel: Sender<()>
}

impl MesgConsumer {
    pub fn new(reciever: UnboundedReceiver<ConsumerItem>, shudown_channel: Sender<()>) -> Self {
        MesgConsumer {
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

pub struct ConsumerItem {
    pub id: i64,
    pub data: Bytes,
    pub consumer_id: u32,
}

impl Clone for ConsumerItem {
    fn clone(&self) -> Self {
        ConsumerItem {
            id: self.id, 
            consumer_id: self.consumer_id,
            data: Bytes::clone(&self.data),
        }
    }
}

impl Drop for MesgConsumer {
    fn drop(&mut self) {
        MetricsWriter::decr_consumers_count_metric();

        self.shudown_channel.try_send(());
        
        info!("client disconnected");
    }
}
