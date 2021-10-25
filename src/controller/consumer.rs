use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::Bytes;
use crate::metrics::MetricsWriter;
use log::{debug, info};
use tokio::sync::mpsc::UnboundedReceiver;

pub struct MesgConsumer {
    pub reciever: UnboundedReceiver<ConsumerItem>,
}

impl MesgConsumer {
    pub fn new(reciever: UnboundedReceiver<ConsumerItem>) -> Self {
        MesgConsumer {
            reciever
        }
    }
}

impl Future for MesgConsumer {
    type Output = ConsumerItem;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        debug!("poll");
        
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
}

impl Clone for ConsumerItem {
    fn clone(&self) -> Self {
        ConsumerItem {
            id: self.id.clone(),
            data: Bytes::clone(&self.data),
        }
    }
}

impl Drop for MesgConsumer {
    fn drop(&mut self) {
        MetricsWriter::decr_consumers_count_metric();
        info!("client disconnected");
    }
}
