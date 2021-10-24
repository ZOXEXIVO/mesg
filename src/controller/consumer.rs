use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::Bytes;
use crate::metrics::MetricsWriter;
use log::{debug, info};
use tokio::sync::broadcast::Receiver;
use tokio::time::Duration;

pub struct MesgConsumer {
    pub reciever: Receiver<ConsumerItem>
}

impl Future for MesgConsumer {
    type Output = ConsumerItem;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Ok(item) = self.reciever.try_recv() {
            Poll::Ready(ConsumerItem{
                id: item.id,
                data: item.data
            })
        }
        else {
            let waker = cx.waker().clone();
            
            tokio::task::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                waker.wake();
            });
            
            Poll::Pending
        }       
    }   
}

pub struct ConsumerItem {
    pub id: i64,
    pub data: Bytes
}

impl Clone for ConsumerItem {
    fn clone(&self) -> Self {
        ConsumerItem{
            id: self.id.clone(),
            data: Bytes::clone(&self.data)
        }
    }
}

impl Drop for MesgConsumer {
    fn drop(&mut self) {
        MetricsWriter::decr_consumers_count_metric();
        info!("client disconnected");
    }
}
