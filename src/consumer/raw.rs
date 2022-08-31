use crate::consumer::{ConsumerDto, ConsumerHandle};
use crate::metrics::StaticMetricsWriter;
use log::{error, info};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{Receiver, UnboundedSender};

pub struct RawConsumer {
    pub id: u32,
    pub queue: String,
    pub application: String,
    pub receiver: Receiver<ConsumerDto>,
    pub shutdown_channel: UnboundedSender<u32>,
}

impl RawConsumer {
    pub fn new(
        id: u32,
        queue: String,
        application: String,
        receiver: Receiver<ConsumerDto>,
        shutdown_channel: UnboundedSender<u32>,
    ) -> Self {
        RawConsumer {
            id,
            queue,
            application,
            receiver,
            shutdown_channel,
        }
    }
}

impl From<ConsumerHandle> for RawConsumer {
    fn from(handle: ConsumerHandle) -> Self {
        RawConsumer::new(
            handle.id,
            handle.queue,
            handle.application,
            handle.data_rx,
            handle.shutdown_tx,
        )
    }
}

impl Future for RawConsumer {
    type Output = ConsumerDto;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(item),
            _ => Poll::Pending,
        }
    }
}

impl Drop for RawConsumer {
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
