use crate::metrics::MetricsWriter;
use crate::server::transport::grpc::PullResponse;
use crate::storage::StorageReader;
use log::info;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::{sleep, Duration};
use tonic::codegen::futures_core::Stream;

pub struct PullResponseStream {
    pub reader: StorageReader,
}

impl Stream for PullResponseStream {
    type Item = std::result::Result<PullResponse, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.reader.storage.get_mut(&self.reader.queue_name) {
            Some(mut store) => {
                if let Some(item) = store.data.pop_front() {
                    Poll::Ready(Some(Ok(PullResponse {
                        len: item.data.len() as i64,
                        data: item.data.to_vec(),
                        message_id: item.id.to_string(),
                    })))
                } else {
                    let waker = cx.waker().clone();
                    let mut reciever = store.notification.subscribe();

                    tokio::spawn(async move {
                        if reciever.recv().await.is_ok() {
                            waker.wake();
                        }
                    });

                    Poll::Pending
                }
            }
            None => {
                let waker = cx.waker().clone();

                tokio::spawn(async move {
                    sleep(Duration::from_millis(1000)).await;
                    waker.wake();
                });

                Poll::Pending
            }
        }
    }
}

impl Drop for PullResponseStream {
    fn drop(&mut self) {
        MetricsWriter::decr_consumers_count_metric();
        info!("client disconnected");
    }
}
