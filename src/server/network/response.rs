use crate::server::network::grpc::PullResponse;
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
        info!("poll_next");

        match self.reader.storage.get_mut(&self.reader.queue_name) {
            Some(mut store) => {
                info!("store exists");
                if let Some(item) = store.data.pop_front() {
                    info!("return item");

                    Poll::Ready(Some(Ok(PullResponse {
                        len: item.data.len() as i32,
                        data: item.data.to_vec(),
                        message_id: item.id.to_string(),
                    })))
                } else {
                    info!("item not found. subscribe");

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
        info!("client disconnected");
    }
}
