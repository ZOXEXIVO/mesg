use crate::server::network::grpc::PullResponse;
use crate::storage::StorageReader;
use log::info;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::Duration;
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
                    let cloned_message = item.clone();

                    Poll::Ready(Some(Ok(PullResponse {
                        len: cloned_message.data.len() as i32,
                        data: cloned_message.data.to_vec(),
                        message_id: cloned_message.id.to_string(),
                    })))
                } else {
                    let waker = cx.waker().clone();

                    tokio::spawn(async {
                        tokio::time::sleep(Duration::from_micros(1000)).await;
                        waker.wake();
                    });

                    Poll::Pending
                }
            }
            None => {
                info!("receiver.poll_recv: None");
                Poll::Pending
            }
        }
    }
}

impl Drop for PullResponseStream {
    fn drop(&mut self) {
        println!("dropped");
    }
}
