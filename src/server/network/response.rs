use crate::server::network::grpc::PullResponse;
use crate::storage::StorageReader;
use std::pin::Pin;
use std::task::{Context, Poll};
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
                        len: item.data.len() as i32,
                        data: item.data.to_vec(),
                        message_id: item.id.to_string(),
                    })))
                } else {
                    let (_, rx) = &store.notification;

                    let waker = cx.waker().clone();
                    let mut reciever = rx.clone();

                    tokio::spawn(async move {
                        if reciever.changed().await.is_ok() {
                            waker.wake();
                        }
                    });

                    Poll::Pending
                }
            }
            None => {
                //TODO
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
