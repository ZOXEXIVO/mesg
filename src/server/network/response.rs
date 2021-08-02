use crate::server::network::grpc::PullResponse;
use crate::storage::StorageReader;
use log::info;
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::codegen::futures_core::Stream;

pub struct PullResponseStream {
    pub reader: StorageReader,
}

impl Stream for PullResponseStream {
    type Item = std::result::Result<PullResponse, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        info!("poll_next");

        let storage = self.reader.queue_storages.lock();

        match Pin::new(self).reader.receiver.poll_recv(cx) {
            Poll::Ready(item_option) => {
                info!("receiver.poll_recv: Ready");
                match item_option {
                    Some(item) => {
                        info!("receiver.poll_recv: Ready: Some");
                        let vec = item.data.to_vec();
                        Poll::Ready(Some(Ok(PullResponse {
                            len: vec.len() as i32,
                            data: vec,
                            message_id: item.id.to_string(),
                        })))
                    }
                    None => {
                        info!("receiver.poll_recv: Ready: None");
                        Poll::Pending
                    }
                }
            }
            Poll::Pending => {
                info!("receiver.poll_recv: None");
                Poll::Pending
            }
        }

        // match self.reader.receiver.recv().await {
        //     Ok(message) => {
        //         let response = PullResponse {
        //             message_id: message.id.to_string(),
        //             len: message.data.len() as i32,
        //             data: message.data.to_vec(),
        //         };
        //
        //         Poll::Ready(Some(Ok(response)))
        //     }
        //     Err(err) => {
        //         let reader = self.reader.clone();
        //         let waker = cx.waker().clone();
        //
        //         tokio::spawn(|| {
        //             info!("check reciever");
        //
        //             match reader.receiver.try_recv() {
        //                 Ok(message) => {
        //                     info!("recieved has message");
        //                     waker.wake();
        //                 }
        //                 Err(err) => {
        //                     info!("wait 1s");
        //                     //delay_for(Duration::from_secs(1)).await;
        //                 }
        //             }
        //         });
        //
        //         Poll::Pending
        //     }
        // }
    }
}

impl Drop for PullResponseStream {
    fn drop(&mut self) {
        println!("dropped");
    }
}
