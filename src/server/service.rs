use crate::server::server::MesgServerOptions;
use crate::storage::{Storage, StorageReader, Message};
use log::info;
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::codegen::Stream;
use tonic::{Request};

use crate::metrics::MetricsWriter;
use crate::server::network::grpc::{PushRequest, PushResponse, PullRequest, CommitRequest, CommitResponse, PullResponse};
use crate::server::network::grpc::mesg_service_server::MesgService;

pub struct MesgInternalService {
    storage: Storage,
    metrics: MetricsWriter,
}

impl MesgInternalService {
    pub fn new(options: MesgServerOptions, metrics_writer: MetricsWriter) -> Self {
        Self {
            storage: Storage::new(metrics_writer.clone()),
            metrics: metrics_writer,
        }
    }
}

#[tonic::async_trait]
impl MesgService for MesgInternalService {
    async fn push(
        &self,
        request: Request<PushRequest>,
    ) -> std::result::Result<tonic::Response<PushResponse>, tonic::Status> {
        let message = request.into_inner();

        self.storage.push(message.queue.clone(), message.data).await;

        //self.metrics.inc_push_operation();

        Ok(tonic::Response::new(PushResponse { ack: true }))
    }

    type PullStream = PullResponseStream;

    async fn pull(
        &self,
        request: Request<PullRequest>,
    ) -> std::result::Result<tonic::Response<Self::PullStream>, tonic::Status> {
        let req = request.into_inner();

        let pull_stream = PullResponseStream {
            queue: req.queue.clone(),
            reader: self.storage.get_reader(req.queue),
        };

        //self.metrics.inc_pull_operation();

        Ok(tonic::Response::new(pull_stream))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> std::result::Result<tonic::Response<CommitResponse>, tonic::Status> {
        let req = request.into_inner();

        self.storage.commit(req.queue, req.message_id).await;

        //self.metrics.inc_commit_operation();

        Ok(tonic::Response::new(CommitResponse {}))
    }
}

pub struct PullResponseStream {
    queue: String,
    pub reader: StorageReader,
}

impl Stream for PullResponseStream {
    type Item = std::result::Result<PullResponse, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        info!("poll_next");

        match Pin::new(self).reader.receiver.poll_recv(cx) {
            Poll::Ready(item_option) => {
                info!("receiver.poll_recv: Ready");
                match item_option {
                    Some(item) => {
                        info!("receiver.poll_recv: Ready: Some");
                        let vec = item.data.to_vec();
                        Poll::Ready(Some(Ok(PullResponse{                            
                            len: vec.len() as i32,
                            data: vec,
                            message_id: item.id.to_string()
                        })))
                    },
                    None => {
                        info!("receiver.poll_recv: Ready: None");
                        Poll::Pending
                    }
                }
            },
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
