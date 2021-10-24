use std::future::Future;
use tonic::Request;

use crate::controller::MesgConsumer;
use crate::server::service::{CommitRequestModel, Mesg, PullRequestModel, PushRequestModel};
use crate::server::transport::grpc::mesg_protocol_server::MesgProtocol;
use crate::server::transport::grpc::{
    CommitRequest, CommitResponse, PullRequest, PushRequest, PushResponse,
};
use crate::server::PullResponse;
use log::debug;
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::codegen::futures_core::Stream;

pub struct MesgGrpcImplService<T: Mesg>
where
    T: Send + Sync + 'static,
{
    inner: T,
}

impl<'g, T: Mesg> MesgGrpcImplService<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

#[tonic::async_trait]
impl<T: Mesg> MesgProtocol for MesgGrpcImplService<T>
where
    T: Send + Sync + 'static,
{
    async fn push(
        &self,
        request: Request<PushRequest>,
    ) -> std::result::Result<tonic::Response<PushResponse>, tonic::Status> {
        let message = request.into_inner();

        let result = self
            .inner
            .push(PushRequestModel {
                queue: message.queue,
                data: message.data,
                broadcast: message.broadcast,
            })
            .await;

        Ok(tonic::Response::new(PushResponse { ack: result.ack }))
    }

    type PullStream = InternalConsumer;

    async fn pull(
        &self,
        request: Request<PullRequest>,
    ) -> std::result::Result<tonic::Response<Self::PullStream>, tonic::Status> {
        let req = request.into_inner();

        let result = self.inner.pull(PullRequestModel { queue: req.queue }).await;

        let internal_consumer = InternalConsumer::new(result.consumer);

        Ok(tonic::Response::new(internal_consumer))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> std::result::Result<tonic::Response<CommitResponse>, tonic::Status> {
        let req = request.into_inner();

        self.inner
            .commit(CommitRequestModel {
                queue: req.queue,
                id: req.id,
            })
            .await;

        Ok(tonic::Response::new(CommitResponse {}))
    }
}

pub struct InternalConsumer {
    pub inner_consumer: MesgConsumer,
}

impl InternalConsumer {
    pub fn new(inner_consumer: MesgConsumer) -> Self {
        InternalConsumer { inner_consumer }
    }
}

impl Stream for InternalConsumer {
    type Item = std::result::Result<PullResponse, tonic::Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner_consumer).poll(cx) {
            Poll::Ready(item) => {
                debug!("poll ready");

                Poll::Ready(Some(Ok(PullResponse {
                    id: item.id,
                    data: item.data.to_vec(), //TODO Allocation
                })))
            }
            _ => {
                debug!("poll pending");
                Poll::Pending
            }
        }
    }
}
