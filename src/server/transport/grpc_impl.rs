use tonic::Request;

use crate::controller::MesgConsumer;
use crate::server::service::{CommitRequestModel, Mesg, PullRequestModel, PushRequestModel};
use crate::server::transport::grpc::mesg_protocol_server::MesgProtocol;
use crate::server::transport::grpc::{
    CommitRequest, CommitResponse, PullRequest, PushRequest, PushResponse,
};
use crate::server::PullResponse;
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

        Ok(tonic::Response::new(InternalConsumer::new(result.consumer)))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> std::result::Result<tonic::Response<CommitResponse>, tonic::Status> {
        let req = request.into_inner();

        self.inner
            .commit(CommitRequestModel {
                queue: req.queue,
                message_id: req.message_id,
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

    async fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner_consumer.try_get_message() {
            Poll::Ready((message_id, data)) => Poll::Ready(Some(Ok(PullResponse {
                message_id,
                data,
                len: 0,
            }))),
            _ => Poll::Pending,
        }
    }
}
