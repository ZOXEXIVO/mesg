use std::future::Future;
use tonic::Request;

use crate::consumer::RawConsumer;
use crate::server::service::{CommitRequestModel, Mesg, PullRequestModel, PushRequestModel};
use crate::server::transport::grpc::mesg_protocol_server::MesgProtocol;
use crate::server::transport::grpc::{
    CommitRequest, CommitResponse, PullRequest, PushRequest, PushResponse,
};
use crate::server::PullResponse;
use bytes::Bytes;
use std::pin::Pin;
use std::task::{Context, Poll};
use async_trait::async_trait;
use tonic::codegen::tokio_stream::Stream;

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

#[async_trait]
impl<T: Mesg> MesgProtocol for MesgGrpcImplService<T>
where
    T: Send + Sync + 'static,
{
    async fn push(
        &self,
        request: Request<PushRequest>,
    ) -> Result<tonic::Response<PushResponse>, tonic::Status> {
        let message = request.into_inner();

        let result = self
            .inner
            .push(PushRequestModel {
                queue: message.queue,
                data: Bytes::copy_from_slice(&message.data),
                is_broadcast: message.is_broadcast,
            })
            .await;

        Ok(tonic::Response::new(PushResponse {
            success: result.success,
        }))
    }

    type PullStream = InternalStreamConsumer;

    async fn pull(
        &self,
        request: Request<PullRequest>,
    ) -> Result<tonic::Response<Self::PullStream>, tonic::Status> {
        let req = request.into_inner();

        let pull_response = self
            .inner
            .pull(PullRequestModel {
                queue: req.queue,
                application: req.application,
                invisibility_timeout_ms: req.invisibility_timeout_ms,
            })
            .await;

        Ok(tonic::Response::new(InternalStreamConsumer::new(
            pull_response.consumer,
        )))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<tonic::Response<CommitResponse>, tonic::Status> {
        let req = request.into_inner();

        let commit_response = self
            .inner
            .commit(CommitRequestModel {
                id: req.id,
                queue: req.queue,
                application: req.application,
                success: req.success,
            })
            .await;

        Ok(tonic::Response::new(CommitResponse {
            success: commit_response.success,
        }))
    }
}

pub struct InternalStreamConsumer {
    pub inner_consumer: RawConsumer,
}

impl InternalStreamConsumer {
    pub fn new(inner_consumer: RawConsumer) -> Self {
        InternalStreamConsumer { inner_consumer }
    }
}

impl Stream for InternalStreamConsumer {
    type Item = Result<PullResponse, tonic::Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner_consumer).poll(cx) {
            Poll::Ready(item) => Poll::Ready(Some(Ok(PullResponse {
                id: item.id,
                data: item.data.to_vec(),
            }))),
            _ => Poll::Pending,
        }
    }
}
