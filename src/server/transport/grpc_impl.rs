use tonic::Request;

use crate::server::service::{CommitRequestModel, Mesg, PullRequestModel, PushRequestModel};
use crate::server::transport::grpc::mesg_protocol_server::MesgProtocol;
use crate::server::transport::grpc::{
    CommitRequest, CommitResponse, PullRequest, PushRequest, PushResponse,
};
use crate::server::transport::response::PullResponseStream;

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
                len: message.len,
                broadcast: message.broadcast,
            })
            .await;

        Ok(tonic::Response::new(PushResponse { ack: result.ack }))
    }

    type PullStream = PullResponseStream;

    async fn pull(
        &self,
        request: Request<PullRequest>,
    ) -> std::result::Result<tonic::Response<Self::PullStream>, tonic::Status> {
        let req = request.into_inner();

        let result = self.inner.pull(PullRequestModel { queue: req.queue }).await;

        let pull_stream = PullResponseStream {
            reader: result.reader,
        };

        Ok(tonic::Response::new(pull_stream))
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
