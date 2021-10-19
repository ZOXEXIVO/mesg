use crate::server::MesgServerOptions;
use crate::storage::Storage;
use tonic::Request;

use crate::metrics::MetricsWriter;
use crate::server::transport::grpc::mesg_protocol_server::MesgProtocol;
use crate::server::transport::grpc::{
    CommitRequest, CommitResponse, PullRequest, PushRequest, PushResponse,
};
use crate::server::transport::response::PullResponseStream;

pub struct MesgProtocolService {
    storage: Storage,
}

impl MesgProtocolService {
    pub fn new(options: MesgServerOptions) -> Self {
        Self {
            storage: Storage::new(),
        }
    }
}

#[tonic::async_trait]
impl MesgProtocol for MesgProtocolService {
    async fn push(
        &self,
        request: Request<PushRequest>,
    ) -> std::result::Result<tonic::Response<PushResponse>, tonic::Status> {
        let message = request.into_inner();

        self.storage.push(message.queue.clone(), message.data).await;

        MetricsWriter::inc_push_metric();

        Ok(tonic::Response::new(PushResponse { ack: true }))
    }

    type PullStream = PullResponseStream;

    async fn pull(
        &self,
        request: Request<PullRequest>,
    ) -> std::result::Result<tonic::Response<Self::PullStream>, tonic::Status> {
        let req = request.into_inner();

        let pull_stream = PullResponseStream {
            reader: self.storage.get_reader(req.queue),
        };

        MetricsWriter::inc_consumers_count_metric();

        Ok(tonic::Response::new(pull_stream))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> std::result::Result<tonic::Response<CommitResponse>, tonic::Status> {
        let req = request.into_inner();

        self.storage.commit(req.queue, req.message_id).await;

        MetricsWriter::inc_commit_metric();

        Ok(tonic::Response::new(CommitResponse {}))
    }
}
