use crate::server::server::MesgServerOptions;
use crate::storage::Storage;
use tonic::Request;

use crate::metrics::MetricsWriter;
use crate::server::network::grpc::mesg_service_server::MesgService;
use crate::server::network::grpc::{
    CommitRequest, CommitResponse, PullRequest, PushRequest, PushResponse,
};
use crate::server::network::response::PullResponseStream;

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

        self.metrics.inc_push_operation();

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

        self.metrics.inc_pull_operation();

        Ok(tonic::Response::new(pull_stream))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> std::result::Result<tonic::Response<CommitResponse>, tonic::Status> {
        let req = request.into_inner();

        self.storage.commit(req.queue, req.message_id).await;

        self.metrics.inc_commit_operation();

        Ok(tonic::Response::new(CommitResponse {}))
    }
}
