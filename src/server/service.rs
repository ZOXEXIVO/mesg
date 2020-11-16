use crate::server::grpc::mesg_service_server::MesgService;
use crate::server::grpc::{
    CommitRequest, CommitResponse, PullRequest, PullResponse, PushRequest, PushResponse,
};
use crate::server::server::MesgServerOptions;
use crate::storage::{Storage, StorageReader};
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::codegen::Stream;
use tonic::Request;

use crate::metrics::MetricsWriter;

pub struct MesgInternalService {
    storage: Storage,
    metrics: MetricsWriter,
}

impl MesgInternalService {
    pub fn new(options: &MesgServerOptions, metrics_writer: MetricsWriter) -> Self {
        Self {
            storage: Storage::new(&options.db_path, metrics_writer.clone()),
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

        self.storage.push(message.queue, &message.data[..]).await;

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
            topic: req.queue.clone(),
            reader: self.storage.pull(req.queue).await,
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

pub struct PullResponseStream {
    topic: String,
    reader: StorageReader,
}

impl Stream for PullResponseStream {
    type Item = std::result::Result<PullResponse, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let topic = &self.topic;

        Poll::Ready(None)
    }
}
