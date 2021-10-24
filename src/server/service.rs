use async_trait::async_trait;
use crate::metrics::MetricsWriter;
use crate::controller::{MesgController, MesgConsumer};

#[async_trait]
pub trait Mesg {
    async fn push(&self, request: PushRequestModel) -> PushResponseModel;
    async fn pull(&self, request: PullRequestModel) -> PullResponseModel;
    async fn commit(&self, request: CommitRequestModel) -> CommitResponseModel;
}

pub struct MesgService {
    controller: MesgController
}

impl MesgService {
    pub fn new(controller: MesgController) -> Self {
        MesgService {
            controller
        }
    }
}

#[async_trait]
impl Mesg for MesgService {
    async fn push(&self, request: PushRequestModel) -> PushResponseModel {
        self.controller.push(&request.queue, request.data).await;

        MetricsWriter::inc_push_metric();

        PushResponseModel {
            ack: true
        }
    }

    async fn pull(&self, request: PullRequestModel) -> PullResponseModel {
        MetricsWriter::inc_consumers_count_metric();

        PullResponseModel{
            consumer: self.controller.create_consumer(&request.queue)
        }
    }

    async fn commit(&self, request: CommitRequestModel) -> CommitResponseModel {
        self.controller.commit(&request.queue, request.message_id).await;

        MetricsWriter::inc_commit_metric();

        CommitResponseModel {}
    }
}

// Push
pub struct PushRequestModel {
    pub queue: String,
    pub data: Vec<u8>,
    pub broadcast: bool
}

pub struct PushResponseModel {
    pub ack: bool,
}

// Pull

pub struct PullRequestModel {
    pub queue: String,
}

pub struct PullResponseModel {
    pub consumer: MesgConsumer
}

// Commit

pub struct CommitRequestModel {
    pub queue: String,
    pub message_id: String,
}

pub struct CommitResponseModel {}
