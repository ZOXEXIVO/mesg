use crate::controller::{MesgConsumer, MesgController};
use crate::metrics::StaticMetricsWriter;
use async_trait::async_trait;
use bytes::Bytes;
use log::info;

#[async_trait]
pub trait Mesg {
    async fn push(&self, request: PushRequestModel) -> PushResponseModel;
    async fn pull(&self, request: PullRequestModel) -> PullResponseModel;
    async fn commit(&self, request: CommitRequestModel) -> CommitResponseModel;
}

pub struct MesgService {
    controller: MesgController,
}

impl MesgService {
    pub fn new(controller: MesgController) -> Self {
        MesgService { controller }
    }
}

#[async_trait]
impl Mesg for MesgService {
    async fn push(&self, request: PushRequestModel) -> PushResponseModel {
        StaticMetricsWriter::inc_push_metric(&request.queue);

        PushResponseModel {
            success: self.controller.push(&request.queue, request.data).await,
        }
    }

    async fn pull(&self, request: PullRequestModel) -> PullResponseModel {
        StaticMetricsWriter::inc_consumers_count_metric(&request.queue);

        let consumer = self
            .controller
            .create_consumer(
                &request.queue,
                &request.application,
                request.invisibility_timeout,
            )
            .await;

        info!(
            "consumer connected: consumer_id: {}, queue: {}, application={}",
            consumer.id, &request.queue, &request.application
        );

        PullResponseModel { consumer }
    }

    async fn commit(&self, request: CommitRequestModel) -> CommitResponseModel {
        StaticMetricsWriter::inc_commit_metric(&request.queue);

        CommitResponseModel {
            success: self
                .controller
                .commit(
                    request.id,
                    &request.queue,
                    &request.application,
                    request.success,
                )
                .await,
        }
    }
}

// Push
pub struct PushRequestModel {
    pub queue: String,
    pub data: Bytes,
    pub is_broadcast: bool,
}

pub struct PushResponseModel {
    pub success: bool,
}

// Pull

pub struct PullRequestModel {
    pub queue: String,
    pub application: String,
    pub invisibility_timeout: u32,
}

pub struct PullResponseModel {
    pub consumer: MesgConsumer,
}

// Commit

pub struct CommitRequestModel {
    pub id: u64,
    pub queue: String,
    pub application: String,

    pub success: bool,
}

pub struct CommitResponseModel {
    pub success: bool,
}
