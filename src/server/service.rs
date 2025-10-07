use std::str::FromStr;
use crate::consumer::RawConsumer;
use crate::controller::MesgController;
use crate::metrics::StaticMetricsWriter;
use async_trait::async_trait;
use bytes::Bytes;
use log::info;
use uuid::Uuid;

#[async_trait]
pub trait Mesg {
    async fn push(&self, request: PushRequestModel) -> PushResponseModel;
    async fn pull(&self, request: PullRequestModel) -> PullResponseModel;
    async fn commit(&self, request: CommitRequestModel) -> CommitResponseModel;
    async fn rollback(&self, request: RollbackRequestModel) -> RollbackResponseModel;
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
            success: self
                .controller
                .push(&request.queue, request.data, request.is_broadcast)
                .await,
        }
    }

    async fn pull(&self, request: PullRequestModel) -> PullResponseModel {
        StaticMetricsWriter::inc_consumers_count_metric(&request.queue);

        let consumer = self
            .controller
            .create_consumer(
                &request.queue,
                &request.application,
                request.invisibility_timeout_ms,
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

        let uuid = Uuid::from_str(&request.id).unwrap();

        CommitResponseModel {
            success: self
                .controller
                .commit(
                    uuid,
                    &request.queue,
                    &request.application
                )
                .await,
        }
    }

    async fn rollback(&self, request: RollbackRequestModel) -> RollbackResponseModel {
        StaticMetricsWriter::inc_rollback_metric(&request.queue);

        let uuid = Uuid::from_str(&request.id).unwrap();

        RollbackResponseModel {
            success: self
                .controller
                .rollback(
                    uuid,
                    &request.queue,
                    &request.application
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
    pub invisibility_timeout_ms: i32,
}

pub struct PullResponseModel {
    pub consumer: RawConsumer,
}

// Commit

pub struct CommitRequestModel {
    pub id: String,
    pub queue: String,
    pub application: String
}

pub struct CommitResponseModel {
    pub success: bool,
}

// Commit

pub struct RollbackRequestModel {
    pub id: String,
    pub queue: String,
    pub application: String
}

pub struct RollbackResponseModel {
    pub success: bool,
}
