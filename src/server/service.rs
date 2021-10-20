use async_trait::async_trait;
use crate::storage::{Storage, StorageReader};
use crate::metrics::MetricsWriter;

#[async_trait]
pub trait Mesg {
    async fn push(&self, request: PushRequestModel) -> PushResponseModel;
    async fn pull(&self, request: PullRequestModel) -> PullResponseModel;
    async fn commit(&self, request: CommitRequestModel) -> CommitResponseModel;
}

pub struct MesgService {
    storage: Storage,
}

impl MesgService {
    pub fn new(storage: Storage) -> Self {
        MesgService {
            storage
        }
    }
}

#[async_trait]
impl Mesg for MesgService {
    async fn push(&self, request: PushRequestModel) -> PushResponseModel {
        self.storage.push(request.queue.clone(), request.data).await;

        MetricsWriter::inc_push_metric();

        PushResponseModel {
            ack: true
        }
    }

    async fn pull(&self, request: PullRequestModel) -> PullResponseModel {
        MetricsWriter::inc_consumers_count_metric();

        PullResponseModel{
            reader: self.storage.get_reader(request.queue)
        }
    }

    async fn commit(&self, request: CommitRequestModel) -> CommitResponseModel {
        self.storage.commit(request.queue, request.message_id).await;

        MetricsWriter::inc_commit_metric();

        CommitResponseModel {}
    }
}

// Push
pub struct PushRequestModel {
    pub queue: String,
    pub data: Vec<u8>,
    pub len: i64,
}

pub struct PushResponseModel {
    pub ack: bool,
}

// Pull

pub struct PullRequestModel {
    pub queue: String,
}

pub struct PullResponseModel {
    pub reader: StorageReader
}

// Commit

pub struct CommitRequestModel {
    pub queue: String,
    pub message_id: String,
}

pub struct CommitResponseModel {}
