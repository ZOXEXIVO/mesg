use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::codegen::futures_core::Stream;
use crate::metrics::MetricsWriter;
use crate::server::PullResponse;
use log::{info};

pub struct MesgConsumer {
    pub queue: String
}

impl MesgConsumer {
    pub async fn try_get_message(&self) -> Poll<(String, Vec<u8>)> {
       Poll::Ready(("".to_string(), Vec::new()))
    }
}

impl Drop for MesgConsumer {
    fn drop(&mut self) {
        MetricsWriter::decr_consumers_count_metric();
        info!("client disconnected");
    }
}
