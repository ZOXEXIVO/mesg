﻿use crate::controller::Consumer;
use std::sync::Arc;
use tokio::sync::{RwLock};

pub struct ConsumerCoordinator;

impl ConsumerCoordinator {
    pub fn start(consumers: Arc<RwLock<Vec<Consumer>>>) {
        tokio::spawn(async move {
            // Run consuming task
            let consumers = consumers.read().await;
        });
    }

    pub async fn consumer_worker(&self, queue: String) {}
}
