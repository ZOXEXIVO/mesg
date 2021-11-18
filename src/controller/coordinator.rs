use std::sync::Arc;
use log::info;
use tokio::sync::{Mutex, RwLock};
use crate::controller::{Consumer};

pub struct ConsumerCoordinator;

impl ConsumerCoordinator {
    pub async fn start(consumers: Arc<RwLock<Vec<Consumer>>>) {
        let consumers = consumers.read().await;
        
        // for consumer in consumers {
        //     tokio::spawn(async move {
        //         loop {
        //             let queue_consumer = notification.get_next_consumer().await;
        // 
        //             tokio::spawn(async move {
        //                 self.consumer_worker(queue_consumer).await
        //             })
        //         }
        //     });
        // }
    }
    
    pub async fn consumer_worker(&self, queue: String) {
        
    }
}