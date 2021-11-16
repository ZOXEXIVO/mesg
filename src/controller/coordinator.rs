use std::sync::Arc;
use log::info;
use tokio::sync::Mutex;
use crate::controller::{Consumer, ConsumerAddedNotification};

pub struct ConsumerCoordinator;

impl ConsumerCoordinator {
    pub fn new() -> Self {
        ConsumerCoordinator {}
    }

    pub fn start(&self, consumers: Arc<Mutex<Vec<Consumer>>>, notification: ConsumerAddedNotification) {
        info!("shudown waiter starts");
        
        tokio::spawn(async move {
            loop {
                //let queue_consumer = notification.get_next_consumer().await;
                
                // tokio::spawn(async move {
                //     self.consumer_worker(queue_consumer).await
                // })
            }
        });
    }
    
    pub async fn consumer_worker(&self, queue: String) {
        
    }
}