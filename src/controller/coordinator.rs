use std::sync::Arc;
use tokio::sync::Mutex;
use crate::controller::Consumer;

pub struct ConsumerCoordinator;

impl ConsumerCoordinator {
    pub fn new() -> Self {
        ConsumerCoordinator {}
    }

    pub fn start(&self, consumers: Arc<Mutex<Vec<Consumer>>>) {
        tokio::spawn(async move {
            // while let Some(consumer_id_to_remove) = receiver.recv().await {
            //     let mut consumers_guard = consumers.lock().await;
            // 
            //     match consumers_guard.iter().position(|c| c.id == consumer_id_to_remove) {
            //         Some(consumer_pos) => {
            //             consumers_guard.remove(consumer_pos);
            //             info!("consumer_id={} removed", consumer_id_to_remove)
            //         },
            //         None => {
            //             info!("cannot remove consumer_id={}", consumer_id_to_remove)
            //         }
            //     }
            // }
        });
    }
}