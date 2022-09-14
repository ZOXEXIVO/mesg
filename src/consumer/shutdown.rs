use crate::consumer::Consumer;
use log::{info, warn};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::RwLock;

pub struct ConsumersShutdownWaiter;

impl ConsumersShutdownWaiter {
    // waiting consumers shutdown
    pub fn wait(consumers: Arc<RwLock<Vec<Consumer>>>, mut shutdown_rx: UnboundedReceiver<u32>) {
        tokio::spawn(async move {
            while let Some(consumer_id_to_remove) = shutdown_rx.recv().await {
                let mut consumers_guard = consumers.write().await;

                match consumers_guard
                    .iter()
                    .position(|c| c.id == consumer_id_to_remove)
                {
                    Some(consumer_pos) => {
                        let consumer = consumers_guard.remove(consumer_pos);

                        consumer.shutdown().await;

                        info!("consumer[id={}] removed", consumer_id_to_remove)
                    }
                    None => {
                        warn!(
                            "consumer[id={}] removing error: not found",
                            consumer_id_to_remove
                        )
                    }
                }
            }
        });
    }
}
