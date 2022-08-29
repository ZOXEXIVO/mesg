use crate::consumer::ConsumerDto;
use tokio::sync::mpsc::{Receiver, UnboundedSender};

pub struct ConsumerHandle {
    pub id: u32,
    pub queue: String,
    pub application: String,
    pub data_rx: Receiver<ConsumerDto>,
    pub shutdown_tx: UnboundedSender<u32>,
}
