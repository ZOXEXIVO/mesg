use crossbeam_channel::Receiver;
use crate::storage::Message;

pub struct StorageReader {
    pub receiver: Receiver<Message>
}

impl Clone for StorageReader {
    fn clone(&self) -> Self {
        StorageReader {
            receiver: self.receiver.clone()
        }
    }
}