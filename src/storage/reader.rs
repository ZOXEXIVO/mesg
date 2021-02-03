use crate::storage::Message;
use tokio::sync::mpsc::UnboundedReceiver;

pub struct StorageReader {
    pub receiver: UnboundedReceiver<Message>
}

// impl Clone for StorageReader {
//     fn clone(&self) -> Self {
//         StorageReader {
//             receiver: self.receiver.clone()
//         }
//     }
// }