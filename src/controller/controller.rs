use crate::storage::Storage;
use crate::controller::MesgConsumer;

pub struct MesgController {
    storage: Storage,
}

impl MesgController {
    pub fn new(storage: Storage) -> Self {
        MesgController {
            storage
        }
    }

    pub fn create_consumer(&self, queue: &str) -> MesgConsumer {
        MesgConsumer {
            reciever: self.storage.subscribe(queue)
        }
    }

    pub async fn push(&self, queue: &str, data: Vec<u8>) {
        self.storage.push(queue, data).await
    }

    pub async fn commit(&self, queue: &str, id: i64) {
        self.storage.commit(queue, id).await;
    }
}



