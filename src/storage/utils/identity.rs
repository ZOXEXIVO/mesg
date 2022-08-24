use crate::storage::{IdPair, QueueNames};
use sled::IVec;

pub struct Identity;

impl Identity {
    pub async fn generate(db: &sled::Db, queue: &str) -> IdPair {
        let identity_key = QueueNames::identity(queue);

        let mut current_value = 0;

        let _ = db
            .fetch_and_update(identity_key, |old| {
                let number = match old {
                    Some(bytes) => {
                        let array: [u8; 8] = bytes.try_into().unwrap();
                        let number = u64::from_be_bytes(array);
                        number + 1
                    }
                    None => 0,
                };

                current_value = number;

                Some(IVec::from(&number.to_be_bytes()))
            })
            .unwrap();

        db.flush_async().await.unwrap();

        IdPair::from_value(current_value)
    }
}
