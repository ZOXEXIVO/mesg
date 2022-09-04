use crate::storage::{IdPair, QueueNames};
use sled::IVec;

pub struct Identity;

impl Identity {
    pub async fn generate(db: &sled::Db, queue: &str) -> IdPair {
        let mut current_value = 0;

        db.fetch_and_update(QueueNames::identity(queue), |old| {
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

        IdPair::from_value(current_value)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::Identity;
    use sled::Config;

    #[tokio::test]
    async fn identity_generate_is_correct() {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        assert_eq!(0, (Identity::generate(&db, "queue1").await).value());
        assert_eq!(1, (Identity::generate(&db, "queue1").await).value());
        assert_eq!(2, (Identity::generate(&db, "queue1").await).value());
        assert_eq!(3, (Identity::generate(&db, "queue1").await).value());

        assert_eq!(0, (Identity::generate(&db, "queue2").await).value());
        assert_eq!(1, (Identity::generate(&db, "queue2").await).value());
        assert_eq!(2, (Identity::generate(&db, "queue2").await).value());
        assert_eq!(3, (Identity::generate(&db, "queue2").await).value());
    }
}
