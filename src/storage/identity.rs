use sled::IVec;

pub struct Identity;

const IDENTITY_KEY: &str = "identity";

impl Identity {
    pub fn get(db: &sled::Db, queue: &str) -> (u64, IVec) {
        let identity_key = format!("{IDENTITY_KEY}_{queue}");

        let mut current_value = 0;

        let identity_value = db
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

                Some(number.to_be_bytes().to_vec())
            })
            .unwrap();

        match identity_value {
            Some(identity_val) => (current_value, identity_val),
            None => (
                current_value,
                IVec::from(current_value.to_be_bytes().to_vec()),
            ),
        }
    }
}
