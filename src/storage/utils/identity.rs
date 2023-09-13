use uuid::Uuid;

pub struct Identity;

impl Identity {
    pub fn generate() -> Uuid {
        Uuid::new_v4()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::Identity;

    fn identity_generate_is_correct() {
        let first = Identity::generate();
        let second = Identity::generate();
        let third = Identity::generate();

        assert_eq!(first, second);
        assert_eq!(second, third);
    }
}
