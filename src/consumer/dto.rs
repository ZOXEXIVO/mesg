use bytes::Bytes;
use crate::storage::Message;

pub struct ConsumerDto {
    pub id: String,
    pub data: Bytes,
}

impl Clone for ConsumerDto {
    fn clone(&self) -> Self {
        ConsumerDto {
            id: self.id.clone(),
            data: Bytes::clone(&self.data),
        }
    }
}

impl From<Message> for ConsumerDto {
    fn from(message: Message) -> Self {
        ConsumerDto {
            id: message.id,
            data: Bytes::clone(&message.data),
        }
    }
}
