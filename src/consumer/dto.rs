use bytes::Bytes;

pub struct ConsumerDto {
    pub id: u64,
    pub data: Bytes,
}

impl Clone for ConsumerDto {
    fn clone(&self) -> Self {
        ConsumerDto {
            id: self.id,
            data: Bytes::clone(&self.data),
        }
    }
}

// impl From<Message> for ConsumerDto {
//     fn from(message: Message) -> Self {
//         ConsumerDto {
//             id: message.id,
//             data: Bytes::clone(&message.data),
//         }
//     }
// }
