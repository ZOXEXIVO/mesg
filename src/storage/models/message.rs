use bytes::Bytes;
use std::cmp::Ordering;

pub struct Message {
    pub id: String,
    pub data: Bytes,
    pub delivered: bool,
}

impl Message {
    pub fn new(id: String, data: Bytes) -> Self {
        Message {
            id,
            data: Bytes::clone(&data),
            delivered: false,
        }
    }
}

impl Ord for Message {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for Message {}

impl PartialOrd<Message> for Message {
    fn partial_cmp(&self, other: &Message) -> Option<Ordering> { Some(self.cmp(other)) }
}

impl Clone for Message {
    fn clone(&self) -> Self {
        Message {
            id: self.id.to_owned(),
            data: Bytes::clone(&self.data),
            delivered: self.delivered,
        }
    }
}
