use crate::storage::Message;
use crossbeam_channel::{Sender, Receiver};
use std::cell::Cell;

pub struct DataNotifiers {
    pub notifiers: Vec<DataNotifier>,
    pub last_notifier_idx: Cell<u32>
}

impl DataNotifiers {
    pub fn new() -> Self {
        DataNotifiers {
            notifiers: Vec::new(),
            last_notifier_idx: Cell::new(0)
        }
    }
    
    pub fn send(&self, message: Message) {
        if self.notifiers.is_empty() {
            return;
        }

        let mut current_idx = self.last_notifier_idx.get() + 1;

        current_idx %= self.notifiers.len() as u32;
        
        self.last_notifier_idx.set(current_idx);
        
        let notifier = &self.notifiers[current_idx as usize];
        
        notifier.send(message);
    }
}

pub struct DataNotifier {
    sender: Sender<Message>,
    receiver: Receiver<Message>
}

impl DataNotifier {
    pub fn new(sender: Sender<Message>,  receiver: Receiver<Message>) -> Self {
        DataNotifier {
            sender,
            receiver
        }
    }
    
    pub fn send(&self, message: Message) {
        self.sender.send(message).unwrap();
    }
    
    pub fn get_reciever(&self) -> &Receiver<Message> {
        &self.receiver
    }
}