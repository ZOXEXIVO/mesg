use crate::storage::Message;
use std::cell::Cell;
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver};

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
    sender: UnboundedSender<Message>
}

impl DataNotifier {
    pub fn new(sender: UnboundedSender<Message>) -> Self {
        DataNotifier {
            sender
        }
    }
    
    pub fn send(&self, message: Message) -> Result<SendResult, SendError> {
        match self.sender.send(message) {
            Ok(res) => {
                Ok(SendResult{
                    
                })
            },
            Err(error) =>  {
                Err(SendError{
                    
                })
            }
        }
    }
}

pub struct SendResult {

}

pub struct SendError {
    
}