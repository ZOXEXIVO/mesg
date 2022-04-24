pub struct NameUtils;

pub struct QueueNames {
    application_len: usize,
    inner: String,
}

const UNACKED_QUEUE_POSTFIX: &str = "_unacked";

impl QueueNames {
    pub fn new(application: &str) -> Self {
        QueueNames {
            application_len: application.len(),
            inner: format!("{}{}", application, UNACKED_QUEUE_POSTFIX),
        }
    }

    pub fn default(&self) -> &str {
        &self.inner[0..self.application_len]
    }

    pub fn unacked(&self) -> &str {
        &self.inner[..]
    }

    pub fn is_unacked(queue_name: &str) -> bool {
        queue_name.ends_with(UNACKED_QUEUE_POSTFIX)
    }
}

impl NameUtils {
    pub fn application(application: &str) -> QueueNames {
        QueueNames::new(application)
    }
}
