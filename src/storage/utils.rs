pub struct NameUtils;

pub struct QueueNames<'a> {
    application: &'a str,
}

const READY_QUEUE_POSTFIX: &str = "ready";
const UNACKED_QUEUE_POSTFIX: &str = "unacked";

impl<'a> QueueNames<'a> {
    pub const fn new(application: &'a str) -> Self {
        QueueNames { application }
    }

    pub fn data(&self) -> &str {
        self.application
    }

    pub fn ready(&self) -> String {
        format!("{}_{}", self.application, READY_QUEUE_POSTFIX)
    }

    pub fn unacked(&self) -> String {
        format!("{}_{}", self.application, UNACKED_QUEUE_POSTFIX)
    }

    pub fn is_ready(queue_name: &str) -> bool {
        queue_name.ends_with(READY_QUEUE_POSTFIX)
    }
}

impl NameUtils {
    pub fn from_application(application: &str) -> QueueNames {
        QueueNames::new(application)
    }

    pub fn from_queue(queue: &str) -> QueueNames {
        QueueNames::new(queue)
    }
}
