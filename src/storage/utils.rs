pub struct QueueNames<'a> {
    queue: &'a str,
    application: &'a str,
}

const READY_QUEUE_POSTFIX: &str = "ready";
const UNACKED_QUEUE_POSTFIX: &str = "unacked";

impl<'a> QueueNames<'a> {
    pub const fn from(queue: &'a str, application: &'a str) -> Self {
        QueueNames { queue, application }
    }

    pub fn data(&self) -> &str {
        self.queue
    }

    pub fn ready(&self) -> String {
        format!("{}_{}", self.queue, READY_QUEUE_POSTFIX)
    }

    pub fn unacked(&self) -> String {
        format!("{}_{}", self.queue, UNACKED_QUEUE_POSTFIX)
    }

    pub fn is_ready(queue_name: &str) -> bool {
        queue_name.ends_with(READY_QUEUE_POSTFIX)
    }
}
