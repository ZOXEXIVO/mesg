pub struct QueueNames<'a> {
    queue: &'a str,
    application: &'a str,
}

const READY_QUEUE_POSTFIX: &str = "ready";
const UNACK_QUEUE_POSTFIX: &str = "unack";

impl<'a> QueueNames<'a> {
    pub const fn from(queue: &'a str, application: &'a str) -> Self {
        QueueNames { queue, application }
    }

    pub fn data(&self) -> &str {
        self.queue
    }

    pub fn ready(&self) -> String {
        format!(
            "{}_{}_{}",
            self.queue, self.application, READY_QUEUE_POSTFIX
        )
    }

    pub fn unack(&self) -> String {
        format!(
            "{}_{}_{}",
            self.queue, self.application, UNACK_QUEUE_POSTFIX
        )
    }

    pub fn is_ready(queue_name: &str) -> bool {
        queue_name.ends_with(READY_QUEUE_POSTFIX)
    }
}
