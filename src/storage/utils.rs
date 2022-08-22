pub struct QueueNames<'a> {
    application: &'a str,
}

const READY_QUEUE_POSTFIX: &str = "ready";
const UNACK_QUEUE_POSTFIX: &str = "unack";

const DELIMITER: &str = "_";

impl<'a> QueueNames<'a> {
    pub const fn from_application(application: &'a str) -> Self {
        QueueNames { application }
    }

    pub fn data(&self) -> &str {
        self.application
    }

    pub fn ready(&self) -> String {
        format!("{}{}{}", self.application, DELIMITER, READY_QUEUE_POSTFIX)
    }

    pub fn unack(&self) -> String {
        format!("{}{}{}", self.application, DELIMITER, UNACK_QUEUE_POSTFIX)
    }

    pub fn is_ready(queue_name: &str) -> bool {
        queue_name.ends_with(READY_QUEUE_POSTFIX)
    }

    pub fn is_unack(queue_name: &str) -> bool {
        queue_name.ends_with(UNACK_QUEUE_POSTFIX)
    }

    pub fn get_unack_queue_name(unack_queue_name: &str) -> &str {
        let delim_idx = unack_queue_name.find(DELIMITER).unwrap();
        &unack_queue_name[0..delim_idx]
    }
}
