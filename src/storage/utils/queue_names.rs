pub struct QueueNames<'a> {
    queue: &'a str,
    application: &'a str,
}

const READY_QUEUE_POSTFIX: &str = "ready";
const UNACK_QUEUE_POSTFIX: &str = "unack";
const UNACK_ORDER_QUEUE_POSTFIX: &str = "unack_order";
const USAGE_DATA_QUEUE_POSTFIX: &str = "data_usage";
const IDENTITY_POSTFIX: &str = "identity";

const DELIMITER: &str = "_";

impl<'a> QueueNames<'a> {
    pub const fn new(queue: &'a str, application: &'a str) -> Self {
        QueueNames { queue, application }
    }

    #[inline]
    pub fn ready(&self) -> String {
        format!("{}{}{}", self.base(), DELIMITER, READY_QUEUE_POSTFIX)
    }

    #[inline]
    pub fn unack(&self) -> String {
        format!("{}{}{}", self.base(), DELIMITER, UNACK_QUEUE_POSTFIX)
    }

    #[inline]
    pub fn unack_order(&self) -> String {
        format!("{}{}{}", self.base(), DELIMITER, UNACK_ORDER_QUEUE_POSTFIX)
    }

    #[inline]
    fn base(&self) -> String {
        format!("{}{}{}", self.queue, DELIMITER, self.application)
    }

    pub fn identity(queue: &str) -> String {
        format!("{}{}{}", queue, DELIMITER, IDENTITY_POSTFIX)
    }

    #[inline]
    pub fn is_ready(db_queue_name: &str, queue: &str) -> bool {
        db_queue_name.starts_with(queue) && db_queue_name.ends_with(READY_QUEUE_POSTFIX)
    }

    #[inline]
    pub fn is_unack(queue_name: &str) -> bool {
        queue_name.ends_with(UNACK_QUEUE_POSTFIX)
    }

    pub fn parse_queue_application(unack_queue_name: &str) -> (&str, &str) {
        let mut split_iterator = unack_queue_name.split(DELIMITER);

        (
            split_iterator.next().unwrap(),
            split_iterator.next().unwrap(),
        )
    }

    pub fn data_usage(queue: &str) -> String {
        format!("{}{}{}", queue, DELIMITER, USAGE_DATA_QUEUE_POSTFIX)
    }
}
