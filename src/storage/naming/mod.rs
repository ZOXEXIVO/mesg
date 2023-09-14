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

    // generators

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

    pub fn data_usage(queue: &str) -> String {
        format!("{}{}{}", queue, DELIMITER, USAGE_DATA_QUEUE_POSTFIX)
    }

    #[inline]
    pub fn base(&self) -> String {
        format!("{}{}{}", self.queue, DELIMITER, self.application)
    }

    pub fn identity(queue: &str) -> String {
        format!("{}{}{}", queue, DELIMITER, IDENTITY_POSTFIX)
    }

    // checkers

    #[inline]
    pub fn is_ready(queue_name: &str, base_queue_name: &str) -> bool {
        queue_name.starts_with(base_queue_name) && queue_name.ends_with(READY_QUEUE_POSTFIX)
    }

    #[inline]
    pub fn is_unack(queue_name: &str) -> bool {
        queue_name.ends_with(UNACK_QUEUE_POSTFIX)
    }

    #[inline]
    pub fn is_unack_order(queue_name: &str) -> bool {
        queue_name.ends_with(UNACK_ORDER_QUEUE_POSTFIX)
    }

    pub fn parse_queue_application(unack_queue_name: &str) -> (&str, &str) {
        let mut split_iterator = unack_queue_name.split(DELIMITER);

        (
            split_iterator.next().unwrap(),
            split_iterator.next().unwrap(),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::QueueNames;

    #[test]
    fn queue_names_is_correct() {
        let queue_names = QueueNames::new("queue1", "application1");

        assert_eq!("queue1_application1".to_owned(), queue_names.base());
        assert_eq!("queue1_application1_ready".to_owned(), queue_names.ready());
        assert_eq!("queue1_application1_unack".to_owned(), queue_names.unack());
        assert_eq!(
            "queue1_application1_unack_order".to_owned(),
            queue_names.unack_order()
        );
        assert_eq!(
            "queue1_data_usage".to_owned(),
            QueueNames::data_usage("queue1")
        );

        assert_eq!("queue1_identity".to_owned(), QueueNames::identity("queue1"));
    }

    #[test]
    fn is_ready_is_correct() {
        assert!(QueueNames::is_ready("queue1_application1_ready", "queue1"));
        assert!(QueueNames::is_ready("queue2_application2_ready", "queue2"));
        assert!(QueueNames::is_ready("queue3_application3_ready", "queue3"));

        // check for non ready queues
        assert!(!QueueNames::is_ready("queue1_application1", "queue1"));
        assert!(!QueueNames::is_ready("queue2_application2", "queue2"));
        assert!(!QueueNames::is_ready("queue3_application3", "queue3"));

        assert!(!QueueNames::is_ready("queue4_application1_ready", "queue1"));
        assert!(!QueueNames::is_ready("queue5_application2_ready", "queue1"));
        assert!(!QueueNames::is_ready("queue6_application3_ready", "queue1"));
    }

    #[test]
    fn is_unack_is_correct() {
        assert!(QueueNames::is_unack("queue1_application1_unack"));
        assert!(!QueueNames::is_unack("queue1_application1"));
    }

    #[test]
    fn is_unack_order_is_correct() {
        assert!(QueueNames::is_unack_order(
            "queue1_application1_unack_order"
        ));
        assert!(!QueueNames::is_unack_order("queue1_application1"));
    }
}
