use crate::storage::{MesgInnerStorage, MesgStorageError, Message};
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::interval;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct QueueMessage {
    id: String,
    data: Bytes,
    timestamp: u64,
    delivery_count: u32,
    consumer_application: Option<String>,
    visibility_timeout: Option<u64>,
}

#[derive(Debug)]
struct QueueState {
    /// Ready messages waiting to be consumed
    ready_queue: VecDeque<QueueMessage>,
    /// Messages currently being processed (unacknowledged)
    unacked_messages: DashMap<String, QueueMessage>,
    /// Message sequence number for ordering
    sequence: AtomicU64,
}

impl QueueState {
    fn new() -> Self {
        QueueState {
            ready_queue: VecDeque::new(),
            unacked_messages: DashMap::new(),
            sequence: AtomicU64::new(0),
        }
    }

    fn check_expired_messages(&mut self, now: u64) {
        // Collect expired message IDs
        let expired_ids: Vec<String> = self
            .unacked_messages
            .iter()
            .filter_map(|entry| {
                let message = entry.value();
                if let Some(visibility_timeout) = message.visibility_timeout {
                    if now >= visibility_timeout {
                        Some(entry.key().clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        // Move expired messages back to ready queue
        for id in expired_ids {
            if let Some((_, mut message)) = self.unacked_messages.remove(&id) {
                message.visibility_timeout = None;
                message.consumer_application = None;
                message.delivery_count += 1;
                self.ready_queue.push_back(message);
            }
        }
    }
}

pub struct InMemoryStorage {
    /// All queues in memory
    queues: DashMap<String, Arc<RwLock<QueueState>>>,
    /// Cleanup task handle
    cleanup_task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl InMemoryStorage {
    async fn get_or_create_queue(&self, queue_name: &str) -> Arc<RwLock<QueueState>> {
        if let Some(queue) = self.queues.get(queue_name) {
            return Arc::clone(&queue);
        }

        // Create new queue
        let queue_state = QueueState::new();
        let queue_arc = Arc::new(RwLock::new(queue_state));
        self.queues
            .insert(queue_name.to_string(), Arc::clone(&queue_arc));

        queue_arc
    }

    fn start_cleanup_task(&mut self) {
        let queues = self.queues.clone();
        let cleanup_interval = Duration::from_secs(5); // Check every 5 seconds

        let handle = tokio::spawn(async move {
            let mut interval = interval(cleanup_interval);

            loop {
                interval.tick().await;

                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;

                // Check all queues for expired messages
                for queue_entry in queues.iter() {
                    let queue_state = queue_entry.value();
                    let mut queue = queue_state.write().await;
                    queue.check_expired_messages(now);
                }
            }
        });

        self.cleanup_task_handle = Some(handle);
    }
}

impl MesgInnerStorage for InMemoryStorage {
    async fn create<P: AsRef<Path>>(_path: P) -> Self {
        let mut storage = InMemoryStorage {
            queues: DashMap::new(),
            cleanup_task_handle: None,
        };

        storage.start_cleanup_task();
        storage
    }

    async fn push(
        &self,
        queue: &str,
        data: Bytes,
        is_broadcast: bool,
    ) -> Result<bool, MesgStorageError> {
        let queue_state = self.get_or_create_queue(queue).await;
        let mut queue_guard = queue_state.write().await;

        let sequence = queue_guard.sequence.fetch_add(1, Ordering::SeqCst);
        let message_id = format!("{}-{}", Uuid::new_v4(), sequence);

        let message = QueueMessage {
            id: message_id,
            data: data.clone(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            delivery_count: 0,
            consumer_application: None,
            visibility_timeout: None,
        };

        queue_guard.ready_queue.push_back(message);

        Ok(true)
    }

    async fn pop(
        &self,
        queue: &str,
        application: &str,
        invisibility_timeout_ms: i32,
    ) -> Result<Option<Message>, MesgStorageError> {
        let queue_state = self.get_or_create_queue(queue).await;
        let mut queue_guard = queue_state.write().await;

        // First, check for expired messages and restore them
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        queue_guard.check_expired_messages(now);

        // Try to pop a message from the ready queue
        if let Some(mut message) = queue_guard.ready_queue.pop_front() {
            // Set visibility timeout
            message.visibility_timeout = Some(now + invisibility_timeout_ms as u64);
            message.consumer_application = Some(application.to_string());
            message.delivery_count += 1;

            // Move to unacked messages
            let message_id = message.id.clone();
            let data = message.data.clone();
            queue_guard
                .unacked_messages
                .insert(message_id.clone(), message);

            return Ok(Some(Message {
                id: message_id,
                data,
                delivered: true,
            }));
        }

        Ok(None)
    }

    async fn commit(
        &self,
        id: Uuid,
        queue: &str,
        application: &str,
    ) -> Result<bool, MesgStorageError> {
        let queue_state = self.get_or_create_queue(queue).await;
        let queue_guard = queue_state.read().await;

        let message_id = id.to_string();

        // Check if message exists and belongs to this application
        if let Some(message_entry) = queue_guard.unacked_messages.get(&message_id) {
            let message = message_entry.value();

            // Verify the application matches
            if let Some(ref consumer_app) = message.consumer_application {
                if consumer_app == application {
                    // Remove from unacked (this commits the message)
                    drop(message_entry);
                    queue_guard.unacked_messages.remove(&message_id);
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    async fn rollback(
        &self,
        id: Uuid,
        queue: &str,
        application: &str,
    ) -> Result<bool, MesgStorageError> {
        let queue_state = self.get_or_create_queue(queue).await;
        let mut queue_guard = queue_state.write().await;

        let message_id = id.to_string();

        // Try to remove from unacked messages
        if let Some((_, message)) = queue_guard.unacked_messages.remove(&message_id) {
            // Verify the application matches
            if let Some(ref consumer_app) = message.consumer_application {
                if consumer_app != application {
                    // Put it back if application doesn't match
                    queue_guard
                        .unacked_messages
                        .insert(message_id.clone(), message);
                    return Ok(false);
                }
            }

            // Reset message state and add back to ready queue
            let mut rolled_back_message = message;
            rolled_back_message.visibility_timeout = None;
            rolled_back_message.consumer_application = None;
            // Keep delivery_count to track redeliveries

            queue_guard.ready_queue.push_front(rolled_back_message);
            return Ok(true);
        }

        Ok(false)
    }
}

impl Drop for InMemoryStorage {
    fn drop(&mut self) {
        if let Some(handle) = self.cleanup_task_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_push_pop() {
        let storage = InMemoryStorage::create("").await;
        let queue = "test_queue";
        let app = "test_app";

        // Push a message
        let data = Bytes::from_static(b"test message");
        let result = storage.push(queue, data.clone(), false).await;
        assert!(result.is_ok());
        assert!(result.unwrap());

        // Pop the message
        let msg = storage.pop(queue, app, 5000).await.unwrap();
        assert!(msg.is_some());
        let msg = msg.unwrap();
        assert_eq!(data, msg.data);
    }

    #[tokio::test]
    async fn test_in_memory_commit() {
        let storage = InMemoryStorage::create("").await;
        let queue = "test_queue";
        let app = "test_app";

        // Push and pop
        storage
            .push(queue, Bytes::from_static(b"test"), false)
            .await
            .unwrap();
        let msg = storage.pop(queue, app, 5000).await.unwrap().unwrap();

        // Commit
        let id = Uuid::parse_str(&msg.id).unwrap();
        let result = storage.commit(id, queue, app).await.unwrap();
        assert!(result);

        // Should not be available anymore
        let msg2 = storage.pop(queue, app, 1000).await.unwrap();
        assert!(msg2.is_none());
    }

    #[tokio::test]
    async fn test_in_memory_rollback() {
        let storage = InMemoryStorage::create("").await;
        let queue = "test_queue";
        let app = "test_app";

        // Push and pop
        let data = Bytes::from_static(b"rollback test");
        storage.push(queue, data.clone(), false).await.unwrap();
        let msg = storage.pop(queue, app, 5000).await.unwrap().unwrap();

        // Rollback
        let id = Uuid::parse_str(&msg.id).unwrap();
        let result = storage.rollback(id, queue, app).await.unwrap();
        assert!(result);

        // Should be available again
        let msg2 = storage.pop(queue, app, 5000).await.unwrap();
        assert!(msg2.is_some());
        assert_eq!(data, msg2.unwrap().data);
    }

    #[tokio::test]
    async fn test_in_memory_visibility_timeout() {
        let storage = InMemoryStorage::create("").await;
        let queue = "test_queue";
        let app = "test_app";

        // Push a message
        storage
            .push(queue, Bytes::from_static(b"timeout test"), false)
            .await
            .unwrap();

        // Pop with short timeout
        let msg = storage.pop(queue, app, 100).await.unwrap().unwrap();

        // Should not be immediately available
        let msg2 = storage.pop(queue, app, 100).await.unwrap();
        assert!(msg2.is_none());

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should be available again
        let msg3 = storage.pop(queue, app, 5000).await.unwrap();
        assert!(msg3.is_some());
    }

    #[tokio::test]
    async fn test_in_memory_fifo_order() {
        let storage = InMemoryStorage::create("").await;
        let queue = "test_queue";
        let app = "test_app";

        // Push multiple messages
        for i in 0..5 {
            storage
                .push(queue, Bytes::from(vec![i]), false)
                .await
                .unwrap();
        }

        // Pop and verify order
        for i in 0..5 {
            let msg = storage.pop(queue, app, 5000).await.unwrap().unwrap();
            assert_eq!(vec![i], msg.data.to_vec());
            let id = Uuid::parse_str(&msg.id).unwrap();
            storage.commit(id, queue, app).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_in_memory_wrong_application_commit() {
        let storage = InMemoryStorage::create("").await;
        let queue = "test_queue";

        // Push and pop with app1
        storage
            .push(queue, Bytes::from_static(b"test"), false)
            .await
            .unwrap();
        let msg = storage.pop(queue, "app1", 5000).await.unwrap().unwrap();

        // Try to commit with app2
        let id = Uuid::parse_str(&msg.id).unwrap();
        let result = storage.commit(id, queue, "app2").await.unwrap();
        assert!(!result, "Commit with wrong application should fail");

        // Commit with correct app should work
        let result2 = storage.commit(id, queue, "app1").await.unwrap();
        assert!(result2);
    }

    #[tokio::test]
    async fn test_in_memory_empty_queue() {
        let storage = InMemoryStorage::create("").await;
        let msg = storage.pop("empty_queue", "app", 1000).await.unwrap();
        assert!(msg.is_none());
    }

    #[tokio::test]
    async fn test_in_memory_multiple_queues() {
        let storage = InMemoryStorage::create("").await;

        // Push to different queues
        storage
            .push("queue1", Bytes::from_static(b"msg1"), false)
            .await
            .unwrap();
        storage
            .push("queue2", Bytes::from_static(b"msg2"), false)
            .await
            .unwrap();

        // Pop from each queue
        let msg1 = storage.pop("queue1", "app", 5000).await.unwrap().unwrap();
        let msg2 = storage.pop("queue2", "app", 5000).await.unwrap().unwrap();

        assert_eq!(b"msg1", msg1.data.as_ref());
        assert_eq!(b"msg2", msg2.data.as_ref());
    }
}