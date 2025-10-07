use crate::storage::{MesgInnerStorage, MesgStorageError, Message};
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::{VecDeque, HashSet};
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
}

#[derive(Debug)]
struct ApplicationQueue {
    /// Messages ready for this specific application
    ready_messages: VecDeque<QueueMessage>,
    /// Messages being processed by this application
    unacked_messages: DashMap<String, (QueueMessage, u64)>, // (message, visibility_timeout)
}

impl ApplicationQueue {
    fn new() -> Self {
        ApplicationQueue {
            ready_messages: VecDeque::new(),
            unacked_messages: DashMap::new(),
        }
    }

    fn check_expired_messages(&mut self, now: u64) {
        let expired_ids: Vec<String> = self
            .unacked_messages
            .iter()
            .filter_map(|entry| {
                let (_, visibility_timeout) = entry.value();
                if now >= *visibility_timeout {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect();

        for id in expired_ids {
            if let Some((_, (mut message, _))) = self.unacked_messages.remove(&id) {
                message.delivery_count += 1;
                self.ready_messages.push_back(message);
            }
        }
    }
}

#[derive(Debug)]
struct QueueState {
    /// Main queue for non-broadcast messages (shared across all apps)
    shared_queue: VecDeque<QueueMessage>,
    /// Broadcast messages with tracking of which apps have received them
    broadcast_messages: VecDeque<(QueueMessage, HashSet<String>)>, // (message, delivered_to_apps)
    /// Per-application sub-queues
    app_queues: DashMap<String, ApplicationQueue>,
    /// Message sequence number for ordering
    sequence: AtomicU64,
}

impl QueueState {
    fn new() -> Self {
        QueueState {
            shared_queue: VecDeque::new(),
            broadcast_messages: VecDeque::new(),
            app_queues: DashMap::new(),
            sequence: AtomicU64::new(0),
        }
    }

    fn get_or_create_app_queue(&mut self, application: &str) -> dashmap::mapref::one::RefMut<String, ApplicationQueue> {
        let is_new = !self.app_queues.contains_key(application);
        let mut app_queue = self.app_queues
            .entry(application.to_string())
            .or_insert_with(ApplicationQueue::new);

        // If this is a new application, deliver all pending broadcast messages to it
        if is_new {
            for (broadcast_msg, delivered_to) in &mut self.broadcast_messages {
                if !delivered_to.contains(application) {
                    app_queue.ready_messages.push_back(broadcast_msg.clone());
                    delivered_to.insert(application.to_string());
                }
            }
        }

        app_queue
    }

    fn check_expired_messages(&mut self, now: u64) {
        // Check each application's unacked messages
        for mut app_queue in self.app_queues.iter_mut() {
            app_queue.check_expired_messages(now);
        }
    }

    fn distribute_broadcast_messages(&mut self) {
        // Distribute pending broadcasts to all existing apps
        let mut fully_delivered = Vec::new();

        for (idx, (broadcast_msg, delivered_to)) in self.broadcast_messages.iter_mut().enumerate() {
            let apps_to_deliver: Vec<String> = self.app_queues
                .iter()
                .filter_map(|entry| {
                    let app_name = entry.key();
                    if !delivered_to.contains(app_name.as_str()) {
                        Some(app_name.clone())
                    } else {
                        None
                    }
                })
                .collect();

            for app_name in apps_to_deliver {
                if let Some(mut app_queue) = self.app_queues.get_mut(&app_name) {
                    app_queue.ready_messages.push_back(broadcast_msg.clone());
                    delivered_to.insert(app_name);
                }
            }

            // If all apps have received it, mark for removal
            if !self.app_queues.is_empty() &&
                self.app_queues.iter().all(|entry| delivered_to.contains(entry.key().as_str())) {
                fully_delivered.push(idx);
            }
        }

        // Remove fully distributed broadcasts (in reverse order)
        for idx in fully_delivered.iter().rev() {
            self.broadcast_messages.remove(*idx);
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
        self.queues
            .entry(queue_name.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(QueueState::new())))
            .clone()
    }

    fn start_cleanup_task(&mut self) {
        let queues = self.queues.clone();
        let cleanup_interval = Duration::from_millis(500);

        let handle = tokio::spawn(async move {
            let mut interval = interval(cleanup_interval);

            loop {
                interval.tick().await;

                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;

                for queue_entry in queues.iter() {
                    let queue_state = queue_entry.value();
                    let mut queue = queue_state.write().await;
                    queue.check_expired_messages(now);
                    queue.distribute_broadcast_messages();
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

        let message_id = Uuid::new_v4().to_string();

        let message = QueueMessage {
            id: message_id,
            data: data.clone(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            delivery_count: 0,
        };

        if is_broadcast {
            // Add to broadcast queue with empty delivery tracking
            queue_guard.broadcast_messages.push_back((message, HashSet::new()));
            // Immediately distribute to existing apps
            queue_guard.distribute_broadcast_messages();
        } else {
            // Add to shared queue - will be consumed by first available app/consumer
            queue_guard.shared_queue.push_back(message);
        }

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

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Ensure app queue exists (this will also deliver pending broadcasts)
        let app_queue = queue_guard.get_or_create_app_queue(application);
        drop(app_queue); // Release the RefMut immediately

        // Get mutable reference to app queue
        if let Some(mut app_queue) = queue_guard.app_queues.get_mut(application) {
            // Check for expired messages first
            app_queue.check_expired_messages(now);

            // Try to get from app-specific queue first (broadcast messages)
            if let Some(mut message) = app_queue.ready_messages.pop_front() {
                let visibility_timeout = now + invisibility_timeout_ms as u64;
                message.delivery_count += 1;

                let msg_id = message.id.clone();
                let data = message.data.clone();

                app_queue.unacked_messages.insert(
                    msg_id.clone(),
                    (message, visibility_timeout)
                );

                return Ok(Some(Message {
                    id: msg_id,
                    data,
                    delivered: true,
                }));
            }
        }

        // Try to get from shared queue (non-broadcast messages, load-balanced across all apps)
        if let Some(mut message) = queue_guard.shared_queue.pop_front() {
            let visibility_timeout = now + invisibility_timeout_ms as u64;
            message.delivery_count += 1;

            let msg_id = message.id.clone();
            let data = message.data.clone();

            // Get the app queue again
            if let Some(app_queue) = queue_guard.app_queues.get(application) {
                app_queue.unacked_messages.insert(
                    msg_id.clone(),
                    (message, visibility_timeout)
                );

                return Ok(Some(Message {
                    id: msg_id,
                    data,
                    delivered: true,
                }));
            }
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

        if let Some(app_queue) = queue_guard.app_queues.get(application) {
            if app_queue.unacked_messages.contains_key(&message_id) {
                app_queue.unacked_messages.remove(&message_id);
                return Ok(true);
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
        let queue_guard = queue_state.read().await;

        let message_id = id.to_string();

        if let Some(mut app_queue) = queue_guard.app_queues.get_mut(application) {
            if let Some((_, (message, _))) = app_queue.unacked_messages.remove(&message_id) {
                // Put at front for immediate redelivery
                app_queue.ready_messages.push_front(message);
                return Ok(true);
            }
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