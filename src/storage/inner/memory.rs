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
    // For tracking which apps have seen this broadcast message
    delivered_to_apps: HashSet<String>,
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
    /// Main queue for non-broadcast messages (shared pool)
    shared_queue: VecDeque<QueueMessage>,
    /// Broadcast messages that need to be delivered to all apps
    broadcast_queue: VecDeque<QueueMessage>,
    /// Per-application sub-queues
    app_queues: DashMap<String, ApplicationQueue>,
    /// Message sequence number for ordering
    sequence: AtomicU64,
}

impl QueueState {
    fn new() -> Self {
        QueueState {
            shared_queue: VecDeque::new(),
            broadcast_queue: VecDeque::new(),
            app_queues: DashMap::new(),
            sequence: AtomicU64::new(0),
        }
    }

    fn get_or_create_app_queue(&self, application: &str) -> dashmap::mapref::one::RefMut<String, ApplicationQueue> {
        self.app_queues
            .entry(application.to_string())
            .or_insert_with(ApplicationQueue::new)
    }

    fn check_expired_messages(&mut self, now: u64) {
        // Check each application's unacked messages
        for mut app_queue in self.app_queues.iter_mut() {
            app_queue.check_expired_messages(now);
        }
    }

    fn distribute_broadcast_messages(&mut self) {
        // Copy broadcast messages to each application queue that hasn't received them
        let mut processed_broadcasts = Vec::new();

        for broadcast_msg in &mut self.broadcast_queue {
            let mut all_apps_received = true;

            // Collect app names that need this message
            let apps_needing_message: Vec<String> = self.app_queues
                .iter()
                .filter_map(|entry| {
                    if !broadcast_msg.delivered_to_apps.contains(entry.key()) {
                        Some(entry.key().clone())
                    } else {
                        None
                    }
                })
                .collect();

            // Distribute to each app that needs it
            for app_name in apps_needing_message {
                // Clone message for this app
                let mut app_msg = broadcast_msg.clone();
                app_msg.delivered_to_apps.clear(); // Reset for the app's copy

                // Get mutable reference to app queue
                if let Some(mut app_queue) = self.app_queues.get_mut(&app_name) {
                    app_queue.ready_messages.push_back(app_msg);
                    broadcast_msg.delivered_to_apps.insert(app_name);
                    all_apps_received = false;
                }
            }

            // If no apps are registered yet, keep the broadcast message
            if self.app_queues.is_empty() {
                all_apps_received = false;
            }

            if all_apps_received && !self.app_queues.is_empty() {
                processed_broadcasts.push(broadcast_msg.id.clone());
            }
        }

        // Remove fully distributed broadcasts
        self.broadcast_queue.retain(|msg| !processed_broadcasts.contains(&msg.id));
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
        let cleanup_interval = Duration::from_secs(5);

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

        let message_id = Uuid::new_v4().to_string(); // Use just UUID

        let message = QueueMessage {
            id: message_id,
            data: data.clone(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            delivery_count: 0,
            delivered_to_apps: HashSet::new(),
        };

        if is_broadcast {
            // Add to broadcast queue - will be distributed to all apps
            queue_guard.broadcast_queue.push_back(message);
            // Immediately distribute to existing apps
            queue_guard.distribute_broadcast_messages();
        } else {
            // Add to shared queue - will be consumed by one app
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

        // Ensure app queue exists
        let mut app_queue = queue_guard.get_or_create_app_queue(application);

        // Check for expired messages first
        app_queue.check_expired_messages(now);

        // Try to get from app-specific queue first
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

        // Drop the app_queue RefMut before trying to modify queue_guard
        drop(app_queue);

        // Distribute any pending broadcast messages
        queue_guard.distribute_broadcast_messages();

        // Try to get from shared queue
        if let Some(mut message) = queue_guard.shared_queue.pop_front() {
            let visibility_timeout = now + invisibility_timeout_ms as u64;
            message.delivery_count += 1;

            let msg_id = message.id.clone();
            let data = message.data.clone();

            // Re-acquire the app queue
            let mut app_queue = queue_guard.get_or_create_app_queue(application);
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

        // Check if any broadcast messages are now available after distribution
        let mut app_queue = queue_guard.get_or_create_app_queue(application);
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
            // Check if the message exists in unacked messages
            if app_queue.unacked_messages.contains_key(&message_id) {
                // Remove it (this commits the message)
                drop(app_queue); // Release the read lock on app_queue

                // Re-acquire with write access for the specific app queue
                let queue_guard = queue_state.read().await;
                if let Some(app_queue) = queue_guard.app_queues.get(application) {
                    app_queue.unacked_messages.remove(&message_id);
                    return Ok(true);
                };
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
                // Put at back to maintain fairness (not front)
                app_queue.ready_messages.push_back(message);
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