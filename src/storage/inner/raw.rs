use crate::storage::{MesgInnerStorage, MesgStorageError, Message};
use bincode::{Decode, Encode};
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval};
use uuid::Uuid;

#[derive(Debug, Clone, Encode, Decode)]
struct QueueMessage {
    id: String,
    data: Vec<u8>,
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
    /// Persistent log file for this queue
    log_file: Arc<Mutex<File>>,
    /// Current offset in the log file
    log_offset: AtomicU64,
    /// Message sequence number for ordering
    sequence: AtomicU64,
}

impl QueueState {
    async fn new(queue_path: &Path) -> Result<Self, MesgStorageError> {
        // Ensure directory exists
        if let Some(parent) = queue_path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| MesgStorageError {
                message: format!("Failed to create queue directory: {}", e),
            })?;
        }

        // Open or create the log file
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(queue_path)
            .await
            .map_err(|e| MesgStorageError {
                message: format!("Failed to open queue log file: {}", e),
            })?;

        let log_offset = log_file
            .metadata()
            .await
            .map_err(|e| MesgStorageError {
                message: format!("Failed to get file metadata: {}", e),
            })?
            .len();

        Ok(QueueState {
            ready_queue: VecDeque::new(),
            unacked_messages: DashMap::new(),
            log_file: Arc::new(Mutex::new(log_file)),
            log_offset: AtomicU64::new(log_offset),
            sequence: AtomicU64::new(0),
        })
    }

    async fn recover_from_log(&mut self) -> Result<(), MesgStorageError> {
        let mut file = self.log_file.lock().await;
        file.seek(SeekFrom::Start(0)).await.map_err(|e| MesgStorageError {
            message: format!("Failed to seek to start of log: {}", e),
        })?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await.map_err(|e| MesgStorageError {
            message: format!("Failed to read log file: {}", e),
        })?;

        let mut offset = 0;
        let mut max_sequence = 0u64;

        while offset < buffer.len() {
            if offset + 8 > buffer.len() {
                break; // Not enough bytes for length prefix
            }

            // Read message length (8 bytes, big endian)
            let length = u64::from_be_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
                buffer[offset + 4],
                buffer[offset + 5],
                buffer[offset + 6],
                buffer[offset + 7],
            ]);

            offset += 8;

            if offset + length as usize > buffer.len() {
                break; // Incomplete message
            }

            // Deserialize message
            let message_data = &buffer[offset..offset + length as usize];
            let config = bincode::config::standard();
            match bincode::decode_from_slice::<QueueMessage, _>(message_data, config) {
                Ok((message, _)) => {
                    max_sequence = max_sequence.max(
                        message.id.split('-').last()
                            .and_then(|s| s.parse::<u64>().ok())
                            .unwrap_or(0)
                    );

                    // Check if message is still within visibility timeout
                    if let Some(visibility_timeout) = message.visibility_timeout {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64;

                        if now < visibility_timeout {
                            // Still invisible, add to unacked
                            self.unacked_messages.insert(message.id.clone(), message);
                        } else {
                            // Visibility timeout expired, add back to ready queue
                            self.ready_queue.push_back(message);
                        }
                    } else {
                        // No visibility timeout, add to ready queue
                        self.ready_queue.push_back(message);
                    }
                }
                Err(_) => {
                    // Skip corrupted message
                    continue;
                }
            }

            offset += length as usize;
        }

        self.sequence.store(max_sequence + 1, Ordering::SeqCst);
        Ok(())
    }

    async fn append_to_log(&self, message: &QueueMessage) -> Result<(), MesgStorageError> {
        let config = bincode::config::standard();
        let serialized = bincode::encode_to_vec(message, config).map_err(|e| MesgStorageError {
            message: format!("Failed to serialize message: {}", e),
        })?;

        let mut file = self.log_file.lock().await;

        // Write length prefix (8 bytes, big endian)
        let length = serialized.len() as u64;
        file.write_all(&length.to_be_bytes()).await.map_err(|e| MesgStorageError {
            message: format!("Failed to write length prefix: {}", e),
        })?;

        // Write message data
        file.write_all(&serialized).await.map_err(|e| MesgStorageError {
            message: format!("Failed to write message data: {}", e),
        })?;

        file.flush().await.map_err(|e| MesgStorageError {
            message: format!("Failed to flush log file: {}", e),
        })?;

        self.log_offset.fetch_add(8 + length, Ordering::SeqCst);
        Ok(())
    }
}

pub struct RawFileStorage {
    base_path: PathBuf,
    queues: DashMap<String, Arc<RwLock<QueueState>>>,
    cleanup_task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl RawFileStorage {
    async fn get_or_create_queue(&self, queue_name: &str) -> Result<Arc<RwLock<QueueState>>, MesgStorageError> {
        if let Some(queue) = self.queues.get(queue_name) {
            return Ok(Arc::clone(&queue));
        }

        // Create new queue
        let queue_path = self.base_path.join(format!("{}.log", queue_name));
        let mut queue_state = QueueState::new(&queue_path).await?;
        queue_state.recover_from_log().await?;

        let queue_arc = Arc::new(RwLock::new(queue_state));
        self.queues.insert(queue_name.to_string(), Arc::clone(&queue_arc));

        Ok(queue_arc)
    }

    fn start_cleanup_task(&self) {
        let queues = Arc::new(self.queues.clone());
        let cleanup_interval = Duration::from_secs(30); // Check every 30 seconds

        let handle = tokio::spawn(async move {
            let mut interval = interval(cleanup_interval);

            loop {
                interval.tick().await;

                for queue_entry in queues.iter() {
                    let queue_state = queue_entry.value();
                    let mut queue = queue_state.write().await;

                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;

                    // Check for expired unacked messages
                    let expired_ids: Vec<String> = queue
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
                        if let Some((_, mut message)) = queue.unacked_messages.remove(&id) {
                            message.visibility_timeout = None;
                            message.consumer_application = None;
                            message.delivery_count += 1;

                            // Log the redelivery
                            if let Err(e) = queue.append_to_log(&message).await {
                                eprintln!("Failed to log redelivery: {}", e.message);
                            }

                            queue.ready_queue.push_back(message);
                        }
                    }
                }
            }
        });

        // Note: In a real implementation, you'd want to store this handle
        // and properly clean it up when the storage is dropped
        std::mem::forget(handle);
    }
}

impl MesgInnerStorage for RawFileStorage {
    async fn create<P: AsRef<Path>>(path: P) -> Self {
        let base_path = path.as_ref().to_path_buf();

        // Ensure base directory exists
        if !base_path.exists() {
            fs::create_dir_all(&base_path).await.unwrap();
        }

        let storage = RawFileStorage {
            base_path,
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
        let queue_state = self.get_or_create_queue(queue).await?;
        let mut queue = queue_state.write().await;

        let sequence = queue.sequence.fetch_add(1, Ordering::SeqCst);
        let message_id = format!("{}-{}", Uuid::new_v4(), sequence);

        let message = QueueMessage {
            id: message_id,
            data: data.to_vec(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            delivery_count: 0,
            consumer_application: None,
            visibility_timeout: None,
        };

        // Append to persistent log
        queue.append_to_log(&message).await?;

        if is_broadcast {
            // For broadcast messages, we might want to handle differently
            // For now, treat them the same as regular messages
            queue.ready_queue.push_back(message);
        } else {
            queue.ready_queue.push_back(message);
        }

        Ok(true)
    }

    async fn pop(
        &self,
        queue: &str,
        application: &str,
        invisibility_timeout_ms: i32,
    ) -> Result<Option<Message>, MesgStorageError> {
        let queue_state = self.get_or_create_queue(queue).await?;
        let mut queue = queue_state.write().await;

        if let Some(mut message) = queue.ready_queue.pop_front() {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            // Set visibility timeout
            message.visibility_timeout = Some(now + invisibility_timeout_ms as u64);
            message.consumer_application = Some(application.to_string());
            message.delivery_count += 1;

            // Log the delivery
            queue.append_to_log(&message).await?;

            // Move to unacked messages
            let message_id = message.id.clone();
            let data = Bytes::from(message.data.clone());
            queue.unacked_messages.insert(message_id.clone(), message);

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
        success: bool,
    ) -> Result<bool, MesgStorageError> {
        let queue_state = self.get_or_create_queue(queue).await?;
        let mut queue = queue_state.write().await;

        let message_id = id.to_string();

        if success {
            // Acknowledge the message - remove from unacked
            if queue.unacked_messages.remove(&message_id).is_some() {
                // Log the acknowledgment
                let ack_message = QueueMessage {
                    id: format!("ACK-{}", message_id),
                    data: vec![], // Empty data for ACK record
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    delivery_count: 0,
                    consumer_application: Some(application.to_string()),
                    visibility_timeout: None,
                };
                queue.append_to_log(&ack_message).await?;
                Ok(true)
            } else {
                Ok(false) // Message not found in unacked
            }
        } else {
            // Negative acknowledgment - move back to ready queue
            if let Some((_, mut message)) = queue.unacked_messages.remove(&message_id) {
                message.visibility_timeout = None;
                message.consumer_application = None;

                // Log the negative acknowledgment
                queue.append_to_log(&message).await?;

                queue.ready_queue.push_back(message);
                Ok(true)
            } else {
                Ok(false) // Message not found in unacked
            }
        }
    }

    async fn revert(
        &self,
        id: Uuid,
        queue: &str,
        application: &str,
    ) -> Result<bool, MesgStorageError> {
        // Revert is similar to negative acknowledgment
        self.commit(id, queue, application, false).await
    }
}

impl Drop for RawFileStorage {
    fn drop(&mut self) {
        // In a real implementation, you'd want to properly shut down
        // the cleanup task here
        if let Some(handle) = self.cleanup_task_handle.take() {
            handle.abort();
        }
    }
}