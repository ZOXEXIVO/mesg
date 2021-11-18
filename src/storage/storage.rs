use crate::storage::utils::StorageIdGenerator;
use std::collections::{HashMap, VecDeque};

use crate::metrics::MetricsWriter;
use bytes::Bytes;
use chashmap::CHashMap;
use clap::App;
use log::info;
use prost::alloc::collections::BTreeMap;
use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::sync::{AcquireError, Mutex, RwLock};

pub struct Storage {
    storage: Arc<CHashMap<String, MessageStorage>>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            storage: Arc::new(CHashMap::new()),
        }
    }

    pub async fn push(&self, queue: &str, data: Bytes) -> Result<bool, StorageError> {
        let message_id = StorageIdGenerator::generate();

        let message = Message::new(message_id, data);

        match self.storage.get_mut(queue) {
            Some(mut item) => Ok(item.push(message).await?),
            None => {
                MetricsWriter::inc_queues_count_metric();

                // create queue storage, but not push message to it, because no consumers
                self.storage.insert(queue.into(), MessageStorage::new());

                Ok(false)
            }
        }
    }

    pub async fn pop(
        &self,
        queue: &str,
        application: &str,
        invisibility_timeout: u32,
    ) -> Option<Message> {
        info!("try poll, queue: {}, application: {}", queue, application);

        if let Some(mut storage) = self.storage.get_mut(queue) {
            if let Ok(msg) = storage.pop(application, invisibility_timeout).await {
                return msg;
            }
        }

        None
    }

    pub async fn commit(&self, id: i64, queue: &str, application: &str) -> bool {
        if let Some(mut storage) = self.storage.get_mut(queue) {
            if let Ok(msg) = storage.commit(application, id).await {
                return true;
            }
        }

        false
    }

    pub async fn is_application_queue_exists(&self, queue: &str, application: &str) -> bool {
        if let Some(storage) = self.storage.get(queue) {
            return storage.is_application_exists(application).await;
        }

        false
    }

    pub async fn create_application_queue(&self, queue: &str, application: &str) -> bool {
        if let Some(storage) = self.storage.get_mut(queue) {
            return storage.create_application_queue(application).await;
        }

        false
    }
}

impl Clone for Storage {
    fn clone(&self) -> Self {
        Storage {
            storage: Arc::clone(&self.storage),
        }
    }
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Storage lock error")]
    LockError(#[from] MessageStorageError),
}

// Message storage
pub struct MessageStorage {
    application_queues: Arc<RwLock<HashMap<String, Mutex<VecDeque<Message>>>>>,
    uncommited_store: Arc<RwLock<HashMap<String, Mutex<UncommitedStorage>>>>,
    notify: Sender<(String, i64)>,
}

impl MessageStorage {
    pub fn new() -> Self {
        let (restore_tx, mut restore_rx) = tokio::sync::mpsc::channel(1024);

        let storage = MessageStorage {
            application_queues: Arc::new(RwLock::new(HashMap::new())),
            uncommited_store: Arc::new(RwLock::new(HashMap::new())),
            notify: restore_tx,
        };

        let application_queue = Arc::clone(&storage.application_queues);
        let uncommited_store = Arc::clone(&storage.uncommited_store);

        tokio::spawn(async move {
            while let Some((application, id)) = restore_rx.recv().await {
                let uncommited_store_read_guard = uncommited_store.read().await;

                if let Some(uncommited_store) = uncommited_store_read_guard.get(&application) {
                    let mut uncommited_store_write_guard = uncommited_store.lock().await;

                    if let Some(message) = uncommited_store_write_guard.take(id) {
                        let application_queue_read_guard = application_queue.read().await;

                        if let Some(application_queue) =
                            application_queue_read_guard.get(&application)
                        {
                            let mut application_queue_write_guard = application_queue.lock().await;

                            application_queue_write_guard.push_back(message);
                        }
                    }
                }
            }
        });

        storage
    }

    pub async fn commit(
        &mut self,
        application: &str,
        id: i64,
    ) -> Result<bool, MessageStorageError> {
        let uncommited_store_read_guard = self.uncommited_store.read().await;

        if let Some(uncommited_store) = uncommited_store_read_guard.get(application) {
            let mut uncommited_store_write_guard = uncommited_store.lock().await;

            uncommited_store_write_guard.remove(id);

            return Ok(true);
        }

        Ok(false)
    }

    pub async fn push(&mut self, message: Message) -> Result<bool, MessageStorageError> {
        let guard = self.application_queues.read().await;

        let keys: Vec<String> = guard.keys().map(String::from).collect();

        let mut has_any_push = false;

        for app_queue_key in &keys {
            if let Some(app_queue) = guard.get(app_queue_key) {
                let mut guard = app_queue.lock().await;
                guard.push_back(Message::clone(&message));
                has_any_push = true;
            }
        }

        Ok(has_any_push)
    }

    pub async fn pop(
        &mut self,
        application: &str,
        invisibility_timeout: u32,
    ) -> Result<Option<Message>, MessageStorageError> {
        let guard = self.application_queues.read().await;

        match guard.get(application) {
            Some(application_queue) => {
                info!("application_queue finded: {}", application);

                let mut app_queue_guard = application_queue.lock().await;
                if let Some(message) = app_queue_guard.pop_front() {
                    let uncommited_store_read_guard = self.uncommited_store.read().await;

                    match uncommited_store_read_guard.get(application) {
                        Some(uncommited_store) => {
                            let mut uncommited_store_write_guard = uncommited_store.lock().await;

                            uncommited_store_write_guard.add(&message);

                            // start tokio task to restore item
                            self.start_restore_task(
                                (String::from(application), message.id),
                                invisibility_timeout,
                            );

                            info!(
                                "add to uncommited_store {}, message_id ={}",
                                application, message.id
                            );

                            Ok(Some(message))
                        }
                        None => {
                            drop(uncommited_store_read_guard);

                            info!(
                                "no uncommited_store, adding new {}, message_id ={}",
                                application, message.id
                            );

                            let mut uncommited_store_write_guard =
                                self.uncommited_store.write().await;

                            let mut uncommited_store = UncommitedStorage::new();

                            uncommited_store.add(&message);

                            uncommited_store_write_guard
                                .insert(application.into(), Mutex::new(uncommited_store));

                            // start tokio task to restore item
                            self.start_restore_task(
                                (String::from(application), message.id),
                                invisibility_timeout,
                            );

                            Ok(Some(message))
                        }
                    }
                } else {
                    info!("no message in application queue {}", application);
                    Ok(None)
                }
            }
            None => {
                info!("no application queue {}", application);
                Err(MessageStorageError::NoSubqueue)
            }
        }
    }

    pub fn start_restore_task(&self, data: (String, i64), invisibility_timeout: u32) {
        let send_tx = self.notify.clone();

        info!("started restore task for message_id={}", data.1);

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(invisibility_timeout as u64)).await;
            send_tx.send(data).await
        });
    }

    pub async fn is_application_exists(&self, application: &str) -> bool {
        let guard = self.application_queues.read().await;
        guard.contains_key(application)
    }

    pub async fn create_application_queue(&self, application: &str) -> bool {
        let mut guard = self.application_queues.write().await;

        if !guard.contains_key(application) {
            guard.insert(application.into(), Mutex::new(VecDeque::new()));
            info!("application queeu created={}", application);
        }

        true
    }
}

pub struct UncommitedStorage {
    data: BTreeMap<i64, Message>,
}

impl UncommitedStorage {
    pub fn new() -> Self {
        UncommitedStorage {
            data: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, message: &Message) {
        self.data.insert(message.id, Message::clone(message));
    }

    pub fn take(&mut self, id: i64) -> Option<Message> {
        self.data.remove(&id)
    }

    pub fn remove(&mut self, id: i64) {
        self.data.remove(&id);
    }
}

#[derive(Error, Debug)]
pub enum MessageStorageError {
    #[error("Application queue not exists")]
    NoSubqueue,
    #[error("Storage semaphore error")]
    LockError(#[from] AcquireError),
}

pub struct Message {
    pub id: i64,
    pub data: Bytes,
    pub delivered: bool,
}

impl Ord for Message {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for Message {}

impl PartialOrd<Message> for Message {
    fn partial_cmp(&self, other: &Message) -> Option<Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Message {
    pub fn new(id: i64, data: Bytes) -> Self {
        Message {
            id,
            data: Bytes::clone(&data),
            delivered: false,
        }
    }
}

impl Clone for Message {
    fn clone(&self) -> Self {
        Message {
            id: self.id,
            data: Bytes::clone(&self.data),
            delivered: self.delivered,
        }
    }
}
