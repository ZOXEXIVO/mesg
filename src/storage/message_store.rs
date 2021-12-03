﻿use std::collections::{HashMap, VecDeque};

use crate::storage::message::Message;
use log::info;
use prost::alloc::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::sync::{AcquireError, Mutex, RwLock};

// Message storage
pub struct MessageStore {
    application_queues: Arc<RwLock<HashMap<String, Mutex<VecDeque<Message>>>>>,
    uncommited_store: Arc<RwLock<HashMap<String, Mutex<UncommitedStorage>>>>,
    notify: Sender<(String, i64)>,
}

impl MessageStore {
    pub fn new() -> Self {
        let (restore_tx, mut restore_rx) = tokio::sync::mpsc::channel(1024);

        let storage = MessageStore {
            application_queues: Arc::new(RwLock::new(HashMap::new())),
            uncommited_store: Arc::new(RwLock::new(HashMap::new())),
            notify: restore_tx,
        };

        //let application_queue = Arc::clone(&storage.application_queues);
        //let uncommited_store = Arc::clone(&storage.uncommited_store);

        // tokio::spawn(async move {
        //     while let Some((application, id)) = restore_rx.recv().await {
        //         info!(
        //             "received message restore event: id={}, application={}",
        //             id, application
        //         );
        //
        //         info!("begin restore.uncommited_store.read");
        //         let uncommited_store_read_guard = uncommited_store.read().await;
        //         info!("end restore.uncommited_store.read");
        //
        //         if let Some(uncommited_store) = uncommited_store_read_guard.get(&application) {
        //             info!("begin restore.uncommited_store.lock");
        //             let mut uncommited_store_write_guard = uncommited_store.lock().await;
        //             info!("end restore.uncommited_store.lock");
        //
        //             if let Some(message) = uncommited_store_write_guard.take(id) {
        //                 info!("begin restore.application_queue.read");
        //                 let application_queue_read_guard = application_queue.write().await;
        //                 info!("end restore.application_queue.read");
        //
        //                 if let Some(application_queue) =
        //                     application_queue_read_guard.get(&application)
        //                 {
        //                     info!("begin restore.application_queue.lock");
        //                     let mut application_queue_write_guard = application_queue.lock().await;
        //                     info!("end restore.application_queue.lock");
        //
        //                     application_queue_write_guard.push_back(message);
        //                 }
        //             }
        //         }
        //     }
        // });

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

            info!("message commited: id={}", id);

            return Ok(true);
        }

        Ok(false)
    }

    pub async fn uncommit(
        &mut self,
        application: &str,
        id: i64,
    ) -> Result<bool, MessageStorageError> {
        let uncommited_store_read_guard = self.uncommited_store.read().await;
        if let Some(uncommited_store) = uncommited_store_read_guard.get(application) {
            info!("begin uncommited_store.lock");
            let mut uncommited_store_write_guard = uncommited_store.lock().await;
            info!("end uncommited_store.lock");

            if let Some(message) = uncommited_store_write_guard.take(id) {
                info!("begin application_queues.read");
                let application_queue_read_guard = self.application_queues.read().await;
                info!("end application_queues.read");

                if let Some(application_queue) = application_queue_read_guard.get(application) {
                    info!("begin application_queue.lock");
                    let mut application_queue_write_guard = application_queue.lock().await;
                    info!("end application_queue.lock");

                    let id = message.id;

                    application_queue_write_guard.push_back(message);

                    info!("message uncommited: {}", id);

                    return Ok(true);
                }
            }
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
                let mut app_queue_guard = application_queue.lock().await;
                if let Some(message) = app_queue_guard.pop_front() {
                    let uncommited_store_read_guard = self.uncommited_store.read().await;

                    info!("end pop.uncommited_store.read");

                    match uncommited_store_read_guard.get(application) {
                        Some(uncommited_store) => {
                            info!("begin pop.uncommited_store.lock");
                            let mut uncommited_store_write_guard = uncommited_store.lock().await;
                            info!("end pop.uncommited_store.lock");

                            uncommited_store_write_guard.add(&message);

                            // start tokio task to restore item
                            self.start_restore_task(
                                (String::from(application), message.id),
                                invisibility_timeout,
                            );

                            info!(
                                "add to uncommited_store id={}, application={}",
                                message.id, application
                            );

                            Ok(Some(message))
                        }
                        None => {
                            drop(uncommited_store_read_guard);

                            info!(
                                "no uncommited_store, adding new id={}, application={}",
                                message.id, application
                            );

                            info!("begin uncommited_store.write");
                            let mut uncommited_store_write_guard =
                                self.uncommited_store.write().await;
                            info!("end uncommited_store.write");

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
                    Ok(None)
                }
            }
            None => {
                info!("no application queue application={}", application);
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
            info!("application queue created={}", application);
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
