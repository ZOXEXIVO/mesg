use crate::storage::identity::Identity;
use crate::storage::message::Message;
use crate::storage::{NameUtils, QueueCollection};
use bytes::Bytes;
use log::{error, info};
use sled::{Db, IVec, Subscriber};
use std::collections::HashMap;
use std::sync::Arc;
use structopt::clap::App;
use thiserror::Error;
use tokio::sync::RwLock;

type DbInternal = (Db, QueueCollection);

pub struct Storage {
    store: Arc<RwLock<HashMap<String, DbInternal>>>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            store: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn subscribe(&self, queue: &str, application: &str) -> Option<Subscriber> {
        let result = self
            .execute_in_context(queue, move |db, queues| {
                let queue_names = NameUtils::application(application);

                queues.execute(db, queue_names.default(), |tree| {
                    Some(tree.watch_prefix(vec![]))
                })
            })
            .await;

        match result {
            Ok(res) => res,
            Err(_) => None,
        }
    }

    pub async fn push(&self, queue: &str, data: Bytes) -> Result<bool, StorageError> {
        let result = self
            .execute_in_context(queue, move |db, queues| {
                let (identity_val, identity_vec) = Identity::get(db, queue);
                let message = Message::new(identity_val, Bytes::clone(&data));

                let mut pushed = false;

                for tree_name in queues.tree_names() {
                    queues.execute(db, &tree_name, |app_queue| {
                        app_queue
                            .insert(&identity_vec, message.clone().data.to_vec())
                            .unwrap();
                        //
                        // db.flush();
                        //
                        // let available_items = app_queue
                        //     .iter()
                        //     .keys()
                        //     .map(|k| {
                        //         u64::from_be_bytes(k.unwrap().to_vec().try_into().unwrap())
                        //             .to_string()
                        //     })
                        //     .fold(String::new(), |a, b| a + ", " + &b);
                        //
                        // info!(
                        //     "message pushed, message_id={}, available_items={}",
                        //     message.id, available_items
                        // );
                    });

                    pushed = true;
                }

                pushed
            })
            .await;

        match result {
            Ok(res) => Ok(res),
            Err(msg) => Err(StorageError::Generic(msg)),
        }
    }

    pub async fn pop(&self, queue: &str, application: &str) -> Option<Message> {
        let result = self
            .execute_in_context(queue, move |db, queues| {
                let queue_names = NameUtils::application(application);

                return queues.execute(db, queue_names.default(), |original_queue| {
                    if let Ok(Some((k, v))) = original_queue.pop_min() {
                        queues.execute(db, queue_names.unacked(), |unacked_queue| {
                            unacked_queue.insert(k.clone(), v.clone()).unwrap();
                        });

                        let key_bytes: Vec<u8> = k.to_vec();
                        let val_bytes: Vec<u8> = v.to_vec();

                        let id = u64::from_be_bytes(key_bytes.try_into().unwrap());

                        return Some(Message::new(id, Bytes::from(val_bytes)));
                    }

                    None
                });
            })
            .await;

        match result {
            Ok(res) => res,
            Err(_) => None,
        }
    }

    pub async fn ack(&self, id: u64, queue: &str, application: &str, success: bool) -> bool {
        match success {
            true => self.ack_inner(id, queue, application).await,
            false => self.unack_inner(id, queue, application).await,
        }
    }

    pub async fn ack_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        let result = self
            .execute_in_context(queue, move |db, queues| {
                let queue_names = NameUtils::application(application);

                return queues.execute(db, queue_names.unacked(), |unacked_queue| {
                    let id_vec = IVec::from(id.to_be_bytes().to_vec());

                    if let Ok(removed) = unacked_queue.remove(id_vec) {
                        if removed.is_some() {
                            info!(
                            "removed form unacked queue, message_id={}, queue={}, application={}",
                            id, queue, application
                        );
                            return true;
                        }
                    }

                    false
                });
            })
            .await;

        result.unwrap_or(false)
    }

    pub async fn unack_inner(&self, id: u64, queue: &str, application: &str) -> bool {
        let result = self
            .execute_in_context(queue, move |db, queues| {
                let queue_names = NameUtils::application(application);

                return queues.execute(db, queue_names.unacked(), |unacked_queue| {
                    let id_vec = IVec::from(id.to_be_bytes().to_vec());

                    //TODO REMOVE CLONE
                    if let Ok(Some(removed_message)) = unacked_queue.remove(id_vec.clone()) {
                        queues.execute(db, queue_names.default(), move |original_queue| {
                            original_queue
                                .insert(id_vec.clone(), removed_message.clone())
                                .unwrap();
                        });

                        info!(
                            "returned to original_queue, message_id={}, queue={}, application={}",
                            id, queue, application
                        );
                    }

                    false
                });
            })
            .await;

        result.unwrap_or(false)
    }

    async fn execute_in_context<F: Fn(&Db, &QueueCollection) -> R, R>(
        &self,
        queue: &str,
        action: F,
    ) -> Result<R, String> {
        let read_lock = self.store.read().await;

        if let Some((db, trees)) = read_lock.get(queue) {
            let result = action(db, trees);

            flush(db).await;

            return Ok(result);
        } else {
            drop(read_lock);

            let mut write_lock = self.store.write().await;

            if let Ok(db) = sled::open(format!("{queue}.mesg")) {
                let trees = QueueCollection::new();

                action(&db, &trees);

                flush(&db).await;

                write_lock.insert(String::from(queue), (db, trees));
            } else {
                drop(write_lock);

                let read_lock = self.store.read().await;

                info!(
                    "execute_in_context: another read_lock getted, queue={}",
                    queue
                );

                if let Some((db, trees)) = read_lock.get(queue) {
                    let result = action(db, trees);

                    flush(db).await;

                    return Ok(result);
                }
            }
        }

        return Err(format!("Cannot create queue={}", queue));

        async fn flush(db: &Db) {
            if let Err(error) = db.flush_async().await {
                error!("flush error: {}", error)
            }
        }
    }

    pub async fn create_application_queue(&self, queue: &str, application: &str) -> bool {
        let result = self
            .execute_in_context(queue, move |db, queues| {
                let queue_names = NameUtils::application(application);

                queues.execute(db, queue_names.default(), move |_| {});
                queues.execute(db, queue_names.unacked(), move |_| {});

                info!(
                    "application queue created, queue={}, appplication={}",
                    queue, application
                );

                true
            })
            .await;

        result.unwrap_or(false)
    }
}

impl Clone for Storage {
    fn clone(&self) -> Self {
        Storage {
            store: Arc::clone(&self.store),
        }
    }
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Storage generic error")]
    Generic(String),
}
