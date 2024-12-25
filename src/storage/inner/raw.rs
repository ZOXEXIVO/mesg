use crate::storage::{MesgInnerStorage, MesgStorageError, Message};
use bytes::Bytes;
use std::path::Path;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid7::Uuid;

pub struct RawFileStorage {
    path: String,
}

impl MesgInnerStorage for RawFileStorage {
    async fn create<P: AsRef<Path>>(path: P) -> Self {
        fs::create_dir_all(&path).await.unwrap();
        RawFileStorage {
            path: path.as_ref().to_string_lossy().into_owned(),
        }
    }

    async fn push(
        &self,
        queue: &str,
        data: Bytes,
        is_broadcast: bool,
    ) -> Result<bool, MesgStorageError> {
        let id = uuid7::uuid7();

        let file_path = format!("{}/{}/{}", self.path, queue, id);
        let mut file = File::create(&file_path).await.unwrap();
        file.write_all(&data).await.unwrap();

        Ok(true)
    }

    async fn pop(
        &self,
        queue: &str,
        application: &str,
        invisibility_timeout_ms: i32,
    ) -> Result<Option<Message>, MesgStorageError> {
        let queue_path = format!("{}/{}", self.path, queue);
        let mut entries = fs::read_dir(&queue_path).await.unwrap();

        if let Some(entry) = entries.next_entry().await.unwrap() {
            let id = entry
                .file_name()
                .into_string()
                .unwrap()
                .parse::<Uuid>()
                .unwrap();

            let mut file = File::open(entry.path()).await.unwrap();
            let mut data = Vec::new();
            file.read_to_end(&mut data).await.unwrap();

            let new_path = format!("{}/processing/{}-{}", self.path, application, id);
            fs::rename(entry.path(), &new_path).await.unwrap();

            return Ok(Some(Message {
                id: id.to_string(),
                data: data.into(),
                delivered: false,
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
        let processing_path = format!("{}/processing/{}-{}", self.path, application, id);
        if success {
            fs::remove_file(&processing_path).await.unwrap();
        } else {
            let new_path = format!("{}/{}/{}", self.path, queue, id);
            fs::rename(&processing_path, &new_path).await.unwrap();
        }

        Ok(true)
    }

    async fn revert(
        &self,
        id: Uuid,
        queue: &str,
        application: &str,
    ) -> Result<bool, MesgStorageError> {
        let processing_path = format!("{}/processing/{}-{}", self.path, application, id);
        let new_path = format!("{}/{}/{}", self.path, queue, id);
        fs::rename(&processing_path, &new_path).await.unwrap();
        Ok(true)
    }
}
