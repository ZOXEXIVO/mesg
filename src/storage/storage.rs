use std::path::{Path, PathBuf};

use crate::metrics::MetricsWriter;
use crate::storage::{FileWrapper};
use std::sync::{Arc};
use crate::storage::utils::StorageIdGenerator;
use dashmap::DashMap;

pub struct Storage {
    metrics_writer: MetricsWriter,
 
    id_generator: StorageIdGenerator,
    
    files: Arc<DashMap<String, FileWrapper>>
}

impl Storage {
    pub fn new<P: AsRef<Path>>(db_path: P, metrics_writer: MetricsWriter) -> Self {
        let mut file_path = PathBuf::with_capacity(2);
        
        file_path.push(db_path);
        file_path.push("data.mesg");

        Storage {
            id_generator: StorageIdGenerator::new(),
            metrics_writer,
            files: Arc::new(DashMap::new())
        }
    }

    pub async fn push(&self, queue_name: String, data: &[u8]) {
        match self.files.get_mut(&queue_name) {
            Some(mut item) => {
                item.write(self.id_generator.generate(), data).await.unwrap();
            },
            None => {
                let mut wrapper = FileWrapper::new(queue_name.clone());

                wrapper.write(self.id_generator.generate(), data).await.unwrap();

                self.files.insert(queue_name.clone(),wrapper).unwrap();
            }
        };
    }

    pub async fn pull(&self, queue_name: String) -> StorageReader {     
        StorageReader {
            
        }
    }
    
    async fn add_to_wait_list(&mut self, queue_name: String){
        let reader = self.pull(String::from("dadwq")).await;
 
    }

    pub async fn commit(&self, queue_name: String, message_id: String) {

    }
}

pub struct StorageItem{
    pub id: String,
    pub data: Vec<u8>
}

pub struct StorageReader {
    
}
