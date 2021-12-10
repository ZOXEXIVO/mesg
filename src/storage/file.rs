use std::io::SeekFrom;
use tokio::fs::{File, OpenOptions};
use tokio::io::{self, AsyncRead, AsyncReadExt};
use tokio::sync::{Mutex, MutexGuard};

pub struct RingBufferFileHeader {
    write_index: u64,
    read_index: u32,
}

pub struct RingBufferFile {
    file: File,
}

impl RingBufferFile {
    pub async fn new(filename: String) -> Self {
        let filename = format!("{}.data", filename);

        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&filename)
            .await
            .unwrap_or_else(|_| panic!("Storage: error with file: {}", &filename));

        RingBufferFile { file: file }
    }

    pub async fn write(&self, data: &[u8]) {
        //let file = self.file.lock().await;

        //self.file.wri
    }

    async fn read_header(mut file: File) -> RingBufferFileHeader {
        let mut buffer = String::new();

        file.read(&mut buffer).await;

        RingBufferFileHeader {
            write_index: 0,
            read_index: 0,
        }
    }
}
