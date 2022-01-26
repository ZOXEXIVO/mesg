use std::io::SeekFrom;
use std::mem::{size_of, transmute};
use tokio::fs::{File, OpenOptions};
use tokio::io::{self, AsyncRead, AsyncReadExt};
use tokio::sync::{Mutex, MutexGuard};

pub struct RingBufferFileHeader {
    write_index: u64,
    read_index: u32,
}

pub struct RingBufferFile {
    file: Mutex<File>,
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

        RingBufferFile {
            file: Mutex::new(file),
        }
    }

    pub async fn write(&self, data: &[u8]) {
        let file = self.file.lock().await;

        // let header = read_header
        //
        // self.file.wri
    }

    async fn read_header(mut file: File) -> RingBufferFileHeader {
        let mut buffer = [0u8; size_of::<RingBufferFileHeader>()];

        file.read_exact(&mut buffer).await;

        unsafe { transmute(buffer) }
    }
}
