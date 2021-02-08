// use std::fs::{File, OpenOptions};
// use std::sync::{Mutex, RwLock, Arc};
// use std::io::{Seek, SeekFrom, Read, Write};
// use std::collections::BTreeMap;
// use std::mem::{transmute};
// 
// pub struct FileWrapper {
//     data_file: Arc<Mutex<File>>,
//     index_file: Arc<RwLock<BTreeMap<String, u64>>>
// }
// 
// impl FileWrapper {
//     pub fn new(queue_name: String) -> Self {
//         let (data_file_name, index_file_name) = Self::get_filenames(&queue_name);
//         
//         let data_file = OpenOptions::new()
//             .write(true)
//             .create(true)
//             .open(&data_file_name)
//             .unwrap_or_else(|_| panic!("Storage: error with file: {}", &data_file_name));
// 
//         // let index_file = OpenOptions::new()
//         //     .write(true)
//         //     .create(true)
//         //     .open(&index_file_name)
//         //     .unwrap_or_else(|_| panic!("Storage: error with file: {}", &index_file_name));
//         //
//         FileWrapper{
//             data_file: Arc::new(Mutex::new(data_file)),
//             index_file: Arc::new(RwLock::new(BTreeMap::new()))
//         }
//     }
//     
//     pub async fn write(&mut self, id: u64, data: &[u8]) -> Result<(), ()> {
//         Ok(())
//     }
//     
//     pub async fn read(&self, queue_name: String) -> Option<(String, Vec<u8>)>{
//         None
//     }
//     
//     pub async fn commit(&mut self, id: u64) {
//         // let index_store = self.index_file.read().unwrap();
//         // 
//         // let mut file = self.data_file.lock().unwrap();
//         // 
//         // match index_store.get(&id) {
//         //     Some(idx) => {
//         //         file.seek(SeekFrom::Start(*idx));
//         //     },
//         //     None => {}
//         // }
// 
//         // let header: DataHeader = unsafe {
//         //     let mut buffer = [0u8, size_of::<DataHeader>() as u8];
//         //    
//         //     file.read_exact(&mut buffer).unwrap();
//         //    
//         //     file.write(&[1]);
//         //     file.flush();
//         //    
//         //     unsafe {
//         //         transmute(buffer)
//         //     }
//         // };
//     }
//     
//     fn get_filenames(queue_name: &str) -> (String, String) {
//         let lowercased_name = queue_name.to_lowercase();
//         
//         (lowercased_name.clone() + ".data", lowercased_name + ".idx")
//     }
// }
// 
// pub struct DataHeader{
//     id: u64,
//     len: u32
// }