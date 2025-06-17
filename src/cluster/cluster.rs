// use std::collections::HashMap;
// use std::sync::{Arc, Mutex};
// use std::time::Duration;
// use log::{debug, info, warn, error};
// use raft::{
//     eraftpb::{ConfState, Entry, HardState, Message, Snapshot},
//     Config, RawNode, Peer, Storage as RaftStorage, StorageError, Result as RaftResult,
// };
// use tokio::sync::mpsc::{channel, Receiver, Sender};
// use tokio::time::interval;
// use crate::storage::Storage as MesgStorage;
//
// // Storage implementation for Raft that persists state and log entries
// struct RaftMemStorage {
//     hard_state: Mutex<HardState>,
//     entries: Mutex<Vec<Entry>>,
//     conf_state: Mutex<ConfState>,
//     snapshot: Mutex<Snapshot>,
// }
//
// impl RaftMemStorage {
//     fn new() -> Self {
//         RaftMemStorage {
//             hard_state: Mutex::new(HardState::default()),
//             entries: Mutex::new(Vec::new()),
//             conf_state: Mutex::new(ConfState::default()),
//             snapshot: Mutex::new(Snapshot::default()),
//         }
//     }
// }
//
// impl RaftStorage for RaftMemStorage {
//     fn initial_state(&self) -> RaftResult<(HardState, ConfState)> {
//         Ok((
//             self.hard_state.lock().unwrap().clone(),
//             self.conf_state.lock().unwrap().clone(),
//         ))
//     }
//
//     fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> RaftResult<Vec<Entry>> {
//         let entries = self.entries.lock().unwrap();
//         if entries.is_empty() {
//             return Ok(Vec::new());
//         }
//
//         let first_idx = entries[0].index;
//         if low < first_idx {
//             return Err(StorageError::Compacted);
//         }
//
//         let low_offset = (low - first_idx) as usize;
//         let high_offset = if high > first_idx + entries.len() as u64 {
//             entries.len()
//         } else {
//             (high - first_idx) as usize
//         };
//
//         if low_offset >= high_offset {
//             return Ok(Vec::new());
//         }
//
//         Ok(entries[low_offset..high_offset].to_vec())
//     }
//
//     fn term(&self, idx: u64) -> RaftResult<u64> {
//         let entries = self.entries.lock().unwrap();
//         if entries.is_empty() {
//             return Err(StorageError::Unavailable);
//         }
//
//         let first_idx = entries[0].index;
//         if idx < first_idx {
//             return Err(StorageError::Compacted);
//         }
//
//         if idx - first_idx >= entries.len() as u64 {
//             return Err(StorageError::Unavailable);
//         }
//
//         Ok(entries[(idx - first_idx) as usize].term)
//     }
//
//     fn first_index(&self) -> RaftResult<u64> {
//         let entries = self.entries.lock().unwrap();
//         if entries.is_empty() {
//             return Ok(1);
//         }
//         Ok(entries[0].index)
//     }
//
//     fn last_index(&self) -> RaftResult<u64> {
//         let entries = self.entries.lock().unwrap();
//         if entries.is_empty() {
//             return Ok(0);
//         }
//         Ok(entries[0].index + entries.len() as u64 - 1)
//     }
//
//     fn snapshot(&self, _request_index: u64) -> RaftResult<Snapshot> {
//         Ok(self.snapshot.lock().unwrap().clone())
//     }
// }
//
// // Represents a node in the Raft cluster
// struct ClusterNode {
//     id: u64,
//     address: String,
// }
//
// // Message type for Raft communication
// enum RaftMessage {
//     Raft(Message),
//     Shutdown,
// }
//
// pub struct Cluster {
//     // Node ID of this cluster member
//     id: u64,
//     // Other nodes in the cluster
//     nodes: HashMap<u64, ClusterNode>,
//     // The Raft node instance
//     raft_node: Option<Arc<Mutex<RawNode<RaftMemStorage>>>>,
//     // Communication channel
//     message_sender: Option<Sender<(u64, RaftMessage)>>,
//     // Application storage
//     mesg_storage: Option<Arc<MesgStorage>>,
// }
//
// impl Cluster {
//     pub fn new() -> Self {
//         Cluster {
//             id: 1, // Default ID
//             nodes: HashMap::new(),
//             raft_node: None,
//             message_sender: None,
//             mesg_storage: None,
//         }
//     }
//
//     // Configure the node ID
//     pub fn with_id(mut self, id: u64) -> Self {
//         self.id = id;
//         self
//     }
//
//     // Set the message broker storage
//     pub fn with_storage(mut self, storage: Arc<MesgStorage>) -> Self {
//         self.mesg_storage = Some(storage);
//         self
//     }
//
//     // Add another node to the cluster
//     pub fn add_node(&mut self, id: u64, address: String) {
//         self.nodes.insert(id, ClusterNode { id, address });
//     }
//
//     // Initialize and start the Raft cluster
//     pub fn start(&mut self) {
//         // Create Raft configuration
//         let config = Config {
//             id: self.id,
//             election_tick: 10,
//             heartbeat_tick: 3,
//             max_size_per_msg: 1024 * 1024, // 1MB
//             max_inflight_msgs: 256,
//             ..Default::default()
//         };
//
//         // Create storage for Raft
//         let storage = RaftMemStorage::new();
//
//         // Set up peer list
//         let mut peers = Vec::new();
//         for &id in self.nodes.keys() {
//             peers.push(Peer { id });
//         }
//         // Add self if not already in peers
//         if !peers.iter().any(|p| p.id == self.id) {
//             peers.push(Peer { id: self.id });
//         }
//
//         // Create and validate the Raft node
//         match RawNode::new(&config, storage, peers) {
//             Ok(node) => {
//                 let raft_node = Arc::new(Mutex::new(node));
//                 self.raft_node = Some(raft_node.clone());
//
//                 // Set up message passing
//                 let (tx, rx) = channel(1000);
//                 self.message_sender = Some(tx);
//
//                 // Start message handler
//                 self.start_message_handler(rx, raft_node.clone());
//
//                 // Start the tick loop
//                 self.start_tick_loop(raft_node);
//
//                 info!("Raft node started with ID: {}", self.id);
//             },
//             Err(e) => {
//                 error!("Failed to create Raft node: {}", e);
//             }
//         }
//     }
//
//     // Handle incoming messages from other nodes
//     fn start_message_handler(
//         &self,
//         mut rx: Receiver<(u64, RaftMessage)>,
//         raft_node: Arc<Mutex<RawNode<RaftMemStorage>>>
//     ) {
//         let storage = self.mesg_storage.clone();
//
//         tokio::spawn(async move {
//             while let Some((from, msg)) = rx.recv().await {
//                 match msg {
//                     RaftMessage::Raft(raft_msg) => {
//                         debug!("Received Raft message from node {}", from);
//
//                         // Process the message through Raft
//                         let mut node = raft_node.lock().unwrap();
//                         if let Err(e) = node.step(raft_msg) {
//                             error!("Error processing Raft message: {}", e);
//                         }
//                     },
//                     RaftMessage::Shutdown => {
//                         info!("Shutting down Raft message handler");
//                         break;
//                     }
//                 }
//             }
//         });
//     }
//
//     // Start the tick loop for Raft timing
//     fn start_tick_loop(&self, raft_node: Arc<Mutex<RawNode<RaftMemStorage>>>) {
//         let storage = self.mesg_storage.clone();
//
//         tokio::spawn(async move {
//             let mut tick_interval = interval(Duration::from_millis(100));
//
//             loop {
//                 tick_interval.tick().await;
//
//                 // Tick the Raft node
//                 {
//                     let mut node = raft_node.lock().unwrap();
//                     node.tick();
//                 }
//
//                 // Process any ready state
//                 {
//                     let mut node = raft_node.lock().unwrap();
//                     if node.has_ready() {
//                         let mut ready = node.ready();
//
//                         // Handle persistence of hard state and entries
//                         if !ready.entries().is_empty() {
//                             debug!("New entries: {}", ready.entries().len());
//                             // In a production system, would persist entries here
//                         }
//
//                         // Send messages to other nodes
//                         if !ready.messages().is_empty() {
//                             debug!("Messages to send: {}", ready.messages().len());
//                             // In a production system, would send messages here
//                         }
//
//                         // Apply committed entries to state machine
//                         if !ready.committed_entries().is_empty() {
//                             debug!("Applying {} committed entries", ready.committed_entries().len());
//
//                             for entry in ready.committed_entries().clone() {
//                                 if !entry.data.is_empty() {
//                                     // In a production system, would apply command to state machine here
//                                     debug!("Applying entry {}: {} bytes", entry.index, entry.data.len());
//                                 }
//                             }
//                         }
//
//                         // Advance the Raft node
//                         node.advance(ready);
//                     }
//                 }
//             }
//         });
//     }
//
//     // Propose a command to the Raft cluster
//     pub async fn propose(&self, data: Vec<u8>) -> Result<(), String> {
//         if let Some(raft_node) = &self.raft_node {
//             let mut node = raft_node.lock().unwrap();
//             node.propose(vec![], data).map_err(|e| e.to_string())
//         } else {
//             Err("Raft node not initialized".to_string())
//         }
//     }
//
//     // Process an incoming message from another node
//     pub async fn process_message(&self, from: u64, msg: Message) -> Result<(), String> {
//         if let Some(sender) = &self.message_sender {
//             sender.send((from, RaftMessage::Raft(msg))).await
//                 .map_err(|e| format!("Failed to send message: {}", e))
//         } else {
//             Err("Message sender not initialized".to_string())
//         }
//     }
// }