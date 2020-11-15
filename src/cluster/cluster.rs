pub struct Cluster {
}

impl Cluster {
    pub fn new() -> Self {
        Cluster { }
    }

    pub fn start(&self) {
        // let config = Config {
        //     id: 1,
        //     peers: vec![1],
        //     ..Default::default()
        // };
        //
        // let storage = MemStorage::default();
        //
        // config.validate().unwrap();
        //
        // let mut node = RawNode::new(&config, storage, vec![]).unwrap();
        //
        // node.raft.become_candidate();
        // node.raft.become_leader();
    }
}
