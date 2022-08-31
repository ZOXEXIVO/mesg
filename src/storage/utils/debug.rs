use crate::storage::IdPair;
use sled::Tree;

pub struct DebugUtils;

impl DebugUtils {
    pub fn print_tree(tree: &Tree, name: &str) {
        let queue_states: Vec<u64> = tree
            .iter()
            .values()
            .map(|v| {
                let val = v.unwrap();
                IdPair::from_vector(val).value()
            })
            .collect();

        let mut str = String::new();

        for queue_state in queue_states {
            str += &queue_state.to_string();
            str += ", "
        }

        println!("tree state: {}: [{}]", name, str);
    }
}
