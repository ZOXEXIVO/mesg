use crate::storage::IdPair;
use sled::{IVec, Tree};

pub struct DebugUtils;

impl DebugUtils {
    #[allow(dead_code)]
    pub fn print_keys_tree(tree: &Tree, title: &str) {
        Self::print_generic(title, tree.iter().keys());
    }

    #[allow(dead_code)]
    pub fn print_values_tree(tree: &Tree, title: &str) {
        Self::print_generic(title, tree.iter().values());
    }

    #[allow(dead_code)]
    pub fn render_values(items: &[u64]) -> String {
        let ids: Vec<String> = items.iter().map(|ri| ri.to_string()).collect();
        ids.join(",")
    }

    #[allow(dead_code)]
    pub fn render_pair_values(items: &[IdPair]) -> String {
        let vec: Vec<u64> = items.iter().map(|i| i.value()).collect();
        Self::render_values(&vec)
    }

    #[inline]
    fn print_generic(title: &str, items: impl Iterator<Item = sled::Result<IVec>>) {
        let state: Vec<String> = items
            .map(|v| {
                let val = v.unwrap();
                IdPair::from_vector(val).value().to_string()
            })
            .collect();

        println!("tree state: {}: [{}]", title, state.join(","));
    }
}
