﻿use crate::storage::IdPair;
use sled::{IVec, Tree};

pub struct DebugUtils;

impl DebugUtils {
    pub fn print_keys_tree(tree: &Tree, title: &str) {
        Self::print_generic(title, tree.iter().keys());
    }

    pub fn print_values_tree(tree: &Tree, title: &str) {
        Self::print_generic(title, tree.iter().values());
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
