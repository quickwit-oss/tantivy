// src/schema/nested_options.rs
use crate::schema::TextFieldIndexing;
use serde::{Deserialize, Serialize};

/// Options for a "nested" field in Tantivy.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NestedOptions {
    /// If true, nested child fields also appear in the parent doc.
    pub include_in_parent: bool,
    /// If true, nested child fields also appear in the root doc (if multiple levels).
    pub include_in_root: bool,
    /// If true, we store a hidden parent flag for each doc.
    /// Some users may prefer to do parent-detection in a different way.
    pub store_parent_flag: bool,
}

impl NestedOptions {
    pub fn new() -> Self {
        println!("Creating new NestedOptions with defaults:");
        println!("  include_in_parent: false");
        println!("  include_in_root: false"); 
        println!("  store_parent_flag: true");
        NestedOptions {
            include_in_parent: false,
            include_in_root: false,
            store_parent_flag: true, // default to true
        }
    }

    pub fn set_include_in_parent(mut self, val: bool) -> Self {
        println!("Setting include_in_parent: {} -> {}", self.include_in_parent, val);
        self.include_in_parent = val;
        self
    }
    pub fn set_include_in_root(mut self, val: bool) -> Self {
        println!("Setting include_in_root: {} -> {}", self.include_in_root, val);
        self.include_in_root = val;
        self
    }
    pub fn set_store_parent_flag(mut self, val: bool) -> Self {
        println!("Setting store_parent_flag: {} -> {}", self.store_parent_flag, val);
        self.store_parent_flag = val;
        self
    }

    pub fn is_indexed(&self) -> bool {
        // By default, we do not index the nested field itself
        // because we rely on expansions. If you want to index it,
        // you'd change this logic.
        println!("Checking is_indexed() -> false");
        false
    }

    pub fn is_stored(&self) -> bool {
        println!("Checking is_stored() -> false");
        false
    }

    pub fn is_fast(&self) -> bool {
        println!("Checking is_fast() -> false");
        false
    }

    pub fn fieldnorms(&self) -> bool {
        println!("Checking fieldnorms() -> false");
        false
    }

    pub fn get_text_indexing_options(&self) -> Option<&TextFieldIndexing> {
        println!("Getting text_indexing_options() -> None");
        None
    }
}
