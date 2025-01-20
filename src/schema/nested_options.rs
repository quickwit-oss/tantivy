use serde::{Deserialize, Serialize};

use super::TextFieldIndexing;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NestedOptions {
    /// If true, fields in the nested doc also appear in the parent doc.
    pub include_in_parent: bool,
    /// If true, fields also appear in the root doc (if multiple levels).
    pub include_in_root: bool,
}

impl NestedOptions {
    pub fn new() -> Self {
        NestedOptions {
            include_in_parent: false,
            include_in_root: false,
        }
    }

    pub fn set_include_in_parent(mut self, val: bool) -> Self {
        self.include_in_parent = val;
        self
    }
    pub fn set_include_in_root(mut self, val: bool) -> Self {
        self.include_in_root = val;
        self
    }

    #[inline]
    pub fn is_stored(&self) -> bool {
        false
    }

    #[inline]
    pub fn is_fast(&self) -> bool {
        false
    }

    #[inline]
    pub fn is_indexed(&self) -> bool {
        false
    }

    #[inline]
    pub fn fieldnorms(&self) -> bool {
        false
    }

    #[inline]
    pub fn should_coerce(&self) -> bool {
        false
    }

    #[inline]
    pub fn get_text_indexing_options(&self) -> Option<&TextFieldIndexing> {
        None
    }
}
