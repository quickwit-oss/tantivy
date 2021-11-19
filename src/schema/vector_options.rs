use serde::{Deserialize, Serialize};

use super::flags::StoredFlag;

/// Define how an a bytes field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VectorOptions {
    dimension: usize,
    indexed: bool,
    stored: bool
}

impl VectorOptions {
    /// Returns the dimension of the vectors of this field.
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Set the dimension of the vectors of this field.
    pub fn set_dimension(mut self, dimension: usize) -> VectorOptions {
        self.dimension = dimension;
        self
    }

    /// Returns true iff the value is indexed.
    pub fn is_indexed(&self) -> bool {
        self.indexed
    }

    /// Returns true iff the value is stored.
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Set the field as stored.
    ///
    /// Only the fields that are set as *stored* are
    /// persisted into the Tantivy's store.
    pub fn set_stored(mut self) -> VectorOptions {
        self.stored = true;
        self
    }

    
}

impl From<()> for VectorOptions {
    fn from(_: ()) -> VectorOptions {
        VectorOptions::default()
    }
}

