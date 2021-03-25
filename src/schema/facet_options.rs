use crate::schema::flags::{IndexedFlag, SchemaFlagList, StoredFlag};
use serde::{Deserialize, Serialize};
use std::ops::BitOr;

/// Define how a facet field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FacetOptions {
    indexed: bool,
    stored: bool,
}

impl FacetOptions {
    /// Returns true iff the value is stored.
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Returns true iff the value is indexed.
    pub fn is_indexed(&self) -> bool {
        self.indexed
    }

    /// Set the field as stored.
    ///
    /// Only the fields that are set as *stored* are
    /// persisted into the Tantivy's store.
    pub fn set_stored(mut self) -> FacetOptions {
        self.stored = true;
        self
    }

    /// Set the field as indexed.
    ///
    /// Setting a facet as indexed will generate
    /// a walkable path.
    pub fn set_indexed(mut self) -> FacetOptions {
        self.indexed = true;
        self
    }
}

impl Default for FacetOptions {
    fn default() -> FacetOptions {
        FacetOptions {
            indexed: false,
            stored: false,
        }
    }
}

impl From<()> for FacetOptions {
    fn from(_: ()) -> FacetOptions {
        FacetOptions::default()
    }
}

impl From<StoredFlag> for FacetOptions {
    fn from(_: StoredFlag) -> Self {
        FacetOptions {
            indexed: false,
            stored: true,
        }
    }
}

impl From<IndexedFlag> for FacetOptions {
    fn from(_: IndexedFlag) -> Self {
        FacetOptions {
            indexed: true,
            stored: false,
        }
    }
}

impl<T: Into<FacetOptions>> BitOr<T> for FacetOptions {
    type Output = FacetOptions;

    fn bitor(self, other: T) -> FacetOptions {
        let other = other.into();
        FacetOptions {
            indexed: self.indexed | other.indexed,
            stored: self.stored | other.stored,
        }
    }
}

impl<Head, Tail> From<SchemaFlagList<Head, Tail>> for FacetOptions
where
    Head: Clone,
    Tail: Clone,
    Self: BitOr<Output = Self> + From<Head> + From<Tail>,
{
    fn from(head_tail: SchemaFlagList<Head, Tail>) -> Self {
        Self::from(head_tail.head) | Self::from(head_tail.tail)
    }
}
