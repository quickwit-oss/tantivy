use std::ops::BitOr;

use serde::{Deserialize, Serialize};

use super::flags::{FastFlag, IndexedFlag, SchemaFlagList, StoredFlag};

/// Define how an ip field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct IpOptions {
    indexed: bool,
    fast: bool,
    stored: bool,
}

impl IpOptions {
    /// Returns true iff the value is a fast field.
    pub fn is_fast(&self) -> bool {
        self.fast
    }

    /// Returns `true` if the json object should be stored.
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Returns `true` iff the json object should be indexed.
    pub fn is_indexed(&self) -> bool {
        self.indexed
    }

    /// Set the field as indexed.
    ///
    /// Setting an integer as indexed will generate
    /// a posting list for each value taken by the integer.
    ///
    /// This is required for the field to be searchable.
    #[must_use]
    pub fn set_indexed(mut self) -> Self {
        self.indexed = true;
        self
    }

    /// Sets the field as stored
    #[must_use]
    pub fn set_stored(mut self) -> Self {
        self.stored = true;
        self
    }

    /// Set the field as a single-valued fast field.
    ///
    /// Fast fields are designed for random access.
    /// Access time are similar to a random lookup in an array.
    /// If more than one value is associated to a fast field, only the last one is
    /// kept.
    #[must_use]
    pub fn set_fast(mut self) -> Self {
        self.fast = true;
        self
    }
}

impl From<()> for IpOptions {
    fn from(_: ()) -> IpOptions {
        IpOptions::default()
    }
}

impl From<FastFlag> for IpOptions {
    fn from(_: FastFlag) -> Self {
        IpOptions {
            indexed: false,
            stored: false,
            fast: true,
        }
    }
}

impl From<StoredFlag> for IpOptions {
    fn from(_: StoredFlag) -> Self {
        IpOptions {
            indexed: false,
            stored: true,
            fast: false,
        }
    }
}

impl From<IndexedFlag> for IpOptions {
    fn from(_: IndexedFlag) -> Self {
        IpOptions {
            indexed: true,
            stored: false,
            fast: false,
        }
    }
}

impl<T: Into<IpOptions>> BitOr<T> for IpOptions {
    type Output = IpOptions;

    fn bitor(self, other: T) -> IpOptions {
        let other = other.into();
        IpOptions {
            indexed: self.indexed | other.indexed,
            stored: self.stored | other.stored,
            fast: self.fast | other.fast,
        }
    }
}

impl<Head, Tail> From<SchemaFlagList<Head, Tail>> for IpOptions
where
    Head: Clone,
    Tail: Clone,
    Self: BitOr<Output = Self> + From<Head> + From<Tail>,
{
    fn from(head_tail: SchemaFlagList<Head, Tail>) -> Self {
        Self::from(head_tail.head) | Self::from(head_tail.tail)
    }
}
