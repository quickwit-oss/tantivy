use std::ops::BitOr;

use serde::{Deserialize, Serialize};

use super::flags::{FastFlag, IndexedFlag, SchemaFlagList, StoredFlag};
use super::Cardinality;

/// Define how an ip field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct IpOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    fast: Option<Cardinality>,
    stored: bool,
}

impl IpOptions {
    /// Returns true iff the value is a fast field.
    pub fn is_fast(&self) -> bool {
        self.fast.is_some()
    }

    /// Returns `true` if the json object should be stored.
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Returns the cardinality of the fastfield.
    ///
    /// If the field has not been declared as a fastfield, then
    /// the method returns None.
    pub fn get_fastfield_cardinality(&self) -> Option<Cardinality> {
        self.fast
    }

    /// Sets the field as stored
    #[must_use]
    pub fn set_stored(mut self) -> Self {
        self.stored = true;
        self
    }

    /// Set the field as a fast field.
    ///
    /// Fast fields are designed for random access.
    /// Access time are similar to a random lookup in an array.
    /// If more than one value is associated with a fast field, only the last one is
    /// kept.
    #[must_use]
    pub fn set_fast(mut self, cardinality: Cardinality) -> Self {
        self.fast = Some(cardinality);
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
            stored: false,
            fast: Some(Cardinality::SingleValue),
        }
    }
}

impl From<StoredFlag> for IpOptions {
    fn from(_: StoredFlag) -> Self {
        IpOptions {
            stored: true,
            fast: None,
        }
    }
}

impl From<IndexedFlag> for IpOptions {
    fn from(_: IndexedFlag) -> Self {
        IpOptions {
            stored: false,
            fast: None,
        }
    }
}

impl<T: Into<IpOptions>> BitOr<T> for IpOptions {
    type Output = IpOptions;

    fn bitor(self, other: T) -> IpOptions {
        let other = other.into();
        IpOptions {
            stored: self.stored | other.stored,
            fast: self.fast.or(other.fast),
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
