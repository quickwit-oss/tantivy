use std::ops::BitOr;

/// Define how an int field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntOptions {
    indexed: bool,
    fast: bool,
    stored: bool,
}

impl IntOptions {
    /// Returns true iff the value is stored.
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Returns true iff the value is indexed.
    pub fn is_indexed(&self) -> bool {
        self.indexed
    }

    /// Returns true iff the value is a fast field.
    pub fn is_fast(&self) -> bool {
        self.fast
    }

    /// Set the u64 options as stored.
    ///
    /// Only the fields that are set as *stored* are
    /// persisted into the Tantivy's store.
    pub fn set_stored(mut self) -> IntOptions {
        self.stored = true;
        self
    }

    /// Set the u64 options as indexed.
    ///
    /// Setting an integer as indexed will generate
    /// a posting list for each value taken by the integer.
    pub fn set_indexed(mut self) -> IntOptions {
        self.indexed = true;
        self
    }

    /// Set the u64 options as a fast field.
    ///
    /// Fast fields are designed for random access.
    /// Access time are similar to a random lookup in an array.
    /// If more than one value is associated to a fast field, only the last one is
    /// kept.
    pub fn set_fast(mut self) -> IntOptions {
        self.fast = true;
        self
    }
}

impl Default for IntOptions {
    fn default() -> IntOptions {
        IntOptions {
            fast: false,
            indexed: false,
            stored: false,
        }
    }
}

/// Shortcut for a u64 fast field.
///
/// Such a shortcut can be composed as follows `STORED | FAST | INT_INDEXED`
pub const FAST: IntOptions = IntOptions {
    indexed: false,
    stored: false,
    fast: true,
};

/// Shortcut for a u64 indexed field.
///
/// Such a shortcut can be composed as follows `STORED | FAST | INT_INDEXED`
pub const INT_INDEXED: IntOptions = IntOptions {
    indexed: true,
    stored: false,
    fast: false,
};

/// Shortcut for a u64 stored field.
///
/// Such a shortcut can be composed as follows `STORED | FAST | INT_INDEXED`
pub const INT_STORED: IntOptions = IntOptions {
    indexed: false,
    stored: true,
    fast: false,
};

impl BitOr for IntOptions {
    type Output = IntOptions;

    fn bitor(self, other: IntOptions) -> IntOptions {
        let mut res = IntOptions::default();
        res.indexed = self.indexed | other.indexed;
        res.stored = self.stored | other.stored;
        res.fast = self.fast | other.fast;
        res
    }
}
