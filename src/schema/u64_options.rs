use std::ops::BitOr;

/// Define how a u64 field should be handled by tantivy.
#[derive(Clone,Debug,PartialEq,Eq, RustcDecodable, RustcEncodable)]
pub struct U64Options {
    indexed: bool,
    fast: bool,
    stored: bool,
}

impl U64Options {
   
    /// Returns true iff the value is stored.
    pub fn is_stored(&self,) -> bool {
        self.stored
    }
    
    
    /// Returns true iff the value is indexed.
    pub fn is_indexed(&self,) -> bool {
        self.indexed
    }
    
    /// Returns true iff the value is a fast field. 
    pub fn is_fast(&self,) -> bool {
        self.fast
    }
    
    /// Set the u64 options as stored.
    ///
    /// Only the fields that are set as *stored* are
    /// persisted into the Tantivy's store.
    pub fn set_stored(mut self,) -> U64Options {
        self.stored = true;
        self
    }
    
    /// Set the u64 options as indexed.
    ///
    /// Setting an integer as indexed will generate
    /// a posting list for each value taken by the integer.
    pub fn set_indexed(mut self,) -> U64Options {
        self.indexed = true;
        self
    }
    
    /// Set the u64 options as a fast field.
    ///
    /// Fast fields are designed for random access.
    /// Access time are similar to a random lookup in an array. 
    /// If more than one value is associated to a fast field, only the last one is
    /// kept.
    pub fn set_fast(mut self,) -> U64Options {
        self.fast = true;
        self
    }
}

impl Default for U64Options {
    fn default() -> U64Options {
        U64Options {
            fast: false,
            indexed: false,
            stored: false,
        }
    }    
}


/// Shortcut for a u64 fast field.
///
/// Such a shortcut can be composed as follows `STORED | FAST | U64_INDEXED`
pub const FAST: U64Options = U64Options {
    indexed: false,
    stored: false,
    fast: true,
};

/// Shortcut for a u64 indexed field.
///
/// Such a shortcut can be composed as follows `STORED | FAST | U64_INDEXED`
pub const U64_INDEXED: U64Options = U64Options {
    indexed: true,
    stored: false,
    fast: false,
};

/// Shortcut for a u64 stored field. 
///
/// Such a shortcut can be composed as follows `STORED | FAST | U64_INDEXED`
pub const U64_STORED: U64Options = U64Options {
    indexed: false,
    stored: true,
    fast: false,
};


impl BitOr for U64Options {

    type Output = U64Options;

    fn bitor(self, other: U64Options) -> U64Options {
        let mut res = U64Options::default();
        res.indexed = self.indexed | other.indexed;
        res.stored = self.stored | other.stored;
        res.fast = self.fast | other.fast;
        res
    }
}