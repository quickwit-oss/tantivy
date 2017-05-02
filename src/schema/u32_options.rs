use std::ops::BitOr;

/// Define how a U32 field should be handled by tantivy.
#[derive(Clone,Debug,PartialEq,Eq, Serialize, Deserialize)]
pub struct U32Options {
    indexed: bool,
    fast: bool,
    stored: bool,
}

impl U32Options {
   
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
    
    /// Set the u32 options as stored.
    ///
    /// Only the fields that are set as *stored* are
    /// persisted into the Tantivy's store.
    pub fn set_stored(mut self,) -> U32Options {
        self.stored = true;
        self
    }
    
    /// Set the u32 options as indexed.
    ///
    /// Setting an integer as indexed will generate
    /// a posting list for each value taken by the integer.
    pub fn set_indexed(mut self,) -> U32Options {
        self.indexed = true;
        self
    }
    
    /// Set the u32 options as a fast field.
    ///
    /// Fast fields are designed for random access.
    /// Access time are similar to a random lookup in an array. 
    /// If more than one value is associated to a fast field, only the last one is
    /// kept.
    pub fn set_fast(mut self,) -> U32Options {
        self.fast = true;
        self
    }
}

impl Default for U32Options {
    fn default() -> U32Options {
        U32Options {
            fast: false,
            indexed: false,
            stored: false,
        }
    }    
}


/// Shortcut for a u32 fast field.
///
/// Such a shortcut can be composed as follows `STORED | FAST | U32_INDEXED`
pub const FAST: U32Options = U32Options {
    indexed: false,
    stored: false,
    fast: true,
};

/// Shortcut for a u32 indexed field.
///
/// Such a shortcut can be composed as follows `STORED | FAST | U32_INDEXED`
pub const U32_INDEXED: U32Options = U32Options {
    indexed: true,
    stored: false,
    fast: false,
};

/// Shortcut for a u32 stored field. 
///
/// Such a shortcut can be composed as follows `STORED | FAST | U32_INDEXED`
pub const U32_STORED: U32Options = U32Options {
    indexed: false,
    stored: true,
    fast: false,
};


impl BitOr for U32Options {

    type Output = U32Options;

    fn bitor(self, other: U32Options) -> U32Options {
        let mut res = U32Options::default();
        res.indexed = self.indexed | other.indexed;
        res.stored = self.stored | other.stored;
        res.fast = self.fast | other.fast;
        res
    }
}