#[derive(Clone,Debug,PartialEq,Eq, RustcDecodable, RustcEncodable)]
pub struct U32Options {
    indexed: bool,
    fast: bool,
    stored: bool,
}

impl U32Options {
   
    pub fn is_stored(&self,) -> bool {
        self.stored
    }
    
    pub fn is_indexed(&self,) -> bool {
        self.indexed
    }
    
    pub fn is_fast(&self,) -> bool {
        self.fast
    }
    
    pub fn set_stored(mut self,) -> U32Options {
        self.stored = true;
        self
    }

    pub fn set_indexed(mut self,) -> U32Options {
        self.indexed = true;
        self
    }
    
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


/// The field will be tokenized and indexed
pub const FAST: U32Options = U32Options {
    indexed: false,
    stored: false,
    fast: true,
};
