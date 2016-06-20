use rustc_serialize::Decoder;
use rustc_serialize::Encoder;

#[derive(Clone,Debug,PartialEq,Eq, RustcDecodable, RustcEncodable)]
pub struct U32Options {
    indexed: bool,
    fast: bool,
    stored: bool,
}

impl U32Options {

    pub fn new() -> U32Options {
        U32Options {
            fast: false,
            indexed: false,
            stored: false,
        }
    }
    
    pub fn is_stored(&self,) -> bool {
        self.stored
    }
    
    pub fn is_indexed(&self,) -> bool {
        self.indexed
    }

    pub fn set_indexed(mut self,) -> U32Options {
        self.indexed = true;
        self
    }

    pub fn is_fast(&self,) -> bool {
        self.fast
    }

    pub fn set_fast(mut self,) -> U32Options {
        self.fast = true;
        self
    }
}




/// The field will be tokenized and indexed
pub const FAST_U32: U32Options = U32Options {
    indexed: false,
    stored: false,
    fast: true,
};
