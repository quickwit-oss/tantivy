use uuid::Uuid;
use std::fmt;
use rustc_serialize::{Encoder, Decoder, Encodable, Decodable};
use core::SegmentComponent;
use std::path::PathBuf;
use std::cmp::{Ordering, Ord};


#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegmentId(Uuid);

impl SegmentId {
    pub fn generate_random() -> SegmentId {
        SegmentId(Uuid::new_v4())
    }
    
    pub fn uuid_string(&self,) -> String {
        self.0.to_simple_string()
    }
    
    pub fn relative_path(&self, component: SegmentComponent) -> PathBuf {
        let filename = self.uuid_string() + component.path_suffix();
        PathBuf::from(filename)
    }
}

impl Encodable for SegmentId {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        self.0.encode(s)
    }
}

impl Decodable for SegmentId {
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        Uuid::decode(d).map(SegmentId)
    }
}

impl fmt::Debug for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentId({:?})", self.uuid_string())
    }
}


impl PartialOrd for SegmentId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SegmentId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.as_bytes().cmp(other.0.as_bytes())
    }
}