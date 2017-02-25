use uuid::Uuid;
use std::fmt;
use rustc_serialize::{Encoder, Decoder, Encodable, Decodable};
use std::cmp::{Ordering, Ord};

#[cfg(test)]
use std::sync::atomic;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegmentId(Uuid);


#[cfg(test)]
lazy_static! {
    static ref AUTO_INC_COUNTER: atomic::AtomicUsize = atomic::AtomicUsize::default();
    static ref EMPTY_ARR: [u8; 8] = [0u8; 8];
}


// During tests, we generate the segment id in a autoincrement manner
// for consistency of segment id between run.
//
// The order of the test execution is not guaranteed, but the order
// of segments within a single test is guaranteed.
#[cfg(test)]
fn create_uuid() -> Uuid {
    let new_auto_inc_id = (*AUTO_INC_COUNTER).fetch_add(1, atomic::Ordering::SeqCst);
    Uuid::from_fields(new_auto_inc_id as u32, 0, 0, &*EMPTY_ARR).unwrap()
}

#[cfg(not(test))]
fn create_uuid() -> Uuid {
    Uuid::new_v4()
}

impl SegmentId {
    pub fn generate_random() -> SegmentId {
        SegmentId(create_uuid())
    }

    pub fn short_uuid_string(&self,) -> String {
        (&self.0.simple().to_string()[..8]).to_string()
    }

    pub fn uuid_string(&self,) -> String {
        self.0.simple().to_string()
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
