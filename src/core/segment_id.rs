use std::cmp::{Ord, Ordering};
use std::fmt;
use uuid::Uuid;

#[cfg(test)]
use std::sync::atomic;

/// Uuid identifying a segment.
///
/// Tantivy's segment are identified
/// by a UUID which is used to prefix the filenames
/// of all of the file associated with the segment.
///
/// In unit test, for reproducability, the `SegmentId` are
/// simply generated in an autoincrement fashion.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    #[doc(hidden)]
    pub fn generate_random() -> SegmentId {
        SegmentId(create_uuid())
    }

    /// Returns a shorter identifier of the segment.
    ///
    /// We are using UUID4, so only 6 bits are fixed,
    /// and the rest is random.
    ///
    /// Picking the first 8 chars is ok to identify
    /// segments in a display message.
    pub fn short_uuid_string(&self) -> String {
        (&self.0.simple().to_string()[..8]).to_string()
    }

    /// Returns a segment uuid string.
    pub fn uuid_string(&self) -> String {
        self.0.simple().to_string()
    }
}

impl fmt::Debug for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Seg({:?})", self.short_uuid_string())
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
