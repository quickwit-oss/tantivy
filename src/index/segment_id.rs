use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use std::str::FromStr;
#[cfg(test)]
use std::sync::atomic;

#[cfg(test)]
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Uuid identifying a segment.
///
/// Tantivy's segment are identified
/// by a UUID which is used to prefix the filenames
/// of all of the file associated with the segment.
///
/// Segments created by tantivy use a UUIDv7, which embeds the segment's
/// creation time in its most significant bits. As a result segment ids sort
/// chronologically and the creation time can be recovered through
/// [`SegmentId::creation_time`]. Ids read from older indices (created before
/// this change) are UUIDv4 and remain fully supported; for those
/// [`SegmentId::creation_time`] returns `None`.
///
/// In unit test, for reproducibility, the `SegmentId` are
/// simply generated in an autoincrement fashion.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentId(Uuid);

#[cfg(test)]
static AUTO_INC_COUNTER: Lazy<atomic::AtomicUsize> = Lazy::new(atomic::AtomicUsize::default);

#[cfg(test)]
const ZERO_ARRAY: [u8; 8] = [0u8; 8];

// During tests, we generate the segment id in a autoincrement manner
// for consistency of segment id between run.
//
// The order of the test execution is not guaranteed, but the order
// of segments within a single test is guaranteed.
#[cfg(test)]
fn create_uuid() -> Uuid {
    let new_auto_inc_id = (*AUTO_INC_COUNTER).fetch_add(1, atomic::Ordering::SeqCst);
    Uuid::from_fields(new_auto_inc_id as u32, 0, 0, &ZERO_ARRAY)
}

#[cfg(not(test))]
fn create_uuid() -> Uuid {
    // UUIDv7 embeds a 48-bit millisecond creation timestamp in its most significant
    // bits, followed by random bits. This keeps segment ids universally unique and
    // lock-free to generate from any indexing thread (like the previous v4), while
    // additionally making them:
    //   - chronologically sortable (ids sort by creation time), and
    //   - self-describing (the creation time can be recovered, see `SegmentId::creation_time`).
    Uuid::now_v7()
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
    /// segments in a display message (e.g. a5c4dfcb).
    pub fn short_uuid_string(&self) -> String {
        self.0.as_simple().to_string()[..8].to_string()
    }

    /// Returns a segment uuid string.
    ///
    /// It consists in 32 lowercase hexadecimal chars
    /// (e.g. a5c4dfcbdfe645089129e308e26d5523)
    pub fn uuid_string(&self) -> String {
        self.0.as_simple().to_string()
    }

    /// Build a `SegmentId` string from the full uuid string.
    ///
    /// E.g. "a5c4dfcbdfe645089129e308e26d5523"
    pub fn from_uuid_string(uuid_string: &str) -> Result<SegmentId, SegmentIdParseError> {
        FromStr::from_str(uuid_string)
    }

    /// Returns the creation time embedded in the segment id, if available.
    ///
    /// Segments created by recent versions of tantivy use a UUIDv7, whose most
    /// significant bits encode the millisecond timestamp at which the id (and
    /// hence the segment) was created. This can be handy when investigating an
    /// index: it tells you when each segment was produced without relying on
    /// filesystem timestamps.
    ///
    /// Returns `None` for segment ids that do not carry a timestamp, i.e. ids
    /// from older indices (UUIDv4) and the autoincrement ids used in tests.
    pub fn creation_time(&self) -> Option<std::time::SystemTime> {
        // `get_timestamp` returns `Some` only for UUID versions that carry a
        // timestamp (v7 here); it is `None` for v4.
        let timestamp = self.0.get_timestamp()?;
        let (secs, nanos) = timestamp.to_unix();
        Some(std::time::UNIX_EPOCH + std::time::Duration::new(secs, nanos))
    }
}

/// Error type used when parsing a `SegmentId` from a string fails.
pub struct SegmentIdParseError(uuid::Error);

impl Error for SegmentIdParseError {}

impl fmt::Debug for SegmentIdParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for SegmentIdParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for SegmentId {
    type Err = SegmentIdParseError;

    fn from_str(uuid_string: &str) -> Result<Self, SegmentIdParseError> {
        let uuid = Uuid::parse_str(uuid_string).map_err(SegmentIdParseError)?;
        Ok(SegmentId(uuid))
    }
}

impl fmt::Debug for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Seg({:?})", self.short_uuid_string())
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

#[cfg(test)]
mod tests {
    use super::SegmentId;

    #[test]
    fn test_to_uuid_string() {
        let full_uuid = "a5c4dfcbdfe645089129e308e26d5523";
        let segment_id = SegmentId::from_uuid_string(full_uuid).unwrap();
        assert_eq!(segment_id.uuid_string(), full_uuid);
        assert_eq!(segment_id.short_uuid_string(), "a5c4dfcb");
        // one extra char
        assert!(SegmentId::from_uuid_string("a5c4dfcbdfe645089129e308e26d5523b").is_err());
    }

    #[test]
    fn test_creation_time_none_for_v4() {
        // A legacy UUIDv4 id (version nibble `4`) carries no timestamp.
        let v4 = SegmentId::from_uuid_string("a5c4dfcbdfe645089129e308e26d5523").unwrap();
        assert!(v4.creation_time().is_none());
    }

    #[test]
    fn test_creation_time_some_for_v7() {
        use std::time::{Duration, SystemTime};

        use uuid::Uuid;

        // Build a UUIDv7 directly so this test does not depend on the (test-only)
        // autoincrement id generation used elsewhere.
        let before = SystemTime::now();
        let seg = SegmentId(Uuid::now_v7());
        let after = SystemTime::now();

        let created = seg
            .creation_time()
            .expect("a v7 segment id must expose a creation time");

        // v7 has millisecond precision, so allow a small slack around the window.
        let slack = Duration::from_millis(1);
        assert!(created >= before - slack);
        assert!(created <= after + slack);
    }
}
