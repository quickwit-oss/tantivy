pub mod searcher;

pub mod index;
mod segment_reader;
mod segment_id;
mod segment_component;
mod segment;
mod pool;

pub use self::segment_component::SegmentComponent;
pub use self::segment_id::SegmentId;
pub use self::segment_reader::SegmentReader;
pub use self::segment::Segment;
pub use self::segment::SegmentInfo;
pub use self::segment::SerializableSegment;
pub use self::index::Index;

#[derive(Clone, Debug, RustcDecodable,RustcEncodable)]
pub struct SegmentMeta {
    pub segment_id: SegmentId,
    pub num_docs: usize,
}

#[cfg(test)]
impl SegmentMeta {
    pub fn new(segment_id: SegmentId, num_docs: usize) -> SegmentMeta {
        SegmentMeta {
            segment_id: segment_id,
            num_docs: num_docs,
        }
    }
}