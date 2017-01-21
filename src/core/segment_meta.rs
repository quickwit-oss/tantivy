use core::SegmentId;

#[derive(Clone, Debug, RustcDecodable,RustcEncodable)]
pub struct SegmentMeta {
    pub segment_id: SegmentId,
    pub num_docs: u32,
    pub num_deleted_docs: u32,
}

#[cfg(test)]
impl SegmentMeta {
    pub fn new(segment_id: SegmentId, num_docs: u32) -> SegmentMeta {
        SegmentMeta {
            segment_id: segment_id,
            num_docs: num_docs,
            num_deleted_docs: 0,
        }
    }
}