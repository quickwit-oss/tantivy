use core::SegmentId;


// TODO Option<DeleteMeta>

#[derive(Clone, Debug, RustcDecodable,RustcEncodable)]
pub struct SegmentMeta {
    pub segment_id: SegmentId,
    pub num_docs: u32,
    pub num_deleted_docs: u32,
    pub delete_opstamp: Option<u64>,
}

impl SegmentMeta {
    pub fn new(segment_id: SegmentId) -> SegmentMeta {
        SegmentMeta {
            segment_id: segment_id,
            num_docs: 0,
            num_deleted_docs: 0,
            delete_opstamp: None,
        }
    }
}
