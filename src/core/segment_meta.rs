use core::SegmentId;


#[derive(Clone, Debug, RustcDecodable,RustcEncodable)]
struct DeleteMeta {
    num_deleted_docs: u32,
    opstamp: u64,
}

#[derive(Clone, Debug, RustcDecodable,RustcEncodable)]
pub struct SegmentMeta {
    segment_id: SegmentId,
    num_docs: u32,
    deletes: Option<DeleteMeta>, 
}

impl SegmentMeta {
    pub fn new(segment_id: SegmentId) -> SegmentMeta {
        SegmentMeta {
            segment_id: segment_id,
            num_docs: 0,
            deletes: None,
        }
    }

    pub fn id(&self) -> SegmentId {
        self.segment_id
    }

    pub fn num_docs(&self) -> u32 {
        self.num_docs
    } 

    pub fn delete_opstamp(&self) -> Option<u64> {
        self.deletes
            .as_ref()
            .map(|delete_meta| delete_meta.opstamp)
    }

    pub fn has_deletes(&self) -> bool {
        self.deletes.is_some()
    }

    pub fn set_num_docs(&mut self, num_docs: u32) {
        self.num_docs = num_docs;
    }

    pub fn set_delete_meta(&mut self, num_deleted_docs: u32, opstamp: u64) {
        self.deletes = Some(DeleteMeta {
            num_deleted_docs: num_deleted_docs,
            opstamp: opstamp,
        });
    }
}
