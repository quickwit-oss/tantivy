
use schema::Schema;
use core::SegmentId;


/// MetaInformation about the `Index`.
/// 
/// This object is serialized on disk in the `meta.json` file.
/// It keeps information about 
/// * the searchable segments,
/// * the index docstamp
/// * the schema
///
#[derive(Clone,Debug,RustcDecodable,RustcEncodable)]
pub struct IndexMeta {
    pub committed_segments: Vec<SegmentMeta>,
    pub uncommitted_segments: Vec<SegmentMeta>,
    pub schema: Schema,
    pub docstamp: u64,
}

impl IndexMeta {
    pub fn with_schema(schema: Schema) -> IndexMeta {
        IndexMeta {
            committed_segments: Vec::new(),
            uncommitted_segments: Vec::new(),
            schema: schema,
            docstamp: 0u64,
        }
    }
}

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