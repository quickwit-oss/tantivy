use schema::Schema;
use core::SegmentMeta;

/// Meta information about the `Index`.
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
