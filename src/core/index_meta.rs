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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexMeta {
    pub segments: Vec<SegmentMeta>,
    pub schema: Schema,
    pub opstamp: u64,
}

impl IndexMeta {
    pub fn with_schema(schema: Schema) -> IndexMeta {
        IndexMeta {
            segments: vec![],
            schema: schema,
            opstamp: 0u64,
        }
    }
}
