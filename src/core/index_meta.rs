use schema::Schema;
use core::SegmentMeta;

/// Meta information about the `Index`.
///
/// This object is serialized on disk in the `meta.json` file.
/// It keeps information about
/// * the searchable segments,
/// * the index `docstamp`
/// * the schema
///
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexMeta {
    pub segments: Vec<SegmentMeta>,
    pub schema: Schema,
    pub opstamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
}

impl IndexMeta {
    pub fn with_schema(schema: Schema) -> IndexMeta {
        IndexMeta {
            segments: vec![],
            schema,
            opstamp: 0u64,
            payload: None,
        }
    }
}


#[cfg(test)]
mod tests {

    use serde_json;
    use super::IndexMeta;
    use schema::{TEXT, SchemaBuilder};


    #[test]
    fn test_serialize_metas() {
        let schema = {
            let mut schema_builder = SchemaBuilder::new();
            schema_builder.add_text_field("text", TEXT);
            schema_builder.build()
        };
        let index_metas = IndexMeta {
            segments: Vec::new(),
            schema: schema,
            opstamp: 0u64,
            payload: None
        };
        let json = serde_json::ser::to_string(&index_metas).expect("serialization failed");
        assert_eq!(json, r#"{"segments":[],"schema":[{"name":"text","type":"text","options":{"indexing":{"record":"position","tokenizer":"default"},"stored":false}}],"opstamp":0}"#);
    }
}