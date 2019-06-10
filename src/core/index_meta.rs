use core::SegmentMeta;
use schema::Schema;
use serde_json;
use std::fmt;
use Opstamp;

/// Meta information about the `Index`.
///
/// This object is serialized on disk in the `meta.json` file.
/// It keeps information about
/// * the searchable segments,
/// * the index `docstamp`
/// * the schema
///
#[derive(Clone, Serialize, Deserialize)]
pub struct IndexMeta {
    /// List of `SegmentMeta` informations associated to each finalized segment of the index.
    pub segments: Vec<SegmentMeta>,
    /// Index `Schema`
    pub schema: Schema,
    /// Opstamp associated to the last `commit` operation.
    pub opstamp: Opstamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Payload associated to the last commit.
    ///
    /// Upon commit, clients can optionally add a small `Striing` payload to their commit
    /// to help identify this commit.
    /// This payload is entirely unused by tantivy.
    pub payload: Option<String>,
}

impl IndexMeta {
    /// Create an `IndexMeta` object representing a brand new `Index`
    /// with the given index.
    ///
    /// This new index does not contains any segments.
    /// Opstamp will the value `0u64`.
    pub fn with_schema(schema: Schema) -> IndexMeta {
        IndexMeta {
            segments: vec![],
            schema,
            opstamp: 0u64,
            payload: None,
        }
    }
}

impl fmt::Debug for IndexMeta {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            serde_json::ser::to_string(self)
                .expect("JSON serialization for IndexMeta should never fail.")
        )
    }
}

#[cfg(test)]
mod tests {

    use super::IndexMeta;
    use schema::{Schema, TEXT};
    use serde_json;

    #[test]
    fn test_serialize_metas() {
        let schema = {
            let mut schema_builder = Schema::builder();
            schema_builder.add_text_field("text", TEXT);
            schema_builder.build()
        };
        let index_metas = IndexMeta {
            segments: Vec::new(),
            schema,
            opstamp: 0u64,
            payload: None,
        };
        let json = serde_json::ser::to_string(&index_metas).expect("serialization failed");
        assert_eq!(json, r#"{"segments":[],"schema":[{"name":"text","type":"text","options":{"indexing":{"record":"position","tokenizer":"default"},"stored":false}}],"opstamp":0}"#);
    }
}
