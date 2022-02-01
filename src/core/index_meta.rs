use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::SegmentComponent;
use crate::core::SegmentId;
use crate::schema::Schema;
use crate::store::Compressor;
use crate::{Inventory, Opstamp, TrackedObject};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DeleteMeta {
    num_deleted_docs: u32,
    opstamp: Opstamp,
}

#[derive(Clone, Default)]
pub struct SegmentMetaInventory {
    inventory: Inventory<InnerSegmentMeta>,
}

impl SegmentMetaInventory {
    /// Lists all living `SegmentMeta` object at the time of the call.
    pub fn all(&self) -> Vec<SegmentMeta> {
        self.inventory
            .list()
            .into_iter()
            .map(SegmentMeta::from)
            .collect::<Vec<_>>()
    }

    pub fn new_segment_meta(&self, segment_id: SegmentId, max_doc: u32) -> SegmentMeta {
        let inner = InnerSegmentMeta {
            segment_id,
            max_doc,
            include_temp_doc_store: Arc::new(AtomicBool::new(true)),
            deletes: None,
        };
        SegmentMeta::from(self.inventory.track(inner))
    }
}

/// `SegmentMeta` contains simple meta information about a segment.
///
/// For instance the number of docs it contains,
/// how many are deleted, etc.
#[derive(Clone)]
pub struct SegmentMeta {
    tracked: TrackedObject<InnerSegmentMeta>,
}

impl fmt::Debug for SegmentMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.tracked.fmt(f)
    }
}

impl serde::Serialize for SegmentMeta {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>
    where
        S: serde::Serializer,
    {
        self.tracked.serialize(serializer)
    }
}

impl From<TrackedObject<InnerSegmentMeta>> for SegmentMeta {
    fn from(tracked: TrackedObject<InnerSegmentMeta>) -> SegmentMeta {
        SegmentMeta { tracked }
    }
}

impl SegmentMeta {
    // Creates a new `SegmentMeta` object.

    /// Returns the segment id.
    pub fn id(&self) -> SegmentId {
        self.tracked.segment_id
    }

    /// Removes the Component::TempStore from the alive list and
    /// therefore marks the temp docstore file to be deleted by
    /// the garbage collection.
    pub fn untrack_temp_docstore(&self) {
        self.tracked
            .include_temp_doc_store
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Returns the number of deleted documents.
    pub fn num_deleted_docs(&self) -> u32 {
        self.tracked
            .deletes
            .as_ref()
            .map(|delete_meta| delete_meta.num_deleted_docs)
            .unwrap_or(0u32)
    }

    /// Returns the list of files that
    /// are required for the segment meta.
    /// Note: Some of the returned files may not exist depending on the state of the segment.
    ///
    /// This is useful as the way tantivy removes files
    /// is by removing all files that have been created by tantivy
    /// and are not used by any segment anymore.
    pub fn list_files(&self) -> HashSet<PathBuf> {
        if self
            .tracked
            .include_temp_doc_store
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            SegmentComponent::iterator()
                .map(|component| self.relative_path(*component))
                .collect::<HashSet<PathBuf>>()
        } else {
            SegmentComponent::iterator()
                .filter(|comp| *comp != &SegmentComponent::TempStore)
                .map(|component| self.relative_path(*component))
                .collect::<HashSet<PathBuf>>()
        }
    }

    /// Returns the relative path of a component of our segment.
    ///
    /// It just joins the segment id with the extension
    /// associated to a segment component.
    pub fn relative_path(&self, component: SegmentComponent) -> PathBuf {
        let mut path = self.id().uuid_string();
        path.push_str(&*match component {
            SegmentComponent::Postings => ".idx".to_string(),
            SegmentComponent::Positions => ".pos".to_string(),
            SegmentComponent::Terms => ".term".to_string(),
            SegmentComponent::Store => ".store".to_string(),
            SegmentComponent::TempStore => ".store.temp".to_string(),
            SegmentComponent::FastFields => ".fast".to_string(),
            SegmentComponent::FieldNorms => ".fieldnorm".to_string(),
            SegmentComponent::Delete => format!(".{}.del", self.delete_opstamp().unwrap_or(0)),
        });
        PathBuf::from(path)
    }

    /// Return the highest doc id + 1
    ///
    /// If there are no deletes, then num_docs = max_docs
    /// and all the doc ids contains in this segment
    /// are exactly (0..max_doc).
    pub fn max_doc(&self) -> u32 {
        self.tracked.max_doc
    }

    /// Return the number of documents in the segment.
    pub fn num_docs(&self) -> u32 {
        self.max_doc() - self.num_deleted_docs()
    }

    /// Returns the `Opstamp` of the last delete operation
    /// taken in account in this segment.
    pub fn delete_opstamp(&self) -> Option<Opstamp> {
        self.tracked
            .deletes
            .as_ref()
            .map(|delete_meta| delete_meta.opstamp)
    }

    /// Returns true iff the segment meta contains
    /// delete information.
    pub fn has_deletes(&self) -> bool {
        self.num_deleted_docs() > 0
    }

    /// Updates the max_doc value from the `SegmentMeta`.
    ///
    /// This method is only used when updating `max_doc` from 0
    /// as we finalize a fresh new segment.
    pub(crate) fn with_max_doc(self, max_doc: u32) -> SegmentMeta {
        assert_eq!(self.tracked.max_doc, 0);
        assert!(self.tracked.deletes.is_none());
        let tracked = self.tracked.map(move |inner_meta| InnerSegmentMeta {
            segment_id: inner_meta.segment_id,
            max_doc,
            deletes: None,
            include_temp_doc_store: Arc::new(AtomicBool::new(true)),
        });
        SegmentMeta { tracked }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn with_delete_meta(self, num_deleted_docs: u32, opstamp: Opstamp) -> SegmentMeta {
        assert!(
            num_deleted_docs <= self.max_doc(),
            "There cannot be more deleted docs than there are docs."
        );
        let delete_meta = DeleteMeta {
            num_deleted_docs,
            opstamp,
        };
        let tracked = self.tracked.map(move |inner_meta| InnerSegmentMeta {
            segment_id: inner_meta.segment_id,
            max_doc: inner_meta.max_doc,
            include_temp_doc_store: Arc::new(AtomicBool::new(true)),
            deletes: Some(delete_meta),
        });
        SegmentMeta { tracked }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InnerSegmentMeta {
    segment_id: SegmentId,
    max_doc: u32,
    deletes: Option<DeleteMeta>,
    /// If you want to avoid the SegmentComponent::TempStore file to be covered by
    /// garbage collection and deleted, set this to true. This is used during merge.
    #[serde(skip)]
    #[serde(default = "default_temp_store")]
    pub(crate) include_temp_doc_store: Arc<AtomicBool>,
}
fn default_temp_store() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

impl InnerSegmentMeta {
    pub fn track(self, inventory: &SegmentMetaInventory) -> SegmentMeta {
        SegmentMeta {
            tracked: inventory.inventory.track(self),
        }
    }
}

/// Search Index Settings.
///
/// Contains settings which are applied on the whole
/// index, like presort documents.
#[derive(Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct IndexSettings {
    /// Sorts the documents by information
    /// provided in `IndexSortByField`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_by_field: Option<IndexSortByField>,
    /// The `Compressor` used to compress the doc store.
    #[serde(default)]
    pub docstore_compression: Compressor,
}
/// Settings to presort the documents in an index
///
/// Presorting documents can greatly performance
/// in some scenarios, by applying top n
/// optimizations.
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct IndexSortByField {
    /// The field to sort the documents by
    pub field: String,
    /// The order to sort the documents by
    pub order: Order,
}
/// The order to sort by
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Order {
    /// Ascending Order
    Asc,
    /// Descending Order
    Desc,
}
impl Order {
    /// return if the Order is ascending
    pub fn is_asc(&self) -> bool {
        self == &Order::Asc
    }
    /// return if the Order is descending
    pub fn is_desc(&self) -> bool {
        self == &Order::Desc
    }
}

/// Meta information about the `Index`.
///
/// This object is serialized on disk in the `meta.json` file.
/// It keeps information about
/// * the searchable segments,
/// * the index `docstamp`
/// * the schema
#[derive(Clone, Serialize)]
pub struct IndexMeta {
    /// `IndexSettings` to configure index options.
    #[serde(default)]
    pub index_settings: IndexSettings,
    /// List of `SegmentMeta` informations associated to each finalized segment of the index.
    pub segments: Vec<SegmentMeta>,
    /// Index `Schema`
    pub schema: Schema,
    /// Opstamp associated to the last `commit` operation.
    pub opstamp: Opstamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Payload associated to the last commit.
    ///
    /// Upon commit, clients can optionally add a small `String` payload to their commit
    /// to help identify this commit.
    /// This payload is entirely unused by tantivy.
    pub payload: Option<String>,
}

#[derive(Deserialize)]
struct UntrackedIndexMeta {
    pub segments: Vec<InnerSegmentMeta>,
    #[serde(default)]
    pub index_settings: IndexSettings,
    pub schema: Schema,
    pub opstamp: Opstamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
}

impl UntrackedIndexMeta {
    pub fn track(self, inventory: &SegmentMetaInventory) -> IndexMeta {
        IndexMeta {
            index_settings: self.index_settings,
            segments: self
                .segments
                .into_iter()
                .map(|inner_seg_meta| inner_seg_meta.track(inventory))
                .collect::<Vec<SegmentMeta>>(),
            schema: self.schema,
            opstamp: self.opstamp,
            payload: self.payload,
        }
    }
}

impl IndexMeta {
    /// Create an `IndexMeta` object representing a brand new `Index`
    /// with the given index.
    ///
    /// This new index does not contains any segments.
    /// Opstamp will the value `0u64`.
    pub fn with_schema(schema: Schema) -> IndexMeta {
        IndexMeta {
            index_settings: IndexSettings::default(),
            segments: vec![],
            schema,
            opstamp: 0u64,
            payload: None,
        }
    }

    pub(crate) fn deserialize(
        meta_json: &str,
        inventory: &SegmentMetaInventory,
    ) -> serde_json::Result<IndexMeta> {
        let untracked_meta_json: UntrackedIndexMeta = serde_json::from_str(meta_json)?;
        Ok(untracked_meta_json.track(inventory))
    }
}

impl fmt::Debug for IndexMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    use crate::schema::{Schema, TEXT};
    use crate::{IndexSettings, IndexSortByField, Order};

    #[test]
    fn test_serialize_metas() {
        let schema = {
            let mut schema_builder = Schema::builder();
            schema_builder.add_text_field("text", TEXT);
            schema_builder.build()
        };
        let index_metas = IndexMeta {
            index_settings: IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "text".to_string(),
                    order: Order::Asc,
                }),
                ..Default::default()
            },
            segments: Vec::new(),
            schema,
            opstamp: 0u64,
            payload: None,
        };
        let json = serde_json::ser::to_string(&index_metas).expect("serialization failed");
        assert_eq!(
            json,
            r#"{"index_settings":{"sort_by_field":{"field":"text","order":"Asc"},"docstore_compression":"lz4"},"segments":[],"schema":[{"name":"text","type":"text","options":{"indexing":{"record":"position","fieldnorms":true,"tokenizer":"default"},"stored":false}}],"opstamp":0}"#
        );
    }
}
