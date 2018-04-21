use super::SegmentComponent;
use core::SegmentId;
use std::collections::HashSet;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DeleteMeta {
    num_deleted_docs: u32,
    opstamp: u64,
}

/// `SegmentMeta` contains simple meta information about a segment.
///
/// For instance the number of docs it contains,
/// how many are deleted, etc.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SegmentMeta {
    segment_id: SegmentId,
    max_doc: u32,
    deletes: Option<DeleteMeta>,
}

impl SegmentMeta {
    /// Creates a new segment meta for
    /// a segment with no deletes and no documents.
    pub fn new(segment_id: SegmentId) -> SegmentMeta {
        SegmentMeta {
            segment_id,
            max_doc: 0,
            deletes: None,
        }
    }

    /// Returns the segment id.
    pub fn id(&self) -> SegmentId {
        self.segment_id
    }

    /// Returns the number of deleted documents.
    pub fn num_deleted_docs(&self) -> u32 {
        self.deletes
            .as_ref()
            .map(|delete_meta| delete_meta.num_deleted_docs)
            .unwrap_or(0u32)
    }

    /// Returns the list of files that
    /// are required for the segment meta.
    ///
    /// This is useful as the way tantivy removes files
    /// is by removing all files that have been created by tantivy
    /// and are not used by any segment anymore.
    pub fn list_files(&self) -> HashSet<PathBuf> {
        SegmentComponent::iterator()
            .map(|component| self.relative_path(*component))
            .collect::<HashSet<PathBuf>>()
    }

    /// Returns the relative path of a component of our segment.
    ///
    /// It just joins the segment id with the extension
    /// associated to a segment component.
    pub fn relative_path(&self, component: SegmentComponent) -> PathBuf {
        let mut path = self.id().uuid_string();
        path.push_str(&*match component {
            SegmentComponent::POSITIONS => ".pos".to_string(),
            SegmentComponent::POSTINGS => ".idx".to_string(),
            SegmentComponent::TERMS => ".term".to_string(),
            SegmentComponent::STORE => ".store".to_string(),
            SegmentComponent::FASTFIELDS => ".fast".to_string(),
            SegmentComponent::FIELDNORMS => ".fieldnorm".to_string(),
            SegmentComponent::DELETE => format!(".{}.del", self.delete_opstamp().unwrap_or(0)),
        });
        PathBuf::from(path)
    }

    /// Return the highest doc id + 1
    ///
    /// If there are no deletes, then num_docs = max_docs
    /// and all the doc ids contains in this segment
    /// are exactly (0..max_doc).
    pub fn max_doc(&self) -> u32 {
        self.max_doc
    }

    /// Return the number of documents in the segment.
    pub fn num_docs(&self) -> u32 {
        self.max_doc() - self.num_deleted_docs()
    }

    /// Returns the opstamp of the last delete operation
    /// taken in account in this segment.
    pub fn delete_opstamp(&self) -> Option<u64> {
        self.deletes.as_ref().map(|delete_meta| delete_meta.opstamp)
    }

    /// Returns true iff the segment meta contains
    /// delete information.
    pub fn has_deletes(&self) -> bool {
        self.deletes.is_some()
    }

    #[doc(hidden)]
    pub fn set_max_doc(&mut self, max_doc: u32) {
        self.max_doc = max_doc;
    }

    #[doc(hidden)]
    pub fn set_delete_meta(&mut self, num_deleted_docs: u32, opstamp: u64) {
        self.deletes = Some(DeleteMeta {
            num_deleted_docs,
            opstamp,
        });
    }
}
