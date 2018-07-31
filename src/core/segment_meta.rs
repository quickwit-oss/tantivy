use super::SegmentComponent;
use census::{Inventory, TrackedObject};
use core::SegmentId;
use serde;
use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;

lazy_static! {
    static ref INVENTORY: Inventory<InnerSegmentMeta> = { Inventory::new() };
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DeleteMeta {
    num_deleted_docs: u32,
    opstamp: u64,
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
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
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

impl<'a> serde::Deserialize<'a> for SegmentMeta {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as serde::Deserializer<'a>>::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let inner = InnerSegmentMeta::deserialize(deserializer)?;
        let tracked = INVENTORY.track(inner);
        Ok(SegmentMeta { tracked: tracked })
    }
}

impl SegmentMeta {
    /// Lists all living `SegmentMeta` object at the time of the call.
    pub fn all() -> Vec<SegmentMeta> {
        INVENTORY
            .list()
            .into_iter()
            .map(|inner| SegmentMeta { tracked: inner })
            .collect::<Vec<_>>()
    }

    /// Creates a new `SegmentMeta` object.
    #[doc(hidden)]
    pub fn new(segment_id: SegmentId, max_doc: u32) -> SegmentMeta {
        let inner = InnerSegmentMeta {
            segment_id,
            max_doc,
            deletes: None,
        };
        SegmentMeta {
            tracked: INVENTORY.track(inner),
        }
    }

    /// Returns the segment id.
    pub fn id(&self) -> SegmentId {
        self.tracked.segment_id
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
            SegmentComponent::POSTINGS => ".idx".to_string(),
            SegmentComponent::POSITIONS => ".pos".to_string(),
            SegmentComponent::POSITIONSSKIP => ".posidx".to_string(),
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
        self.tracked.max_doc
    }

    /// Return the number of documents in the segment.
    pub fn num_docs(&self) -> u32 {
        self.max_doc() - self.num_deleted_docs()
    }

    /// Returns the opstamp of the last delete operation
    /// taken in account in this segment.
    pub fn delete_opstamp(&self) -> Option<u64> {
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

    #[doc(hidden)]
    pub fn with_delete_meta(self, num_deleted_docs: u32, opstamp: u64) -> SegmentMeta {
        let delete_meta = DeleteMeta {
            num_deleted_docs,
            opstamp,
        };
        let tracked = self.tracked.map(move |inner_meta| InnerSegmentMeta {
            segment_id: inner_meta.segment_id,
            max_doc: inner_meta.max_doc,
            deletes: Some(delete_meta),
        });
        SegmentMeta { tracked }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct InnerSegmentMeta {
    segment_id: SegmentId,
    max_doc: u32,
    deletes: Option<DeleteMeta>,
}
