use super::SegmentComponent;
use crate::core::Index;
use crate::core::SegmentId;
use crate::core::SegmentMeta;
use crate::directory::error::{OpenReadError, OpenWriteError};
use crate::directory::Directory;
use crate::directory::{ReadOnlySource, WritePtr};
use crate::indexer::segment_serializer::SegmentSerializer;
use crate::schema::Schema;
use crate::Opstamp;
use crate::Result;
use std::fmt;
use std::path::PathBuf;
use std::result;

/// A segment is a piece of the index.
#[derive(Clone)]
pub struct Segment {
    index: Index,
    meta: SegmentMeta,
}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Segment({:?})", self.id().uuid_string())
    }
}

/// Creates a new segment given an `Index` and a `SegmentId`
///
/// The function is here to make it private outside `tantivy`.
/// #[doc(hidden)]
pub fn create_segment(index: Index, meta: SegmentMeta) -> Segment {
    Segment { index, meta }
}

impl Segment {
    /// Returns the index the segment belongs to.
    pub fn index(&self) -> &Index {
        &self.index
    }

    /// Returns our index's schema.
    pub fn schema(&self) -> Schema {
        self.index.schema()
    }

    /// Returns the segment meta-information
    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    #[doc(hidden)]
    pub fn with_delete_meta(self, num_deleted_docs: u32, opstamp: Opstamp) -> Segment {
        Segment {
            index: self.index,
            meta: self.meta.with_delete_meta(num_deleted_docs, opstamp),
        }
    }

    /// Returns the segment's id.
    pub fn id(&self) -> SegmentId {
        self.meta.id()
    }

    /// Returns the relative path of a component of our segment.
    ///
    /// It just joins the segment id with the extension
    /// associated to a segment component.
    pub fn relative_path(&self, component: SegmentComponent) -> PathBuf {
        self.meta.relative_path(component)
    }

    /// Open one of the component file for a *regular* read.
    pub fn open_read(
        &self,
        component: SegmentComponent,
    ) -> result::Result<ReadOnlySource, OpenReadError> {
        let path = self.relative_path(component);
        let source = self.index.directory().open_read(&path)?;
        Ok(source)
    }

    /// Open one of the component file for *regular* write.
    pub fn open_write(
        &mut self,
        component: SegmentComponent,
    ) -> result::Result<WritePtr, OpenWriteError> {
        let path = self.relative_path(component);
        let write = self.index.directory_mut().open_write(&path)?;
        Ok(write)
    }
}

pub trait SerializableSegment {
    /// Writes a view of a segment by pushing information
    /// to the `SegmentSerializer`.
    ///
    /// # Returns
    /// The number of documents in the segment.
    fn write(&self, serializer: SegmentSerializer) -> Result<u32>;
}
