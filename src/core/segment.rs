use super::SegmentComponent;
use crate::core::Index;
use crate::core::SegmentId;
use crate::core::SegmentMeta;
use crate::directory::error::{OpenReadError, OpenWriteError};
use crate::directory::{Directory, DirectoryClone};
use crate::directory::{ReadOnlySource, WritePtr};
use crate::indexer::segment_serializer::SegmentSerializer;
use crate::schema::Schema;
use crate::Opstamp;
use std::fmt;
use std::path::PathBuf;

/// A segment is a piece of the index.
pub struct Segment {
    schema: Schema,
    directory: Box<dyn Directory>,
    meta: SegmentMeta,
}

impl Clone for Segment {
    fn clone(&self) -> Self {
        Segment {
            schema: self.schema.clone(),
            directory: self.directory.box_clone(),
            meta: self.meta.clone(),
        }
    }
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
    Segment {
        directory: index.directory().box_clone(),
        schema: index.schema(),
        meta,
    }
}

impl Segment {
    /// Returns our index's schema.
    // TODO return a ref.
    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }

    /// Returns the segment meta-information
    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    /// Updates the max_doc value from the `SegmentMeta`.
    ///
    /// This method is only used when updating `max_doc` from 0
    /// as we finalize a fresh new segment.
    pub(crate) fn with_max_doc(self, max_doc: u32) -> Segment {
        Segment {
            directory: self.directory,
            schema: self.schema,
            meta: self.meta.with_max_doc(max_doc),
        }
    }

    #[doc(hidden)]
    pub fn with_delete_meta(self, num_deleted_docs: u32, opstamp: Opstamp) -> Segment {
        Segment {
            directory: self.directory,
            schema: self.schema,
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
    pub fn open_read(&self, component: SegmentComponent) -> Result<ReadOnlySource, OpenReadError> {
        let path = self.relative_path(component);
        let source = self.directory.open_read(&path)?;
        Ok(source)
    }

    /// Open one of the component file for *regular* write.
    pub fn open_write(&mut self, component: SegmentComponent) -> Result<WritePtr, OpenWriteError> {
        let path = self.relative_path(component);
        let write = self.directory.open_write(&path)?;
        Ok(write)
    }
}

pub trait SerializableSegment {
    /// Writes a view of a segment by pushing information
    /// to the `SegmentSerializer`.
    ///
    /// # Returns
    /// The number of documents in the segment.
    fn write(&self, serializer: SegmentSerializer) -> crate::Result<u32>;
}
