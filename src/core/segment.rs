use super::SegmentComponent;
use crate::core::Index;
use crate::core::SegmentId;
use crate::core::SegmentMeta;
use crate::directory::error::{OpenReadError, OpenWriteError};
use crate::directory::{Directory, ManagedDirectory, RAMDirectory};
use crate::directory::{ReadOnlySource, WritePtr};
use crate::indexer::segment_serializer::SegmentSerializer;
use crate::schema::Schema;
use crate::Opstamp;
use std::fmt;
use std::ops::Deref;
use std::path::PathBuf;

#[derive(Clone)]
pub(crate) enum SegmentDirectory {
    Persisted(ManagedDirectory),
    Volatile(RAMDirectory),
}

impl SegmentDirectory {
    pub fn new_volatile() -> SegmentDirectory {
        SegmentDirectory::Volatile(RAMDirectory::default())
    }
}

impl From<ManagedDirectory> for SegmentDirectory {
    fn from(directory: ManagedDirectory) -> Self {
        SegmentDirectory::Persisted(directory)
    }
}

impl Deref for SegmentDirectory {
    type Target = dyn Directory;

    fn deref(&self) -> &Self::Target {
        match self {
            SegmentDirectory::Volatile(dir) => dir,
            SegmentDirectory::Persisted(dir) => dir,
        }
    }
}

impl DerefMut for SegmentDirectory {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            SegmentDirectory::Volatile(dir) => dir,
            SegmentDirectory::Persisted(dir) => dir,
        }
    }
}

/// A segment is a piece of the index.
#[derive(Clone)]
pub struct Segment {
    schema: Schema,
    meta: SegmentMeta,
    directory: SegmentDirectory,
}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Segment({:?})", self.id().uuid_string())
    }
}

impl Segment {
    /// Returns our index's schema.
    // TODO return a ref.
    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }

    pub(crate) fn new_persisted(
        meta: SegmentMeta,
        directory: ManagedDirectory,
        schema: Schema,
    ) -> Segment {
        Segment {
            meta,
            schema,
            directory: SegmentDirectory::from(directory),
        }
    }

    /// Creates a new segment that embeds its own `RAMDirectory`.
    ///
    /// That segment is entirely dissociated from the index directory.
    /// It will be persisted by a background thread in charge of IO.
    pub fn new_volatile(meta: SegmentMeta, schema: Schema) -> Segment {
        Segment {
            schema,
            meta,
            directory: SegmentDirectory::new_volatile(),
        }
    }

    /// Creates a new segment given an `Index` and a `SegmentId`
    pub(crate) fn for_index(index: Index, meta: SegmentMeta) -> Segment {
        Segment {
            directory: SegmentDirectory::Persisted(index.directory().clone()),
            schema: index.schema(),
            meta,
        }
    }

    pub fn persist(&mut self, mut dest_directory: ManagedDirectory) -> crate::Result<()> {
        if let SegmentDirectory::Persisted(_) = self.directory {
            // this segment is already persisted.
            return Ok(());
        }
        if let SegmentDirectory::Volatile(ram_directory) = &self.directory {
            ram_directory.persist(&mut dest_directory)?;
        }
        self.directory = SegmentDirectory::Persisted(dest_directory);
        Ok(())
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
