use Result;
use std::path::PathBuf;
use schema::Schema;
use std::fmt;
use core::SegmentId;
use directory::{ReadOnlySource, WritePtr, FileProtection};
use indexer::segment_serializer::SegmentSerializer;
use super::SegmentComponent;
use core::Index;
use std::result;
use directory::Directory;
use core::SegmentMeta;
use directory::error::{OpenReadError, OpenWriteError};

/// A segment is a piece of the index.
#[derive(Clone)]
pub struct Segment {
    index: Index,
    meta: SegmentMeta,
}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Segment({:?})", self.id().uuid_string())
    }
}

/// Creates a new segment given an `Index` and a `SegmentId`
/// 
/// The function is here to make it private outside `tantivy`. 
pub fn create_segment(index: Index, meta: SegmentMeta) -> Segment {
    Segment {
        index: index,
        meta: meta,
    }
}

impl Segment {
    
    /// Returns our index's schema.
    pub fn schema(&self,) -> Schema {
        self.index.schema()
    }

    /// Returns the segment meta-information
    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    #[doc(hidden)]
    pub fn set_delete_meta(&mut self, num_deleted_docs: u32, opstamp: u64) {
        self.meta.set_delete_meta(num_deleted_docs, opstamp);
    }

    /// Returns the segment's id.
    pub fn id(&self,) -> SegmentId {
        self.meta.id()
    }

    /// Returns the relative path of a component of our segment.
    ///  
    /// It just joins the segment id with the extension 
    /// associated to a segment component.
    pub fn relative_path(&self, component: SegmentComponent) -> PathBuf {
        self.meta.relative_path(component)
    }


    /// Protects a specific component file from being deleted.
    ///
    /// Returns a FileProtection object. The file is guaranteed
    /// to not be garbage collected as long as this `FileProtection`  object
    /// lives.
    pub fn protect_from_delete(&self, component: SegmentComponent) -> FileProtection {
        let path = self.relative_path(component);
        self.index.directory().protect_file_from_delete(&path)
    }

    /// Open one of the component file for a *regular* read.
    pub fn open_read(&self, component: SegmentComponent) -> result::Result<ReadOnlySource, OpenReadError> {
        let path = self.relative_path(component);
        let source = try!(self.index.directory().open_read(&path));
        Ok(source)
    }

    /// Open one of the component file for *regular* write.
    pub fn open_write(&mut self, component: SegmentComponent) -> result::Result<WritePtr, OpenWriteError> {
        let path = self.relative_path(component);
        let write = try!(self.index.directory_mut().open_write(&path));
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

#[cfg(test)]
mod tests {

    use core::SegmentComponent;
    use directory::Directory;
    use std::collections::HashSet;
    use schema::SchemaBuilder;
    use Index;

    #[test]
    fn test_segment_protect_component() {
        let mut index = Index::create_in_ram(SchemaBuilder::new().build());
        let segment = index.new_segment();
        let path = segment.relative_path(SegmentComponent::POSTINGS);
        
        let directory = index.directory_mut();
        directory.atomic_write(&*path, &vec!(0u8)).unwrap();
        
        let living_files = HashSet::new();
        {
            let _file_protection = segment.protect_from_delete(SegmentComponent::POSTINGS);
            assert!(directory.exists(&*path));
            directory.garbage_collect(living_files.clone());
            assert!(directory.exists(&*path));
        }

        directory.garbage_collect(living_files);
        assert!(!directory.exists(&*path));
    }

}