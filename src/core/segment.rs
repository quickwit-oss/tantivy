use Result;
use std::path::PathBuf;
use schema::Schema;
use DocId;
use std::fmt;
use core::SegmentId;
use directory::{ReadOnlySource, WritePtr};
use indexer::segment_serializer::SegmentSerializer;
use super::SegmentComponent;
use core::Index;
use std::result;
use core::SegmentMeta;
use directory::error::{FileError, OpenWriteError};

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

    pub fn meta(&self,) -> &SegmentMeta {
        &self.meta
    }

    /// Returns the segment's id.
    pub fn id(&self,) -> SegmentId {
        self.meta.segment_id
    }

    pub fn with_delete_opstamp(self, opstamp: u64) -> Segment {
        let mut meta = self.meta;
        meta.delete_opstamp = Some(opstamp);
        Segment {
            index: self.index,
            meta: meta,
        }
    }

    /// Returns the relative path of a component of our segment.
    ///  
    /// It just joins the segment id with the extension 
    /// associated to a segment component.
    pub fn relative_path(&self, component: SegmentComponent) -> PathBuf {
        use self::SegmentComponent::*;
        let mut path = self.id().uuid_string();
        path.push_str(&*match component {
            POSITIONS => ".pos".to_string(),
            INFO => ".info".to_string(),
            POSTINGS => ".idx".to_string(),
            TERMS => ".term".to_string(),
            STORE => ".store".to_string(),
            FASTFIELDS => ".fast".to_string(),
            FIELDNORMS => ".fieldnorm".to_string(),
            DELETE => {format!(".{}.del", self.meta.delete_opstamp.unwrap_or(0))},
        });
        PathBuf::from(path)
    }

    /// Open one of the component file for read.
    pub fn open_read(&self, component: SegmentComponent) -> result::Result<ReadOnlySource, FileError> {
        let path = self.relative_path(component);
        let source = try!(self.index.directory().open_read(&path));
        Ok(source)
    }

    /// Open one of the component file for write.
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

#[derive(Clone,Debug,RustcDecodable,RustcEncodable)]
pub struct SegmentInfo {
	pub max_doc: DocId,
}