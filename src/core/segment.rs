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
use directory::error::{FileError, OpenWriteError};



/// A segment is a piece of the index.
#[derive(Clone)]
pub struct Segment {
    index: Index,
    segment_id: SegmentId,
}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Segment({:?})", self.segment_id.uuid_string())
    }
}


/// Creates a new segment given an `Index` and a `SegmentId`
/// 
/// The function is here to make it private outside `tantivy`. 
pub fn create_segment(index: Index, segment_id: SegmentId) -> Segment {
    Segment {
        index: index,
        segment_id: segment_id,
    }
}

impl Segment {


    /// Returns our index's schema.
    pub fn schema(&self,) -> Schema {
        self.index.schema()
    }

    /// Returns the segment's id.
    pub fn id(&self,) -> SegmentId {
        self.segment_id
    }
    

    /// Returns the relative path of a component of our segment.
    ///  
    /// It just joins the segment id with the extension 
    /// associated to a segment component.
    pub fn relative_path(&self, component: SegmentComponent) -> PathBuf {
        self.segment_id.relative_path(component)
    }

    /// Deletes all of the document of the segment.
    /// This is called when there is a merge or a rollback.
    ///
    /// # Disclaimer
    /// If deletion of a file fails (e.g. a file 
    /// was read-only.), the method does not
    /// fail and just logs an error when it fails.
    pub fn delete(&self) {
        info!("Deleting segment {:?}", self.segment_id);
        let segment_filepaths_res = self.index.directory().ls_starting_with(
            &*self.segment_id.uuid_string()
        );

        match segment_filepaths_res {
            Ok(segment_filepaths) => {
                for segment_filepath in &segment_filepaths {
                    if let Err(err) = self.index.directory().delete(&segment_filepath) {
                        match err {
                            FileError::FileDoesNotExist(_) => {
                                // this is normal behavior.
                                // the position file for instance may not exists.
                            }
                            FileError::IOError(err) => {
                                error!("Failed to remove {:?} : {:?}", self.segment_id, err);
                            }
                        }
                    }
                }
            }
            Err(_) => {
                error!("Failed to list files of segment {:?} for deletion.", self.segment_id.uuid_string());
            }
        }
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