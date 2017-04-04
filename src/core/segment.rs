use Result;
use std::path::PathBuf;
use schema::Schema;
use DocId;
use std::fmt;
use core::SegmentId;
use directory::{ReadOnlySource, WritePtr, FileProtection};
use indexer::segment_serializer::SegmentSerializer;
use super::SegmentComponent;
use core::Index;
use std::result;
use directory::Directory;
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
    pub fn open_read(&self, component: SegmentComponent) -> result::Result<ReadOnlySource, FileError> {
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

#[derive(Clone,Debug,RustcDecodable,RustcEncodable)]
pub struct SegmentInfo {
	pub max_doc: DocId,
}

#[cfg(test)]
mod tests {

    use core::SegmentComponent;
    use std::path::Path;
    use directory::Directory;
    use schema::{SchemaBuilder, Document, FieldValue, TEXT, Term};
    use Index;

    #[test]
    fn test_segment_protect_component() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
        
        {
            // simply creating two segments
            // with one delete to create the DELETE file.
            {
                let doc1 = doc!(text_field=>"a");
                index_writer.add_document(doc1);
                let doc2 = doc!(text_field=>"b");
                index_writer.add_document(doc2);
                assert!(index_writer.commit().is_ok());
            }
            {
                index_writer.delete_term(Term::from_field_text(text_field, "a"));
                assert!(index_writer.commit().is_ok());
            }
        }

        let segments = index.searchable_segments().unwrap();
        let directory = index.directory().clone();
        assert_eq!(segments.len(), 1);

        
        let delete_file_path = Path::new("00000000000000000000000000000000.4.del");
        let idx_file_path = Path::new("00000000000000000000000000000000.term");
        assert!(directory.exists(&*delete_file_path));
        assert!(directory.exists(&*idx_file_path));

        {
            let _file_protection = segments[0].protect_from_delete(SegmentComponent::DELETE);
            index_writer.delete_term(Term::from_field_text(text_field, "b"));
            index_writer.commit().unwrap();
            // the delete file is protected, and should not be gc'ed.
            assert!(directory.exists(&*delete_file_path));
        }

        index_writer.commit().unwrap();
        
        // at this point the protection is released.
        assert!(!directory.exists(&*delete_file_path));

    }
        

}