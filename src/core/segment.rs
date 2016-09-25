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

impl Segment {

    pub fn new(index: Index, segment_id: SegmentId) -> Segment {
        Segment {
            index: index,
            segment_id: segment_id,
        }
    }

    pub fn schema(&self,) -> Schema {
        self.index.schema()
    }

    pub fn id(&self,) -> SegmentId {
        self.segment_id
    }
    
    pub fn relative_path(&self, component: SegmentComponent) -> PathBuf {
        self.segment_id.relative_path(component)
    }

    pub fn open_read(&self, component: SegmentComponent) -> Result<ReadOnlySource> {
        let path = self.relative_path(component);
        let source = try!(self.index.directory().open_read(&path));
        Ok(source)
    }

    pub fn open_write(&mut self, component: SegmentComponent) -> Result<WritePtr> {
        let path = self.relative_path(component);
        let write = try!(self.index.directory_mut().open_write(&path));
        Ok(write)
    }
}

pub trait SerializableSegment {
    fn write(&self, serializer: SegmentSerializer) -> Result<usize>;
}

#[derive(Clone,Debug,RustcDecodable,RustcEncodable)]
pub struct SegmentInfo {
	pub max_doc: DocId,
}