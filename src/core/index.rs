use Result;
use Error;
use std::path::{PathBuf, Path};
use schema::Schema;
use DocId;
use std::io::Write;
use std::sync::{Arc, RwLock, RwLockWriteGuard, RwLockReadGuard};
use std::fmt;
use rustc_serialize::json;
use core::SegmentId;
use directory::{Directory, MmapDirectory, RAMDirectory, ReadOnlySource, WritePtr};
use core::writer::IndexWriter;
use core::searcher::Searcher;
use std::convert::From;
use super::SegmentComponent;

use num_cpus;
use core::segment_serializer::SegmentSerializer;

#[derive(Clone,Debug,RustcDecodable,RustcEncodable)]
pub struct IndexMeta {
    segments: Vec<SegmentId>,
    schema: Schema,
}

impl IndexMeta {
    fn with_schema(schema: Schema) -> IndexMeta {
        IndexMeta {
            segments: Vec::new(),
            schema: schema,
        }
    }

    fn segment_ordinal(&self, segment_id: SegmentId) -> Option<usize> {
        self.segments
            .iter()
            .position(|&el| el == segment_id)
    }
}

impl fmt::Debug for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
       write!(f, "Index({:?})", self.directory)
   }
}

type DirectoryPtr = Box<Directory>;

#[derive(Clone)]
pub struct Index {
    metas: Arc<RwLock<IndexMeta>>,
    directory: Arc<RwLock<DirectoryPtr>>,
}

lazy_static! {
    static ref  META_FILEPATH: PathBuf = PathBuf::from("meta.json");
}

impl Index {

    pub fn create_in_ram(schema: Schema) -> Index {
        let directory = Box::new(RAMDirectory::create());
        Index::from_directory(directory, schema)
    }

    pub fn create(directory_path: &Path, schema: Schema) -> Result<Index> {
        let directory = Box::new(try!(MmapDirectory::open(directory_path)));
        Ok(Index::from_directory(directory, schema))
    }

    pub fn create_from_tempdir(schema: Schema) -> Result<Index> {
        let directory = Box::new(try!(MmapDirectory::create_from_tempdir()));
        Ok(Index::from_directory(directory, schema))
    }

    pub fn open(directory_path: &Path) -> Result<Index> {
        let directory = try!(MmapDirectory::open(directory_path));
        let directory_ptr = Box::new(directory);
        let mut index = Index::from_directory(directory_ptr, Schema::new());
        try!(index.load_metas()); //< TODO does the directory already exists?
        Ok(index)
    }
    
    /// Creates a multithreaded writer.
    /// Each writer produces an independant segment.
    pub fn writer_with_num_threads(&self, num_threads: usize) -> Result<IndexWriter> {
        IndexWriter::open(self, num_threads)
    }
    
    
    /// Creates a multithreaded writer
    /// It just calls `writer_with_num_threads` with the number of core as `num_threads` 
    pub fn writer(&self,) -> Result<IndexWriter> {
        self.writer_with_num_threads(num_cpus::get())
    }

    pub fn searcher(&self,) -> Result<Searcher> {
        Searcher::for_index(self.clone())
    }

    fn from_directory(directory: DirectoryPtr, schema: Schema) -> Index {
        Index {
            metas: Arc::new(RwLock::new(IndexMeta::with_schema(schema))),
            directory: Arc::new(RwLock::new(directory)),
        }
    }

    pub fn schema(&self,) -> Schema {
        self.metas.read().unwrap().schema.clone()
    }

    fn rw_directory(&mut self) -> Result<RwLockWriteGuard<DirectoryPtr>> {
        self.directory
            .write()
            .map_err(From::from)
    }

    fn ro_directory(&self) -> Result<RwLockReadGuard<DirectoryPtr>> {
        self.directory
            .read()
            .map_err(From::from)
    }


    /// Marks the segment as published.
    // TODO find a rusty way to hide that, while keeping
    // it visible for IndexWriters.
    pub fn publish_segment(&mut self, segment: &Segment) -> Result<()> {
        {
            let mut meta_write = self.metas.write().unwrap();
            meta_write.segments.push(segment.segment_id.clone());
        }
        self.save_metas()
    }

    pub fn publish_merge_segment(&mut self, segments: &Vec<Segment>, merged_segment: &Segment) -> Result<()> {
        {
            let mut meta_write = self.metas.write().unwrap();
            for segment in segments {
                let segment_pos = meta_write.segment_ordinal(segment.id());
                match segment_pos {
                    Some(pos) => {
                        meta_write.segments.remove(pos);
                    }
                    None => {
                        panic!("Segment");
                    }
                }
            }
            meta_write.segments.push(merged_segment.id());
        }
        // TODO use logs
        self.save_metas()
    }

    pub fn sync(&mut self, segment: &Segment) -> Result<()> {
        let directory = try!(self.ro_directory());
        for component in SegmentComponent::values() {
            let path = segment.relative_path(component);
            try!(directory.sync(&path));
        }
        try!(self.ro_directory()).sync_directory()
    }

    pub fn segments(&self,) -> Vec<Segment> {
        self.segment_ids()
            .into_iter()
            .map(|segment_id| self.segment(segment_id))
            .collect()
    }

    pub fn segment(&self, segment_id: SegmentId) -> Segment {
        Segment {
            index: self.clone(),
            segment_id: segment_id
        }
    }

    fn segment_ids(&self,) -> Vec<SegmentId> {
        self.metas
            .read()
            .unwrap()
            .segments
            .iter()
            .cloned()
            .collect()
    }

    pub fn new_segment(&self,) -> Segment {
        self.segment(SegmentId::new())
    }

    pub fn load_metas(&mut self,) -> Result<()> {
        let ro_dir = try!(self.ro_directory());
        let meta_file = try!(ro_dir.open_read(&META_FILEPATH));
        let meta_content = String::from_utf8_lossy(meta_file.as_slice());
        let loaded_meta: IndexMeta = json::decode(&meta_content).unwrap();
        self.metas.write().unwrap().clone_from(&loaded_meta);
        Ok(())
    }

    pub fn save_metas(&mut self,) -> Result<()> {
        let mut w = Vec::new();
        {
            let metas_lock = self.metas.read().unwrap() ;
            try!(write!(&mut w, "{}\n", json::as_pretty_json(&*metas_lock)));
        };
        try!(self.rw_directory())
            .atomic_write(&META_FILEPATH, &w[..])
    }
}



/////////////////////////
// Segment

#[derive(Clone,Debug,RustcDecodable,RustcEncodable)]
pub struct SegmentInfo {
	pub max_doc: DocId,
}



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
        let directory_lock = self.index.directory.read();
        match directory_lock {
            Ok(directory) => {
                directory.open_read(&path)
                         .map_err(From::from)
            }
            Err(_) => {
                Err(Error::Poisoned)
            }
        } 
    }

    pub fn open_write(&self, component: SegmentComponent) -> Result<WritePtr> {
        let path = self.relative_path(component);
        self.index.directory.write().unwrap().open_write(&path)
    }
}

pub trait SerializableSegment {
    fn write(&self, serializer: SegmentSerializer) -> Result<()>;
}
