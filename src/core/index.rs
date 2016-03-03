use std::path::{PathBuf, Path};
use std::io;
use core::schema::Schema;
use std::io::Write;
use std::sync::{Arc, RwLock, RwLockWriteGuard, RwLockReadGuard};
use std::fmt;
use rand::{thread_rng, Rng};
use rustc_serialize::json;
use std::io::Read;
use std::io::ErrorKind as IOErrorKind;
use core::directory::{Directory, MmapDirectory, RAMDirectory, ReadOnlySource, WritePtr};
use core::writer::IndexWriter;
use core::searcher::Searcher;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SegmentId(pub String);


pub fn generate_segment_name() -> SegmentId {
    static CHARS: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let random_name: String = (0..8)
            .map(|_| thread_rng().choose(CHARS).unwrap().clone() as char)
            .collect();
    SegmentId( String::from("_") + &random_name)
}

#[derive(Clone,Debug,RustcDecodable,RustcEncodable)]
pub struct IndexMeta {
    segments: Vec<String>,
    schema: Schema,
}

impl IndexMeta {
    fn new() -> IndexMeta {
        IndexMeta {
            segments: Vec::new(),
            schema: Schema::new(),
        }
    }
    fn with_schema(schema: Schema) -> IndexMeta {
        IndexMeta {
            segments: Vec::new(),
            schema: schema,
        }
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

fn could_not_acquire_lock<E>(_: E) -> io::Error {
    io::Error::new(IOErrorKind::Other, "Could not acquire read lock on directory")
}

lazy_static! {
    static ref  META_FILEPATH: PathBuf = PathBuf::from("meta.json");
}

impl Index {

    pub fn create_in_ram(schema: Schema) -> Index {
        let directory = Box::new(RAMDirectory::create());
        Index::from_directory(directory, schema)
    }

    pub fn create(directory_path: &Path, schema: Schema) -> io::Result<Index> {
        let directory = Box::new(try!(MmapDirectory::create(directory_path)));
        Ok(Index::from_directory(directory, schema))
    }

    pub fn create_from_tempdir(schema: Schema) -> io::Result<Index> {
        let directory = Box::new(try!(MmapDirectory::create_from_tempdir()));
        Ok(Index::from_directory(directory, schema))
    }

    pub fn open(directory_path: &Path) -> io::Result<Index> {
        let directory = try!(MmapDirectory::create(directory_path));
        let directory_ptr = Box::new(directory);
        let mut index = Index::from_directory(directory_ptr, Schema::new());
        try!(index.load_metas()); //< does the directory already exists?
        Ok(index)
    }

    pub fn writer(&self,) -> IndexWriter {
        IndexWriter::open(self,)
    }

    pub fn searcher(&self,) -> Searcher {
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

    fn rw_directory(&mut self) -> io::Result<RwLockWriteGuard<DirectoryPtr>> {
        self.directory
            .write()
            .map_err(|e| io::Error::new(IOErrorKind::Other,
                format!("Failed acquiring lock on directory.\n
                It can happen if another thread panicked! Error was: {:?}", e) ))
    }

    fn ro_directory(&self) -> io::Result<RwLockReadGuard<DirectoryPtr>> {
        self.directory
            .read()
            .map_err(|e| io::Error::new(IOErrorKind::Other,
                format!("Failed acquiring lock on directory.\n
                It can happen if another thread panicked! Error was: {:?}", e) ))
    }


    // TODO find a rusty way to hide that, while keeping
    // it visible for IndexWriters.
    pub fn publish_segment(&mut self, segment: Segment) -> io::Result<()> {
        self.metas.write().unwrap().segments.push(segment.segment_id.0.clone());
        // TODO use logs
        self.save_metas()
    }

    pub fn sync(&mut self, segment: Segment) -> io::Result<()> {
        for component in [SegmentComponent::POSTINGS, SegmentComponent::TERMS].iter() {
            let path = segment.relative_path(component);
            let directory = try!(self.ro_directory());
            try!(directory.sync(&path));
        }
        try!(self.ro_directory()).sync_directory()
    }

    pub fn segments(&self,) -> Vec<Segment> {
        // TODO handle error
        self.segment_ids()
            .into_iter()
            .map(|segment_id| self.segment(&segment_id))
            .collect()
    }

    pub fn segment(&self, segment_id: &SegmentId) -> Segment {
        Segment {
            index: self.clone(),
            segment_id: segment_id.clone()
        }
    }

    fn segment_ids(&self,) -> Vec<SegmentId> {
        self.metas
            .read()
            .unwrap()
            .segments
            .iter()
            .cloned()
            .map(SegmentId)
            .collect()
    }

    pub fn new_segment(&self,) -> Segment {
        // TODO check it does not exists
        self.segment(&generate_segment_name())
    }

    pub fn load_metas(&mut self,) -> io::Result<()> {
        let meta_file = try!(self.ro_directory().and_then(|d| d.open_read(&META_FILEPATH)));
        let meta_content = String::from_utf8_lossy(meta_file.as_slice());
        let loaded_meta: IndexMeta = json::decode(&meta_content).unwrap();
        self.metas.write().unwrap().clone_from(&loaded_meta);
        Ok(())
    }

    pub fn save_metas(&self,) -> io::Result<()> {
        let metas_lock = self.metas.read().unwrap();
        let encoded = json::encode(&*metas_lock).unwrap();
        try!(self.directory
            .write()
            .map_err(could_not_acquire_lock)
        ).atomic_write(&META_FILEPATH, encoded.as_bytes())
    }
}


/////////////////////////
// Segment

pub enum SegmentComponent {
    POSTINGS,
    // POSITIONS,
    TERMS,
    STORE,
}

#[derive(Debug, Clone)]
pub struct Segment {
    index: Index,
    segment_id: SegmentId,
}

impl Segment {

    pub fn id(&self,) -> SegmentId {
        self.segment_id.clone()
    }

    fn path_suffix(component: &SegmentComponent)-> &'static str {
        match *component {
            // SegmentComponent::POSITIONS => ".pos",
            SegmentComponent::POSTINGS => ".idx",
            SegmentComponent::TERMS => ".term",
            SegmentComponent::STORE => ".store",
        }
    }

    pub fn relative_path(&self, component: &SegmentComponent) -> PathBuf {
        let SegmentId(ref segment_id_str) = self.segment_id;
        let filename = String::new() + segment_id_str + Segment::path_suffix(component);
        PathBuf::from(filename)
    }

    pub fn open_read(&self, component: SegmentComponent) -> io::Result<ReadOnlySource> {
        let path = self.relative_path(&component);
        self.index.directory.read().unwrap().open_read(&path)
    }

    pub fn open_write(&self, component: SegmentComponent) -> io::Result<WritePtr> {
        let path = self.relative_path(&component);
        self.index.directory.write().unwrap().open_write(&path)
    }
}


#[cfg(test)]
mod test {

    use super::*;
    use regex::Regex;

    #[test]
    fn test_new_segment() {
        let SegmentId(segment_name) = generate_segment_name();
        let segment_ptn = Regex::new(r"^_[a-z0-9]{8}$").unwrap();
        assert!(segment_ptn.is_match(&segment_name));
    }

}
