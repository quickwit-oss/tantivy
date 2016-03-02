
use std::path::{PathBuf, Path};
use std::collections::HashMap;
use std::fs::File;
use std::fs;
use core::schema::Schema;
use std::collections::hash_map::Entry as HashMapEntry;
use std::io::Write;
use std::borrow::BorrowMut;
use std::sync::{Arc, RwLock, RwLockWriteGuard, RwLockReadGuard};
use std::fmt;
use std::cell::RefCell;
use rand::{thread_rng, Rng};
use fst::raw::MmapReadOnly;
use rustc_serialize::json;
use atomicwrites;
use tempdir::TempDir;
use std::io::Read;
use std::io::Error as IOError;
use std::io::ErrorKind as IOErrorKind;

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
       write!(f, "Index({:?})", self.inner_index.read().unwrap().index_path)
   }
}

fn open_mmap(full_path: &PathBuf) -> Result<MmapReadOnly, IOError> {
    MmapReadOnly::open_path(full_path.clone())
}

fn sync_file(filepath: &PathBuf) -> Result<(), IOError> {
    File::open(filepath.clone())
        .and_then(|fd| fd.sync_all())
}


#[derive(Clone)]
pub struct Index {
    metas: Arc<RwLock<IndexMeta>>,
    inner_index: Arc<RwLock<InnerIndex>>,
}

pub enum CreateError {
    RootIndexDoesNotExist,
    IndexAlreadyExists,
    CannotOpenMetaFile,
}

struct IndexError;

lazy_static! {
    static ref  META_FILEPATH: PathBuf = PathBuf::from("meta.json");
}

impl Index {

    pub fn create(filepath: &Path, schema: Schema) -> Result<Index, CreateError> {
        let inner_index = try!(InnerIndex::create(filepath));
        Ok(Index::from_inner_index(inner_index, schema))
    }

    pub fn create_from_tempdir(schema: Schema) -> Result<Index, IOError> {
        let inner_index = try!(InnerIndex::create_from_tempdir());
        Ok(Index::from_inner_index(inner_index, schema))
    }

    pub fn open<P: AsRef<Path>>(filepath: &P) -> Result<Index, IOError> {
        let inner_index = try!(InnerIndex::open(filepath));
        let mut index = Index::from_inner_index(inner_index, Schema::new());
        try!(index.load_metas()); //< does the directory already exists?
        Ok(index)
    }

    pub fn schema(&self,) -> Schema {
        self.metas.read().unwrap().schema.clone()
    }

    fn get_write(&mut self) -> Result<RwLockWriteGuard<InnerIndex>, IOError> {
        self.inner_index
            .write()
            .map_err(|e| IOError::new(IOErrorKind::Other,
                format!("Failed acquiring lock on directory.\n
                It can happen if another thread panicked! Error was: {:?}", e) ))
    }

    fn get_read(&self) -> Result<RwLockReadGuard<InnerIndex>, IOError> {
        self.inner_index
            .read()
            .map_err(|e| IOError::new(IOErrorKind::Other,
                format!("Failed acquiring lock on directory.\n
                It can happen if another thread panicked! Error was: {:?}", e) ))
    }

    fn from_inner_index(inner_index: InnerIndex, schema: Schema) -> Index {
        Index {
            metas: Arc::new(RwLock::new(IndexMeta::with_schema(schema))),
            inner_index: Arc::new(RwLock::new(inner_index)),
        }
    }

    // TODO find a rusty way to hide that, while keeping
    // it visible for IndexWriters.
    pub fn publish_segment(&mut self, segment: Segment) -> Result<(), IOError> {
        println!("publish segment {:?}", segment);
        self.metas.write().unwrap().segments.push(segment.segment_id.0.clone());
        // TODO use logs
        self.save_metas()
    }

    pub fn sync(&mut self, segment: Segment) -> Result<(), IOError> {
        try!(self.get_write()).sync(segment)
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


    pub fn load_metas(&mut self,) -> Result<(), IOError> {
        let meta_file = try!(self.inner_index.read().unwrap().mmap(&META_FILEPATH));
        let meta_content = String::from_utf8_lossy(unsafe {meta_file.as_slice()});
        let loaded_meta: IndexMeta = json::decode(&meta_content).unwrap();
        self.metas.write().unwrap().clone_from(&loaded_meta);
        Ok(())
    }

    pub fn save_metas(&self,) -> Result<(), IOError> {
        let metas_lock = self.metas.read().unwrap();
        let encoded = json::encode(&*metas_lock).unwrap();
        self.inner_index.write().unwrap().write_atomic(&META_FILEPATH, encoded)
    }
}


struct InnerIndex {
    index_path: PathBuf,
    mmap_cache: RefCell<HashMap<PathBuf, MmapReadOnly>>,
    _temp_directory: Option<TempDir>,
}


fn create_tempdir() -> Result<TempDir, IOError> {
    TempDir::new("index")
}


impl InnerIndex {



    pub fn write_atomic(&self, path: &PathBuf, data: String) -> Result<(), IOError> {
        let meta_file = atomicwrites::AtomicFile::new(path, atomicwrites::AllowOverwrite);
        meta_file.write(|f| {
            f.write_all(data.as_bytes())
        })
    }

    pub fn create<P: AsRef<Path>>(filepath: P) -> Result<InnerIndex, CreateError> {
        let filepath_os_path = filepath.as_ref().as_os_str();
        let directory = InnerIndex {
            index_path: PathBuf::from(&filepath_os_path),
            mmap_cache: RefCell::new(HashMap::new()),
            _temp_directory: None,
        };
        Ok(directory)
    }

    pub fn create_from_tempdir() -> Result<InnerIndex, IOError> {
        let tempdir = try!(create_tempdir());
        let tempdir_path = PathBuf::from(tempdir.path());
        let directory = InnerIndex {
            index_path: PathBuf::from(tempdir_path),
            mmap_cache: RefCell::new(HashMap::new()),
            _temp_directory: Some(tempdir)
        };
        Ok(directory)
    }

    pub fn open<P: AsRef<Path>>(filepath: &P) -> Result<InnerIndex, IOError> {
        let directory = InnerIndex {
            index_path: PathBuf::from(filepath.as_ref().as_os_str()),
            mmap_cache: RefCell::new(HashMap::new()),
            _temp_directory: None,
        };
        Ok(directory)
    }

    pub fn sync(&mut self, segment: Segment) -> Result<(), IOError> {
        for component in [SegmentComponent::POSTINGS, SegmentComponent::TERMS].iter() {
            let relative_path = segment.relative_path(component);
            let full_path = self.resolve_path(&relative_path);
            try!(sync_file(&full_path));
        }
        // syncing the directory itself
        sync_file(&self.index_path)
    }

    fn resolve_path(&self, relative_path: &PathBuf) -> PathBuf {
        self.index_path.join(relative_path)
    }

    fn open_writable(&self, relative_path: &PathBuf) -> Result<File, IOError> {
        let full_path = self.resolve_path(relative_path);
        File::create(full_path.clone())
    }

    fn mmap(&self, relative_path: &PathBuf) -> Result<MmapReadOnly, IOError> {
        let full_path = self.resolve_path(relative_path);
        let mut mmap_cache = self.mmap_cache.borrow_mut();
        Ok(match mmap_cache.entry(full_path.clone()) {
            HashMapEntry::Occupied(e) => e.get().clone(),
            HashMapEntry::Vacant(vacant_entry) => {
                let new_mmap =  try!(open_mmap(&full_path));
                vacant_entry.insert(new_mmap.clone());
                new_mmap
            }
        })
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
            SegmentComponent::POSTINGS => ".idx",
            // SegmentComponent::POSITIONS => ".pos",
            SegmentComponent::TERMS => ".term",
            SegmentComponent::STORE => ".store",
        }
    }

    pub fn relative_path(&self, component: &SegmentComponent) -> PathBuf {
        let SegmentId(ref segment_id_str) = self.segment_id;
        let filename = String::new() + segment_id_str + Segment::path_suffix(component);
        PathBuf::from(filename)
    }

    pub fn mmap(&self, component: SegmentComponent) -> Result<MmapReadOnly, IOError> {
        let path = self.relative_path(&component);
        self.index.inner_index.read().unwrap().mmap(&path)
    }

    pub fn open_writable(&self, component: SegmentComponent) -> Result<File, IOError> {
        let path = self.relative_path(&component);
        self.index.inner_index.write().unwrap().open_writable(&path)
    }
}
