
use std::path::{PathBuf, Path};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs::File;
use std::io::Write;
use std::io::BufWriter;
use std::io;
use std::borrow::Borrow;
use std::borrow::BorrowMut;
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock, MutexGuard, RwLockWriteGuard, RwLockReadGuard};
use std::fmt;
use std::ops::Deref;
use std::cell::RefCell;
use core::error::*;
use rand::{thread_rng, Rng};
use fst::raw::MmapReadOnly;
use rustc_serialize::json;
use atomicwrites;
use tempdir::TempDir;

#[derive(Clone, Debug)]
pub struct SegmentId(pub String);

pub fn generate_segment_name() -> SegmentId {
    static CHARS: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let random_name: String = (0..8)
            .map(|_| thread_rng().choose(CHARS).unwrap().clone() as char)
            .collect();
    SegmentId( String::from("_") + &random_name)
}

// #[derive()]
#[derive(Clone,Debug,RustcDecodable, RustcEncodable)]
pub struct DirectoryMeta {
    segments: Vec<String>
}

impl DirectoryMeta {
    fn new() -> DirectoryMeta {
        DirectoryMeta {
            segments: Vec::new()
        }
    }
}


impl fmt::Debug for Directory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
       write!(f, "Directory({:?})", self.inner_directory.read().unwrap().index_path)
   }
}

fn open_mmap(full_path: &PathBuf) -> Result<MmapReadOnly> {
    match MmapReadOnly::open_path(full_path.clone()) {
        Ok(mmapped_file) => Ok(mmapped_file),
        Err(ioerr) => {
            // TODO add file
            let error_msg = format!("Read-Only MMap of {:?} failed", full_path);
            return Err(Error::IOError(ioerr.kind(), error_msg));
        }
    }
}

fn sync_file(filepath: &PathBuf) -> Result<()> {
    match File::open(filepath.clone()) {
        Ok(fd) => {
            match fd.sync_all() {
                Err(err) => Err(Error::IOError(err.kind(), format!("Failed to sync {:?}", filepath))),
                _ => Ok(())
            }
        },
        Err(err) => Err(Error::IOError(err.kind(), format!("Cause: {:?}", err)))
    }
}


#[derive(Clone)]
pub struct Directory {
    inner_directory: Arc<RwLock<InnerDirectory>>,
}


impl Directory {

    fn get_write(&mut self) -> Result<RwLockWriteGuard<InnerDirectory>> {
        match self.inner_directory.write() {
            Ok(dir) =>
                Ok(dir),
            Err(e) =>
                Err(Error::LockError(format!("Could not acquire write lock on directory. {:?}", e)))
        }
    }

    fn get_read(&self) -> Result<RwLockReadGuard<InnerDirectory>> {
        match self.inner_directory.read() {
            Ok(dir) =>
                Ok(dir),
            Err(e) =>
                Err(Error::LockError(format!("Could not acquire read lock on directory. {:?}", e)))
        }
    }

    pub fn publish_segment(&mut self, segment: Segment) -> Result<()> {
        return try!(self.get_write()).publish_segment(segment);
    }

    pub fn open(filepath: &Path) -> Result<Directory> {
        let inner_directory = try!(InnerDirectory::open(filepath));
        Ok(Directory {
            inner_directory: Arc::new(RwLock::new(inner_directory)),
        })
    }

    pub fn from_tempdir() -> Result<Directory> {
        let inner_directory = try!(InnerDirectory::from_tempdir());
        Ok(Directory {
            inner_directory: Arc::new(RwLock::new(inner_directory)),
        })
    }

    pub fn load_metas(&self,) -> Result<()> {
        match self.inner_directory.read() {
            Ok(dir) => dir.load_metas(),
            Err(e) => Err(Error::LockError(format!("Could not get read lock {:?} for directory", e)))
        }
    }

    pub fn sync(&mut self, segment: Segment) -> Result<()> {
        try!(self.get_write()).sync(segment)
    }

    pub fn segments(&self,) -> Vec<Segment> {
        match self.inner_directory.read() {
            Ok(inner) => inner
                    .segment_ids()
                    .into_iter()
                    .map(|segment_id| self.segment(&segment_id))
                    .collect(),
            Err(e) => {
                //Err(Error::LockError(format!("Could not obtain read lock for {:?}", self)))
                // TODO make it return a result
                panic!("Could not work");
            }
        }

    }

    pub fn segment(&self, segment_id: &SegmentId) -> Segment {
        Segment {
            directory: self.clone(),
            segment_id: segment_id.clone()
        }
    }

    pub fn new_segment(&self,) -> Segment {
        // TODO check it does not exists
        self.segment(&generate_segment_name())
    }

    fn open_writable(&self, relative_path: &PathBuf) -> Result<File> {
        try!(self.get_read()).open_writable(relative_path)
    }

    fn mmap(&self, relative_path: &PathBuf) -> Result<MmapReadOnly> {
        try!(self.get_read()).mmap(relative_path)
    }
}


struct InnerDirectory {
    index_path: PathBuf,
    mmap_cache: RefCell<HashMap<PathBuf, MmapReadOnly>>,
    metas: DirectoryMeta,
    _temp_directory: Option<TempDir>,
}


fn create_tempdir() -> Result<TempDir> {
    let tempdir_res = TempDir::new("index");
    match tempdir_res {
        Ok(tempdir) => Ok(tempdir),
        Err(_) => Err(Error::FileNotFound(String::from("Could not create temp directory")))
    }
}


impl InnerDirectory {

    // TODO find a rusty way to hide that, while keeping
    // it visible for IndexWriters.
    pub fn publish_segment(&mut self, segment: Segment) -> Result<()> {
        self.metas.segments.push(segment.segment_id.0.clone());
        // TODO use logs
        self.save_metas()
    }

    pub fn open(filepath: &Path) -> Result<InnerDirectory> {
        let mut directory = InnerDirectory {
            index_path: PathBuf::from(filepath),
            mmap_cache: RefCell::new(HashMap::new()),
            metas: DirectoryMeta::new(),
            _temp_directory: None,
        };
        try!(directory.load_metas()); //< does the directory already exists?
        Ok(directory)
    }

    pub fn segment_ids(&self,) -> Vec<SegmentId> {
        self.metas
            .segments
            .iter()
            .cloned()
            .map(SegmentId)
            .collect()
    }

    pub fn from_tempdir() -> Result<InnerDirectory> {
        let tempdir = try!(create_tempdir());
        let tempdir_path = PathBuf::from(tempdir.path());
        let mut directory = InnerDirectory {
            index_path: PathBuf::from(tempdir_path),
            mmap_cache: RefCell::new(HashMap::new()),
            metas: DirectoryMeta::new(),
            _temp_directory: Some(tempdir)
        };
        //< does the directory already exists?
        try!(directory.load_metas());
        Ok(directory)
    }

    pub fn load_metas(&self,) -> Result<()> {
        // TODO load segment info
        Ok(())
    }

    fn meta_filepath(&self,) -> PathBuf {
        self.resolve_path(&PathBuf::from("meta.json"))
    }

    pub fn save_metas(&self,) -> Result<()> {
        let encoded = json::encode(&self.metas).unwrap();
        let meta_filepath = self.meta_filepath();
        let meta_file = atomicwrites::AtomicFile::new(meta_filepath, atomicwrites::AllowOverwrite);
        let write_result = meta_file.write(|f| {
            f.write_all(encoded.as_bytes())
        });
        match write_result {
            Ok(_) => { Ok(()) },
            Err(ioerr) => Err(Error::IOError(ioerr.kind(), format!("Failed to write meta file : {:?}", ioerr))),
        }
    }


    pub fn sync(&mut self, segment: Segment) -> Result<()> {
        for component in [SegmentComponent::POSTINGS, SegmentComponent::TERMS].iter() {
            let relative_path = segment.relative_path(component);
            let full_path = self.resolve_path(&relative_path);
            try!(sync_file(&full_path));
        }
        // syncing the directory itself
        try!(sync_file(&self.index_path));
        Ok(())
    }

    fn resolve_path(&self, relative_path: &PathBuf) -> PathBuf {
        self.index_path.join(relative_path)
    }

    fn open_writable(&self, relative_path: &PathBuf) -> Result<File> {
        let full_path = self.resolve_path(relative_path);
        match File::create(full_path.clone()) {
            Ok(f) => Ok(f),
            Err(err) => {
                let path_str = full_path.to_str().unwrap_or("<error on to_str>");
                return Err(Error::IOError(err.kind(), String::from("Could not create file") + path_str))
            }
        }
    }

    fn mmap(&self, relative_path: &PathBuf) -> Result<MmapReadOnly> {
        let full_path = self.resolve_path(relative_path);
        let mut mmap_cache = self.mmap_cache.borrow_mut();
        if !mmap_cache.contains_key(&full_path) {
            mmap_cache.insert(full_path.clone(), try!(open_mmap(&full_path)) );
        }
        let mmap_readonly: &MmapReadOnly = mmap_cache.get(&full_path).unwrap();
        // TODO remove if a proper clone is available
        let len = unsafe { mmap_readonly.as_slice().len() };
        Ok(mmap_readonly.range(0, len))
    }
}



/////////////////////////
// Segment

pub enum SegmentComponent {
    POSTINGS,
    // POSITIONS,
    TERMS,
}

#[derive(Debug, Clone)]
pub struct Segment {
    directory: Directory,
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
        }
    }

    pub fn relative_path(&self, component: &SegmentComponent) -> PathBuf {
        let SegmentId(ref segment_id_str) = self.segment_id;
        let filename = String::new() + segment_id_str + Segment::path_suffix(component);
        PathBuf::from(filename)
    }

    pub fn mmap(&self, component: SegmentComponent) -> Result<MmapReadOnly> {
        let path = self.relative_path(&component);
        self.directory.mmap(&path)
    }

    pub fn open_writable(&self, component: SegmentComponent) -> Result<File> {
        let path = self.relative_path(&component);
        self.directory.open_writable(&path)
    }
}
