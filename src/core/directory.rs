extern crate memmap;

use self::memmap::{Mmap, Protection};
use std::path::PathBuf;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::rc::Rc;
use std::ops::Deref;
use std::cell::RefCell;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct SegmentId(String);

pub trait Dir {
    fn get_data(&self, segment_id: &SegmentId, component: SegmentComponent) -> Result<SharedMmapMemory, io::Error>; // {
}

#[derive(Clone)]
pub struct Directory {
    dir: Rc<Dir>,
}

impl Directory {
    fn segment(&self, segment_id: &SegmentId) -> Segment {
        Segment {
            directory: self.dir.clone(),
            segment_id: segment_id.clone()
        }
    }

    fn from<T: Dir + 'static>(directory: T) -> Directory {
        Directory {
            dir: Rc::new(directory),
        }
    }

    pub fn open(path_str: &str) -> Directory {
        let path = PathBuf::from(path_str);
        Directory::from(FileDirectory::for_path(path))
    }

    pub fn in_mem() -> Directory {
        Directory::from(MemDirectory::new())
    }
}

impl Dir for Directory {
    fn get_data(&self, segment_id: &SegmentId, component: SegmentComponent) -> Result<SharedMmapMemory, io::Error> {
        self.dir.get_data(segment_id, component)
    }
}

pub enum SegmentComponent {
    POSTINGS,
    POSITIONS,
}

pub struct Segment {
    directory: Rc<Dir>,
    segment_id: SegmentId,
}

impl Segment {
    fn path_suffix(component: SegmentComponent)-> &'static str {
        match component {
            SegmentComponent::POSTINGS => ".pstgs",
            SegmentComponent::POSITIONS => ".pos",
        }
    }

    fn get_data(&self, component: SegmentComponent) -> Result<SharedMmapMemory, io::Error> {
        self.directory.get_data(&self.segment_id, component)
    }
}



/////////////////////////////////////////////////////////
//  MemoryPointer

pub trait MemoryPointer {
    fn len(&self) -> usize;
    fn ptr(&self) -> *const u8;
}

/////////////////////////////////////////////////////////
//  ResidentMemoryPointer

pub struct ResidentMemoryPointer {
    data: Box<[u8]>,
    len: usize,
}

impl MemoryPointer for ResidentMemoryPointer {
    fn len(&self) -> usize {
        self.len
    }
    fn ptr(&self) -> *const u8 {
        &self.data[0]
    }
}



/////////////////////////////////////////////////////////
//
//


pub struct MmapMemory(Mmap);

impl MemoryPointer for MmapMemory {
    fn len(&self) -> usize {
        let &MmapMemory(ref mmap) = self;
        mmap.len()
    }
    fn ptr(&self) -> *const u8 {
        let &MmapMemory(ref mmap) = self;
        mmap.ptr()
    }
}

#[derive(Clone)]
pub struct SharedMmapMemory(Arc<MmapMemory>);

impl SharedMmapMemory {
    pub fn new(mmap_memory: MmapMemory) -> SharedMmapMemory {
        SharedMmapMemory(Arc::new(mmap_memory))
    }
}

//////////////////////////////////////////////////////////
//  FileDirectory

pub struct FileDirectory {
    index_path: PathBuf,
    mmap_cache: RefCell<HashMap<PathBuf, SharedMmapMemory >>,
}

impl FileDirectory {
    pub fn for_path(path: PathBuf)-> FileDirectory {
        FileDirectory {
            index_path: path,
            mmap_cache: RefCell::new(HashMap::new()),
        }
    }

    fn get_or_open_mmap(&self, filepath: &PathBuf)->Result<SharedMmapMemory, io::Error> {
        if !self.mmap_cache.borrow().contains_key(filepath) {
            let file = try!(File::open(filepath));
            let mmap = MmapMemory(try!(Mmap::open(&file, Protection::Read)));
            self.mmap_cache.borrow_mut().insert(filepath.clone(), SharedMmapMemory::new(mmap));
        }
        let shared_map: SharedMmapMemory = self.mmap_cache.borrow().get(filepath).unwrap().clone();
        Ok(shared_map)
    }
}

impl Dir for FileDirectory {
    fn get_data(&self, segment_id: &SegmentId, component: SegmentComponent) -> Result<SharedMmapMemory, io::Error> {
        let mut filepath = self.index_path.clone();
        let SegmentId(ref segment_id_str) = *segment_id;
        let filename = String::new() + segment_id_str + "." + Segment::path_suffix(component);
        filepath.push(filename);
        self.get_or_open_mmap(&filepath)
    }

}

//////////////////////////////////////////////////////////
// FileDirectory
//

pub struct MemDirectory {
    dir: HashMap<PathBuf, SharedMmapMemory>,
}

impl MemDirectory {
    pub fn new()-> MemDirectory {
        MemDirectory {
            dir: HashMap::new(),
        }
    }
}

impl Dir for MemDirectory {
    fn get_data(&self, segment_id: &SegmentId, component: SegmentComponent) -> Result<SharedMmapMemory, io::Error> {
        let SegmentId(ref segment_id_str) = *segment_id;
        let mut path = PathBuf::from(segment_id_str);
        path.push(Segment::path_suffix(component));
        match self.dir.get(&path) {
            Some(buf) => Ok(buf.clone()),
            None => Err(io::Error::new(io::ErrorKind::NotFound, "File does not exists")),
        }
    }
}
