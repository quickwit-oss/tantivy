extern crate memmap;

use self::memmap::{Mmap, Protection};
use std::path::PathBuf;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct SegmentId(String);

pub trait Dir {
    fn get_file<'a>(&'a self, segment_id: &SegmentId, component: SegmentComponent) -> Result<&'a MemoryPointer, io::Error>; // {
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
    fn get_file<'a>(&'a self, segment_id: &SegmentId, component: SegmentComponent) -> Result<&'a MemoryPointer, io::Error> {
        self.dir.get_file(segment_id, component)
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

    fn get_file<'a>(&'a self, component: SegmentComponent) -> Result<&'a MemoryPointer, io::Error> {
        self.directory.get_file(&self.segment_id, component)
    }
}






//////////////////////////////////////////////////////////
//  FileDirectory

pub struct FileDirectory {
    index_path: PathBuf,
}

impl FileDirectory {
    pub fn for_path(path: PathBuf)-> FileDirectory {
        FileDirectory {
            index_path: path,
        }
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


struct MmapMemory(Mmap);

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

impl Dir for FileDirectory {
    fn get_file<'a>(&'a self, segment_id: &SegmentId, component: SegmentComponent) -> Result<&'a MemoryPointer, io::Error> {
        let mut res = self.index_path.clone();
        let SegmentId(ref segment_id_str) = *segment_id;
        let filename = String::new() + segment_id_str + "." + Segment::path_suffix(component);
        res.push(filename);
        let file = try!(File::open(res));
        let mmap = MmapMemory(try!(Mmap::open(&file, Protection::Read)));
        // let boxed_mmap: Box<MemoryPointer> = Box::new(mmap);
        // Ok(boxed_mmap)
        Err(io::Error::new(io::ErrorKind::AddrInUse, "eee"))
    }
}

//////////////////////////////////////////////////////////
// FileDirectory
//

pub struct MemDirectory {
    dir: HashMap<PathBuf, Box<MemoryPointer>>,
}

impl MemDirectory {
    pub fn new()-> MemDirectory {
        MemDirectory {
            dir: HashMap::new(),
        }
    }
}

impl Dir for MemDirectory {
    fn get_file<'a>(&'a self, segment_id: &SegmentId, component: SegmentComponent) -> Result<&'a MemoryPointer, io::Error> {
        let SegmentId(ref segment_id_str) = *segment_id;
        let mut path = PathBuf::from(segment_id_str);
        path.push(Segment::path_suffix(component));
        match self.dir.get(&path) {
            Some(buf) => {
                Ok(buf.as_ref())
            },
            None => Err(io::Error::new(io::ErrorKind::NotFound, "File does not exists")),
        }
    }
}
