extern crate memmap;

use self::memmap::{Mmap, Protection};
use std::path::PathBuf;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::io::BufWriter;
use std::io;
use std::borrow::Borrow;
use std::borrow::BorrowMut;
use std::rc::Rc;
use std::ops::Deref;
use std::cell::RefCell;
use std::sync::Arc;
use core::error::*;
use rand::{thread_rng, Rng};


#[derive(Clone, Debug)]
pub struct SegmentId(pub String);

pub fn generate_segment_name() -> SegmentId {
    static CHARS: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let random_name: String = (0..8)
            .map(|_| thread_rng().choose(CHARS).unwrap().clone() as char)
            .collect();
    SegmentId( String::from("_") + &random_name)
}


#[derive(Clone, Debug)]
pub struct Directory {
    index_path: PathBuf,
    // mmap_cache: RefCell<HashMap<PathBuf, SharedMmapMemory >>,
}

impl Directory {

    pub fn from(filepath: &str) -> Directory {
        Directory {
            index_path: PathBuf::from(filepath)
        }
    }

    fn resolve_path(&self, relative_path: &PathBuf) -> PathBuf {
        self.index_path.join(relative_path)
    }

    fn segment<'a>(&'a self, segment_id: &SegmentId) -> Segment<'a> {
        Segment {
            directory: self,
            segment_id: segment_id.clone()
        }
    }

    pub fn new_segment<'a>(&'a self,) -> Segment<'a> {
        // TODO check it does not exists
        self.segment(&generate_segment_name())
    }

    fn open_writable<'a>(&self, relative_path: &PathBuf) -> Result<File> {
        let full_path = self.resolve_path(relative_path);
        match File::create(full_path.clone()) {
            Ok(f) => Ok(f),
            Err(err) => {
                let path_str = full_path.to_str().unwrap_or("<error on to_str>");
                return Err(Error::IOError(err.kind(), String::from("Could not create file") + path_str))
            }
        }
    }
}

/////////////////////////
// Segment

pub enum SegmentComponent {
    POSTINGS,
    POSITIONS,
    TERMS,
}

#[derive(Debug)]
pub struct Segment<'a> {
    directory: &'a Directory,
    segment_id: SegmentId,
}

impl<'a> Segment<'a> {
    fn path_suffix(component: SegmentComponent)-> &'static str {
        match component {
            SegmentComponent::POSTINGS => ".idx",
            SegmentComponent::POSITIONS => ".pos",
            SegmentComponent::TERMS => ".term",
        }
    }

    fn get_relative_path(&self, component: SegmentComponent) -> PathBuf {
        let SegmentId(ref segment_id_str) = self.segment_id;
        let filename = String::new() + segment_id_str + Segment::path_suffix(component);
        PathBuf::from(filename)
    }

    // pub fn get_data(&self, component: SegmentComponent) -> Result<Box<Borrow<[u8]>>> {
    //     self.directory.get_data(&self.segment_id, component)
    // }
    
    pub fn open_writable(&self, component: SegmentComponent) -> Result<File> {
        let path = self.get_relative_path(component);
        self.directory.open_writable(&path)
    }
}




// #[derive(Clone)]
// pub struct Directory {
//     dir: Rc<Dir>,
// }
//
// impl Directory {
//     fn segment(&self, segment_id: &SegmentId) -> Segment {
//         Segment {
//             directory: self.dir.clone(),
//             segment_id: segment_id.clone()
//         }
//     }
//
//     pub fn new_segment(&self,) -> Segment {
//         self.segment(&generate_segment_name())
//     }
//
//     fn from<T: Dir + 'static>(directory: T) -> Directory {
//         Directory {
//             dir: Rc::new(directory),
//         }
//     }
//
//     // pub fn open(path_str: &str) -> Directory {
//     //     let path = PathBuf::from(path_str);
//     //     Directory::from(FileDirectory::for_path(path))
//     // }
//
//     pub fn in_mem() -> Directory {
//         Directory::from(MemDirectory::new())
//     }
// }
//
// impl Dir for Directory {
//     // fn get_data(&self, segment_id: &SegmentId, component: SegmentComponent) -> Result<Box<Borrow<[u8]>>> {
//     //     self.dir.get_data(segment_id, component)
//     // }
//


//     fn open_writable<'a>(&'a self, path: &PathBuf) -> Result<Box<BorrowMut<Write>>> {
//         self.dir.open_writable(path)
//     }
// }

// pub enum SegmentComponent {
//     POSTINGS,
//     POSITIONS,
//     TERMS,
// }
//
// pub struct Segment {
//     directory: Rc<Dir>,
//     segment_id: SegmentId,
// }
//
// impl Segment {
//     fn path_suffix(component: SegmentComponent)-> &'static str {
//         match component {
//             SegmentComponent::POSTINGS => ".idx",
//             SegmentComponent::POSITIONS => ".pos",
//             SegmentComponent::TERMS => ".term",
//         }
//     }
//
//     fn get_path(&self, component: SegmentComponent) -> PathBuf {
//         let mut filepath = self.index_path.clone();
//         let SegmentId(ref segment_id_str) = self.segment_id;
//         let filename = String::new() + segment_id_str + "." + Segment::path_suffix(component);
//         filepath.push(filename);
//         filepath
//     }
//
//     // pub fn get_data(&self, component: SegmentComponent) -> Result<Box<Borrow<[u8]>>> {
//     //     self.directory.get_data(&self.segment_id, component)
//     // }
//     pub fn open_writable(&self, component: SegmentComponent) -> Result<Box<BorrowMut<Write>>> {
//         let path = self.get_path(component);
//         self.directory.open_writable(path)
//     }
// }



//
// /////////////////////////////////////////////////////////
// //  ResidentMemoryPointer
//
// pub struct ResidentMemoryPointer {
//     data: Box<[u8]>,
// }
//
// impl Borrow<[u8]> for ResidentMemoryPointer {
//     fn borrow(&self) -> &[u8] {
//         self.data.deref()
//     }
// }
//
//
// //////////////////////////////////////////////////////////
// // MemDirectory
// //
// pub struct MemDirectory {
//     dir: HashMap<PathBuf, Rc<String>>,
// }
//
// impl MemDirectory {
//     pub fn new()-> MemDirectory {
//         MemDirectory {
//             dir: HashMap::new(),
//         }
//     }
//
//     // fn get_path(&self,  segment_id: &SegmentId, component: SegmentComponent) -> PathBuf {
//     //     let mut filepath = self.index_path.clone();
//     //     let SegmentId(ref segment_id_str) = *segment_id;
//     //     let filename = String::new() + segment_id_str + "." + Segment::path_suffix(component);
//     //     filepath.push(filename);
//     //     filepath
//     // }
// }
//
// impl Directory for MemDirectory {
//
//     type Write = Rc<String>;
//
//     // fn get_data(&self, segment_id: &SegmentId, component: SegmentComponent) -> Result<Box<Borrow<[u8]>>> {
//     //     let path = self.get_path(segment_id, component);
//     //     match self.dir.get(&path) {
//     //         Some(buf) => Ok(buf.clone()),
//     //         None => Err(Error::FileNotFound(String::from("File not found"))), // TODO add filename
//     //     }
//     // }
//
//     fn open_writable<'a>(&'a self, path: &PathBuf) -> Result<Rc<String>> {
//         if self.dir.contains_key(path) {
//             return Err(Error::ReadOnly(String::from("Cannot open an already written buffer.")));
//         }
//         self.dir.insert(path.clone(), Rc::new(String::new()));
//         self.dir.get(path);
//         Ok(Box::new())
//
//     }
// }
//
//
//
//
//
//
//
//
//
//
//
//
//






























//
//
// #[derive(Clone)]
// pub struct SharedMmapMemory(Arc<MmapMemory>);
//
// impl SharedMmapMemory {
//     pub fn new(mmap_memory: MmapMemory) -> SharedMmapMemory {
//         SharedMmapMemory(Arc::new(mmap_memory))
//     }
// }
//
//
// /////////////////////////////////////////////////////////
// // MmapMemory
// //
//
// pub struct MmapMemory(Mmap);
//
// impl MemoryPointer for MmapMemory {
//     fn data(&self) -> &[u8] {
//         let &MmapMemory(ref mmap) = self;
//         unsafe {
//             mmap.as_slice()
//         }
//     }
// }
//
// //////////////////////////////////////////////////////////
// //  FileDirectory
//
// pub struct FileDirectory {
//     index_path: PathBuf,
//     mmap_cache: RefCell<HashMap<PathBuf, SharedMmapMemory >>,
// }
//
// impl FileDirectory {
//     pub fn for_path(path: PathBuf)-> FileDirectory {
//         FileDirectory {
//             index_path: path,
//             mmap_cache: RefCell::new(HashMap::new()),
//         }
//     }
//
//     fn get_or_open_mmap(&self, filepath: &PathBuf)->Result<[u8]> {
//         if !self.mmap_cache.borrow().contains_key(filepath) {
//             let file = try!(File::open(filepath));
//             let mmap = MmapMemory(try!(Mmap::open(&file, Protection::Read)));
//             self.mmap_cache.borrow_mut().insert(filepath.clone(), SharedMmapMemory::new(mmap));
//         }
//         let shared_map: SharedMmapMemory = self.mmap_cache.borrow().get(filepath).unwrap().clone();
//         Ok(shared_map)
//     }
//
//     fn get_path(&self,  segment_id: &SegmentId, component: SegmentComponent) -> PathBuf {
//         let mut filepath = self.index_path.clone();
//         let SegmentId(ref segment_id_str) = *segment_id;
//         let filename = String::new() + segment_id_str + "." + Segment::path_suffix(component);
//         filepath.push(filename);
//         filepath
//     }
// }
//
// impl Dir for FileDirectory {
//     fn get_data(&self, segment_id: &SegmentId, component: SegmentComponent) -> Result<[u8]> {
//         let filepath = self.get_path(segment_id, component);
//         self.get_or_open_mmap(&filepath)
//     }
//
//     fn open_writable<'a>(&'a self, segment_id: &SegmentId, component: SegmentComponent) -> Result<Write + 'a> {
//         Err(Error::IOError("e"))
//     }
// }
