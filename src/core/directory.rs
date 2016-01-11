
extern crate memmap;

use self::memmap::{Mmap, Protection};
use std::path::PathBuf;
use std::fs::File;
use std::io;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct SegmentId(String);

pub trait Dir {
    fn get_file(&self, segment_id: &SegmentId, component: SegmentComponent) -> Result<File, io::Error>;
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

    pub fn open(path_str: &str) -> Directory {
        let path = PathBuf::from(path_str);
        Directory {
            dir: Rc::new(FileDirectory::for_path(path)),
        }
    }
}

impl Dir for Directory {
    fn get_file(&self, segment_id: &SegmentId, component: SegmentComponent) -> Result<File, io::Error> {
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

    fn get_file(&self, component: SegmentComponent) -> Result<File, io::Error> {
        self.directory.get_file(&self.segment_id, component)
    }

    pub fn open(&self, component: SegmentComponent) -> Result<Mmap, io::Error> {
        let file = try!(self.get_file(component));
        Mmap::open(&file, Protection::Read)
    }
}


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

impl Dir for FileDirectory {
    fn get_file(&self, segment_id: &SegmentId, component: SegmentComponent) -> Result<File, io::Error> {
        let mut res = self.index_path.clone();
        let SegmentId(ref segment_id_str) = *segment_id;
        let filename = String::new() + segment_id_str + "." + Segment::path_suffix(component);
        res.push(filename);
        File::open(res)
    }
}
