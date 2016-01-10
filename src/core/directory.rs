
extern crate memmap;

use self::memmap::{Mmap, Protection};
use std::path::PathBuf;
use std::fs::File;
use std::io;

#[derive(Clone, Debug)]
struct SegmentId(String);

struct IndexDirectory {
    index_path: PathBuf,
}

impl IndexDirectory {

    pub fn for_path(path: PathBuf)-> IndexDirectory {
        IndexDirectory {
            index_path: path,
        }
    }

    pub fn read_segment(&self, segment_id: &SegmentId) -> SegmentDirectory {
        SegmentDirectory {
            index_path: self.index_path.clone(),
            segment_id: segment_id.clone()
        }
    }

    

}

enum SegmentComponent {
    POSTINGS,
    POSITIONS,
}

struct SegmentDirectory {
    index_path: PathBuf,
    segment_id: SegmentId,
}

impl SegmentDirectory {

    fn path_suffix(component: SegmentComponent)-> &'static str {
        match component {
            SegmentComponent::POSTINGS => ".pstgs",
            SegmentComponent::POSITIONS => ".pos",
        }
    }

    fn get_file(&self, component: SegmentComponent) -> Result<File, io::Error> {
        let mut res = self.index_path.clone();
        let SegmentId(ref segment_id_str) = self.segment_id;
        let filename = String::new() + segment_id_str + "." + SegmentDirectory::path_suffix(component);
        res.push(filename);
        File::open(res)
    }

    pub fn open(&self, component: SegmentComponent) -> Result<Mmap, io::Error> {
        let file = try!(self.get_file(component));
        Mmap::open(&file, Protection::Read)
    }
}
