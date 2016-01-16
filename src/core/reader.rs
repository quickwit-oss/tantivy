use core::directory::Directory;
use core::global::DocId;
use core::schema::*;

pub struct SegmentIndexReader {
    directory: Directory,
}

//
// impl SegmentIndexReader {
//
//     pub fn open(directory: &Directory) -> IndexReader {
// 		IndexReader {
// 			directory: (*directory).clone(),
//         }
//     }
//
// }
