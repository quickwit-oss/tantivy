use core::directory::Directory;
use core::directory::Segment;

pub struct SegmentIndexReader {
    directory: Directory,
}


// pub trait SearchableSegment {
//
// }
//
// pub struct SimpleSearchableSegment {
//     segment: Segment,
// }
//
// impl SimpleSearchableSegment {
//
//     pub fn new(segment: &Segment) -> SimpleSearchableSegment {
//         SimpleSearchableSegment {
//             segment: segment.clone()
//         }
//     }
// }
//
// impl SearchableSegment for SimpleSearchableSegment {
//
//
// }


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
