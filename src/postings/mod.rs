mod postings;
mod recorder;
mod serializer;
mod writer;
mod term_info;
mod chained_postings;
mod vec_postings;
mod segment_postings;
mod intersection;
mod offset_postings;


pub use self::offset_postings::OffsetPostings;
pub use self::recorder::{Recorder, NothingRecorder, TermFrequencyRecorder, TFAndPositionRecorder};
pub use self::serializer::PostingsSerializer;
pub use self::writer::PostingsWriter;
pub use self::term_info::TermInfo;
pub use self::postings::{Postings, SkipResult};
pub use self::vec_postings::VecPostings;
pub use self::chained_postings::ChainedPostings;
pub use self::segment_postings::SegmentPostings;
pub use self::intersection::intersection;
pub use self::intersection::IntersectionPostings;


// #[cfg(test)]
// mod tests {
//
//     use super::*;
//     use test::Bencher;
//     #[test]
//     fn test_intersection() {
//         {
//             let left = VecPostings::new(vec!(1, 3, 9));
//             let right = VecPostings::new(vec!(3, 4, 9, 18));
//             let inter = IntersectionPostings::from_postings(vec!(left, right));
//             let vals: Vec<DocId> = inter.collect();
//             assert_eq!(vals, vec!(3, 9));
//         }
//         {
//             let a = VecPostings::new(vec!(1, 3, 9));
//             let b = VecPostings::new(vec!(3, 4, 9, 18));
//             let c = VecPostings::new(vec!(1, 5, 9, 111));
//             let inter = IntersectionPostings::from_postings(vec!(a, b, c));
//             let vals: Vec<DocId> = inter.collect();
//             assert_eq!(vals, vec!(9));
//         }
//     }
//
//     #[bench]
//     fn bench_single_intersection(b: &mut Bencher) {
//         b.iter(|| {
//             let docs = VecPostings::new((0..1_000_000).collect());
//             let intersection = IntersectionPostings::from_postings(vec!(docs));
//             intersection.count()
//         });
//     }
// }
