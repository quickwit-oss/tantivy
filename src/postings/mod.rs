mod postings;
mod recorder;
mod serializer;
mod writer;
mod term_info;
mod vec_postings;

pub use self::recorder::{Recorder, NothingRecorder, TermFrequencyRecorder, TFAndPositionRecorder};
pub use self::serializer::PostingsSerializer;
pub use self::writer::PostingsWriter;
pub use self::term_info::TermInfo;
pub use self::postings::Postings;
pub use self::vec_postings::VecPostings;



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
