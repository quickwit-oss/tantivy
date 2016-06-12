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
mod freq_handler;


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
pub use self::freq_handler::FreqHandler;



#[cfg(test)]
mod tests {
    
    use super::*;
    use schema::{TEXT, Schema, Term};
    use core::index::SegmentComponent;
    use core::index::Index;
    
    #[test]
    pub fn test_position_write() {
        let mut schema = Schema::new();
        let text_field = schema.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema);
        let segment = index.new_segment();
        let mut posting_serializer = PostingsSerializer::open(&segment).unwrap();
        let term = Term::from_field_text(&text_field, "abc");
        posting_serializer.new_term(&term, 3).unwrap();
        for _ in 0..3 {
            let a = vec!(1,2,3,2);
            posting_serializer.write_doc(0, 2, &a).unwrap();
        }
        posting_serializer.close_term().unwrap();
        let read = segment.open_read(SegmentComponent::POSITIONS).unwrap();
        assert_eq!(read.len(), 12);
    }
     
}



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
//