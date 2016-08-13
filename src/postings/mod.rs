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
mod union_postings;
mod docset;
mod scored_docset;
mod segment_postings_option;

pub use self::docset::{SkipResult, DocSet};
pub use self::union_postings::UnionPostings;
pub use self::offset_postings::OffsetPostings;
pub use self::recorder::{Recorder, NothingRecorder, TermFrequencyRecorder, TFAndPositionRecorder};
pub use self::serializer::PostingsSerializer;
pub use self::writer::PostingsWriter;
pub use self::writer::SpecializedPostingsWriter;
pub use self::term_info::TermInfo;
pub use self::postings::Postings;
pub use self::vec_postings::VecPostings;
pub use self::chained_postings::ChainedPostings;
pub use self::segment_postings::SegmentPostings;
pub use self::intersection::intersection;
pub use self::intersection::IntersectionDocSet;
pub use self::freq_handler::FreqHandler;
pub use self::scored_docset::ScoredDocSet;
pub use self::postings::HasLen;
pub use self::segment_postings_option::SegmentPostingsOption;

#[cfg(test)]
mod tests {
    
    use super::*;
    use schema::{Document, TEXT, Schema, Term};
    use core::SegmentComponent;
    use core::SegmentWriter;
    use core::SegmentReader;
    use core::index::Index;
    
        
    #[test]
    pub fn test_position_write() {
        let mut schema = Schema::new();
        let text_field = schema.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema);
        let segment = index.new_segment();
        let mut posting_serializer = PostingsSerializer::open(&segment).unwrap();
        let term = Term::from_field_text(text_field, "abc");
        posting_serializer.new_term(&term, 3).unwrap();
        for _ in 0..3 {
            let a = vec!(1,2,3,2);
            posting_serializer.write_doc(0, 2, &a).unwrap();
        }
        posting_serializer.close_term().unwrap();
        let read = segment.open_read(SegmentComponent::POSITIONS).unwrap();
        assert_eq!(read.len(), 13);
    }
    
    #[test]
    pub fn test_position_and_fieldnorm_write_fullstack() {
        let mut schema = Schema::new();
        let text_field = schema.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema.clone());
        let segment = index.new_segment();
        {
            let mut segment_writer = SegmentWriter::for_segment(segment.clone(), &schema).unwrap();
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "a b a c a d a a.");
                doc.add_text(text_field, "d d d d a"); // checking that position works if the field has two values.
                segment_writer.add_document(&doc, &schema).unwrap();
            }
            {
                let mut doc = Document::new();
                doc.add_text(text_field, "b a");
                segment_writer.add_document(&doc, &schema).unwrap();
            }
            segment_writer.finalize().unwrap();
        }
        {
            let segment_reader = SegmentReader::open(segment).unwrap();
            {
                let fieldnorm_reader = segment_reader.get_fieldnorms_reader(text_field).unwrap();
                assert_eq!(fieldnorm_reader.get(0), 8 + 5);
                assert_eq!(fieldnorm_reader.get(1), 2);
            }
            {
                let term = Term::from_field_text(text_field, "a");
                let mut postings = segment_reader.read_postings_all_info(&term).unwrap();
                assert_eq!(postings.len(), 2);
                assert!(postings.advance());
                assert_eq!(postings.doc(), 0);
                assert_eq!(postings.term_freq(), 6);
                assert_eq!(postings.positions(), [0, 2, 4, 6, 7, 13]);
                assert!(postings.advance());
                assert_eq!(postings.doc(), 1);
                assert_eq!(postings.term_freq(), 1);
                assert!(!postings.advance());
            }
        }
    }
    
    #[test]
    fn test_intersection() {
        {
            let left = Box::new(VecPostings::from(vec!(1, 3, 9)));
            let right = Box::new(VecPostings::from(vec!(3, 4, 9, 18)));
            let mut intersection = IntersectionDocSet::new(vec!(left, right));
            assert!(intersection.advance());
            assert_eq!(intersection.doc(), 3);
            assert!(intersection.advance());
            assert_eq!(intersection.doc(), 9);
            assert!(!intersection.advance());
        }
        {
            let a = Box::new(VecPostings::from(vec!(1, 3, 9)));
            let b = Box::new(VecPostings::from(vec!(3, 4, 9, 18)));
            let c = Box::new(VecPostings::from(vec!(1, 5, 9, 111)));
            let mut intersection = IntersectionDocSet::new(vec!(a, b, c));
            assert!(intersection.advance());
            assert_eq!(intersection.doc(), 9);
            assert!(!intersection.advance());
        }
    }
     
}



// #[cfg(test)]
// mod tests {

//     use super::*;
//     use test::Bencher;

//
//     #[bench]
//     fn bench_single_intersection(b: &mut Bencher) {
//         b.iter(|| {
//             let docs = VecPostings::new((0..1_000_000).collect());
//             let intersection = IntersectionDocSet::from_postings(vec!(docs));
//             intersection.count()
//         });
//     }
// }
//