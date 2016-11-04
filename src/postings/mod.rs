/// Postings module
///
/// Postings, also called inverted lists, is the key datastructure
/// to full-text search.


mod postings;
mod recorder;
mod serializer;
mod postings_writer;
mod term_info;
mod chained_postings;
mod vec_postings;
mod segment_postings;
mod intersection;
mod offset_postings;
mod freq_handler;
mod docset;
mod segment_postings_option;

pub use self::docset::{SkipResult, DocSet};
pub use self::offset_postings::OffsetPostings;
pub use self::recorder::{Recorder, NothingRecorder, TermFrequencyRecorder, TFAndPositionRecorder};
pub use self::serializer::PostingsSerializer;
pub use self::postings_writer::PostingsWriter;
pub use self::postings_writer::SpecializedPostingsWriter;
pub use self::term_info::TermInfo;
pub use self::postings::Postings;

#[cfg(test)]
pub use self::vec_postings::VecPostings;
pub use self::chained_postings::ChainedPostings;
pub use self::segment_postings::SegmentPostings;
pub use self::intersection::IntersectionDocSet;
pub use self::freq_handler::FreqHandler;

pub use self::segment_postings_option::SegmentPostingsOption;
pub use common::HasLen;

#[cfg(test)]
mod tests {
    
    use super::*;
    use schema::{Document, TEXT, SchemaBuilder, Term};
    use core::SegmentComponent;
    use indexer::SegmentWriter;
    use core::SegmentReader;
    use core::Index;
    use std::iter;
    use datastruct::stacker::Heap;
    use query::TermQuery;
    
        
    #[test]
    pub fn test_position_write() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut segment = index.new_segment();
        let mut posting_serializer = PostingsSerializer::open(&mut segment).unwrap();
        let term = Term::from_field_text(text_field, "abc");
        posting_serializer.new_term(&term, 3).unwrap();
        for doc_id in 0u32..3u32 {
            let positions = vec!(1,2,3,2);
            posting_serializer.write_doc(doc_id, 2, &positions).unwrap();
        }
        posting_serializer.close_term().unwrap();
        posting_serializer.close().unwrap();
        let read = segment.open_read(SegmentComponent::POSITIONS).unwrap();
        assert_eq!(read.len(), 13);
    }
    
    #[test]
    pub fn test_position_and_fieldnorm() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let segment = index.new_segment();
        let heap = Heap::with_capacity(10_000_000);
        {
            let mut segment_writer = SegmentWriter::for_segment(&heap, segment.clone(), &schema).unwrap();
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "a b a c a d a a.");
                doc.add_text(text_field, "d d d d a"); // checking that position works if the field has two values.
                segment_writer.add_document(&doc, &schema).unwrap();
            }
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "b a");
                segment_writer.add_document(&doc, &schema).unwrap();
            }
            for i in 2..1000 {
                let mut doc = Document::default();
                let mut text = iter::repeat("e ").take(i).collect::<String>();
                text.push_str(" a");
                doc.add_text(text_field, &text);
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
                for i in 2 .. 1000 {
                    assert_eq!(fieldnorm_reader.get(i), i + 1);
                }
            }
            {
                let term_a = Term::from_field_text(text_field, "abcdef");
                assert!(segment_reader.read_postings_all_info(&term_a).is_none());
            }
            {
                let term_a = Term::from_field_text(text_field, "a");
                let mut postings_a = segment_reader.read_postings_all_info(&term_a).unwrap();
                assert_eq!(postings_a.len(), 1000);
                assert!(postings_a.advance());
                assert_eq!(postings_a.doc(), 0);
                assert_eq!(postings_a.term_freq(), 6);
                assert_eq!(postings_a.positions(), [0, 2, 4, 6, 7, 13]);
                assert!(postings_a.advance());
                assert_eq!(postings_a.doc(), 1u32);
                assert_eq!(postings_a.term_freq(), 1);
                for i in 2u32 .. 1000u32 {
                    assert!(postings_a.advance());
                    assert_eq!(postings_a.term_freq(), 1);
                    assert_eq!(postings_a.positions(), [i]);
                    assert_eq!(postings_a.doc(), i);
                }
                assert!(!postings_a.advance());
            }
            {
                let term_e = Term::from_field_text(text_field, "e");
                let mut postings_e = segment_reader.read_postings_all_info(&term_e).unwrap();
                assert_eq!(postings_e.len(), 1000 - 2);
                for i in 2u32 .. 1000u32 {
                    assert!(postings_e.advance());
                    assert_eq!(postings_e.term_freq(), i);
                    let positions = postings_e.positions();
                    assert_eq!(positions.len(), i as usize);
                    for j in 0..positions.len() {
                        assert_eq!(positions[j], (j as u32));
                    }
                    assert_eq!(postings_e.doc(), i);
                }
                assert!(!postings_e.advance());
            }
        }
    }
    
    #[test]
    pub fn test_position_and_fieldnorm2() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "g b b d c g c");
                index_writer.add_document(doc).unwrap();
            }
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "g a b b a d c g c");
                index_writer.add_document(doc).unwrap();
            }
            assert!(index_writer.commit().is_ok());
        }
        let term_query = TermQuery::from(Term::from_field_text(text_field, "a"));
        let searcher = index.searcher();
        let mut term_weight = term_query.specialized_weight(&*searcher);
        term_weight.segment_postings_options = SegmentPostingsOption::FreqAndPositions;
        let segment_reader = &searcher.segment_readers()[0];
        let mut term_scorer = term_weight.specialized_scorer(segment_reader).unwrap();
        assert!(term_scorer.advance());
        assert_eq!(term_scorer.doc(), 1u32);
        assert_eq!(term_scorer.postings().positions(), &[1u32, 4]);
    }
    
    #[test]
    fn test_intersection() {
        {
            let left = VecPostings::from(vec!(1, 3, 9));
            let right = VecPostings::from(vec!(3, 4, 9, 18));
            let mut intersection = IntersectionDocSet::from(vec!(left, right));
            assert!(intersection.advance());
            assert_eq!(intersection.doc(), 3);
            assert!(intersection.advance());
            assert_eq!(intersection.doc(), 9);
            assert!(!intersection.advance());
        }
        {
            let a = VecPostings::from(vec!(1, 3, 9));
            let b = VecPostings::from(vec!(3, 4, 9, 18));
            let c = VecPostings::from(vec!(1, 5, 9, 111));
            let mut intersection = IntersectionDocSet::from(vec!(a, b, c));
            assert!(intersection.advance());
            assert_eq!(intersection.doc(), 9);
            assert!(!intersection.advance());
        }
    }
     
}
