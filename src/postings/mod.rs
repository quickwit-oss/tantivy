/// Postings module
///
/// Postings, also called inverted lists, is the key datastructure
/// to full-text search.


mod postings;
mod recorder;
mod serializer;
mod postings_writer;
mod term_info;
mod vec_postings;
mod segment_postings;
mod intersection;
mod docset;
mod segment_postings_option;

pub use self::docset::{SkipResult, DocSet};
use self::recorder::{Recorder, NothingRecorder, TermFrequencyRecorder, TFAndPositionRecorder};
pub use self::serializer::{InvertedIndexSerializer, FieldSerializer};
pub(crate) use self::postings_writer::MultiFieldPostingsWriter;

pub use self::term_info::TermInfo;
pub use self::postings::Postings;

#[cfg(test)]
pub use self::vec_postings::VecPostings;

pub use self::segment_postings::{SegmentPostings, BlockSegmentPostings};
pub use self::intersection::IntersectionDocSet;
pub use self::segment_postings_option::SegmentPostingsOption;
pub use common::HasLen;


#[cfg(test)]
mod tests {

    use super::*;
    use schema::{Document, INT_INDEXED, TEXT, STRING, SchemaBuilder, Term};
    use core::SegmentComponent;
    use indexer::SegmentWriter;
    use core::SegmentReader;
    use core::Index;
    use postings::SegmentPostingsOption::FreqAndPositions;
    use std::iter;
    use datastruct::stacker::Heap;
    use fastfield::FastFieldReader;
    use query::TermQuery;
    use schema::Field;
    use test::{self, Bencher};
    use indexer::operation::AddOperation;
    use tests;
    use rand::{XorShiftRng, Rng, SeedableRng};

    #[test]
    pub fn test_position_write() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut segment = index.new_segment();
        let mut posting_serializer = InvertedIndexSerializer::open(&mut segment).unwrap();
        {
            let mut field_serializer = posting_serializer.new_field(text_field);
            field_serializer.new_term("abc".as_bytes()).unwrap();
            for doc_id in 0u32..120u32 {
                let delta_positions = vec![1, 2, 3, 2];
                field_serializer.write_doc(doc_id, 2, &delta_positions).unwrap();
            }
            field_serializer.close_term().unwrap();
        }
        posting_serializer.close().unwrap();
        let read = segment.open_read(SegmentComponent::POSITIONS).unwrap();
        assert!(read.len() <= 140);
    }

    #[test]
    pub fn test_position_and_fieldnorm1() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        let segment = index.new_segment();

        let heap = Heap::with_capacity(10_000_000);
        {
            let mut segment_writer = SegmentWriter::for_segment(&heap, 18, segment.clone(), &schema)
                .unwrap();
            {
                let mut doc = Document::default();
                // checking that position works if the field has two values
                doc.add_text(text_field, "a b a c a d a a.");
                doc.add_text(text_field, "d d d d a");
                let op = AddOperation {
                    opstamp: 0u64,
                    document: doc,
                };
                segment_writer.add_document(&op, &schema).unwrap();
            }
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "b a");
                let op = AddOperation {
                    opstamp: 1u64,
                    document: doc,
                };
                segment_writer.add_document(&op, &schema).unwrap();
            }
            for i in 2..1000 {
                let mut doc = Document::default();
                let mut text = iter::repeat("e ").take(i).collect::<String>();
                text.push_str(" a");
                doc.add_text(text_field, &text);
                let op = AddOperation {
                    opstamp: 2u64,
                    document: doc,
                };
                segment_writer.add_document(&op, &schema).unwrap();
            }
            segment_writer.finalize().unwrap();
        }
        {
            let segment_reader = SegmentReader::open(segment).unwrap();
            {
                let fieldnorm_reader = segment_reader.get_fieldnorms_reader(text_field).unwrap();
                assert_eq!(fieldnorm_reader.get(0), 8 + 5);
                assert_eq!(fieldnorm_reader.get(1), 2);
                for i in 2..1000 {
                    assert_eq!(fieldnorm_reader.get(i), (i + 1) as u64);
                }
            }
            {
                let term_a = Term::from_field_text(text_field, "abcdef");
                assert!(segment_reader
                            .read_postings(&term_a, FreqAndPositions)
                            .is_none());
            }
            {
                let term_a = Term::from_field_text(text_field, "a");
                let mut postings_a = segment_reader
                    .read_postings(&term_a, FreqAndPositions)
                    .unwrap();
                assert_eq!(postings_a.len(), 1000);
                assert!(postings_a.advance());
                assert_eq!(postings_a.doc(), 0);
                assert_eq!(postings_a.term_freq(), 6);
                assert_eq!(postings_a.positions(), [0, 2, 4, 6, 7, 13]);
                assert_eq!(postings_a.positions(), [0, 2, 4, 6, 7, 13]);
                assert!(postings_a.advance());
                assert_eq!(postings_a.doc(), 1u32);
                assert_eq!(postings_a.term_freq(), 1);
                for i in 2u32..1000u32 {
                    assert!(postings_a.advance());
                    assert_eq!(postings_a.term_freq(), 1);
                    assert_eq!(postings_a.positions(), [i]);
                    assert_eq!(postings_a.doc(), i);
                }
                assert!(!postings_a.advance());
            }
            {
                let term_e = Term::from_field_text(text_field, "e");
                let mut postings_e = segment_reader
                    .read_postings(&term_e, FreqAndPositions)
                    .unwrap();
                assert_eq!(postings_e.len(), 1000 - 2);
                for i in 2u32..1000u32 {
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
                index_writer.add_document(doc);
            }
            {
                let mut doc = Document::default();
                doc.add_text(text_field, "g a b b a d c g c");
                index_writer.add_document(doc);
            }
            assert!(index_writer.commit().is_ok());
        }
        index.load_searchers().unwrap();
        let term_query = TermQuery::new(Term::from_field_text(text_field, "a"),
                                        SegmentPostingsOption::NoFreq);
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
    fn test_skip_next() {
        let term_0 = Term::from_field_u64(Field(0), 0);
        let term_1 = Term::from_field_u64(Field(0), 1);
        let term_2 = Term::from_field_u64(Field(0), 2);

        let num_docs = 300u32;

        let index = {
            let mut schema_builder = SchemaBuilder::default();
            let value_field = schema_builder.add_u64_field("value", INT_INDEXED);
            let schema = schema_builder.build();

            let index = Index::create_in_ram(schema);
            {
                let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
                for i in 0..num_docs {
                    let mut doc = Document::default();
                    doc.add_u64(value_field, 2);
                    doc.add_u64(value_field, (i % 2) as u64);

                    index_writer.add_document(doc);
                }
                assert!(index_writer.commit().is_ok());
            }
            index.load_searchers().unwrap();

            index
        };
        let searcher = index.searcher();
        let segment_reader = searcher.segment_reader(0);

        // check that the basic usage works
        for i in 0..num_docs - 1 {
            for j in i + 1..num_docs {
                let mut segment_postings = segment_reader
                    .read_postings(&term_2, SegmentPostingsOption::NoFreq)
                    .unwrap();

                assert_eq!(segment_postings.skip_next(i), SkipResult::Reached);
                assert_eq!(segment_postings.doc(), i);

                assert_eq!(segment_postings.skip_next(j), SkipResult::Reached);
                assert_eq!(segment_postings.doc(), j);
            }
        }

        {
            let mut segment_postings = segment_reader
                .read_postings(&term_2, SegmentPostingsOption::NoFreq)
                .unwrap();

            // check that `skip_next` advances the iterator
            assert!(segment_postings.advance());
            assert_eq!(segment_postings.doc(), 0);

            assert_eq!(segment_postings.skip_next(1), SkipResult::Reached);
            assert_eq!(segment_postings.doc(), 1);

            assert_eq!(segment_postings.skip_next(1), SkipResult::OverStep);
            assert_eq!(segment_postings.doc(), 2);

            // check that going beyond the end is handled
            assert_eq!(segment_postings.skip_next(num_docs), SkipResult::End);
        }

        // check that filtering works
        {
            let mut segment_postings = segment_reader
                .read_postings(&term_0, SegmentPostingsOption::NoFreq)
                .unwrap();

            for i in 0..num_docs / 2 {
                assert_eq!(segment_postings.skip_next(i * 2), SkipResult::Reached);
                assert_eq!(segment_postings.doc(), i * 2);
            }

            let mut segment_postings = segment_reader
                .read_postings(&term_0, SegmentPostingsOption::NoFreq)
                .unwrap();

            for i in 0..num_docs / 2 - 1 {
                assert_eq!(segment_postings.skip_next(i * 2 + 1), SkipResult::OverStep);
                assert_eq!(segment_postings.doc(), (i + 1) * 2);
            }
        }

        // delete some of the documents
        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            index_writer.delete_term(term_0);

            assert!(index_writer.commit().is_ok());
        }
        index.load_searchers().unwrap();

        let searcher = index.searcher();
        let segment_reader = searcher.segment_reader(0);

        // make sure seeking still works
        for i in 0..num_docs {
            let mut segment_postings = segment_reader
                .read_postings(&term_2, SegmentPostingsOption::NoFreq)
                .unwrap();

            if i % 2 == 0 {
                assert_eq!(segment_postings.skip_next(i), SkipResult::OverStep);
                assert_eq!(segment_postings.doc(), i + 1);
            } else {
                assert_eq!(segment_postings.skip_next(i), SkipResult::Reached);
                assert_eq!(segment_postings.doc(), i);
            }
        }

        // now try with a longer sequence
        {
            let mut segment_postings = segment_reader
                .read_postings(&term_2, SegmentPostingsOption::NoFreq)
                .unwrap();

            let mut last = 2; // start from 5 to avoid seeking to 3 twice
            let mut cur = 3;
            loop {
                match segment_postings.skip_next(cur) {
                    SkipResult::End => break,
                    SkipResult::Reached => assert_eq!(segment_postings.doc(), cur),
                    SkipResult::OverStep => assert_eq!(segment_postings.doc(), cur + 1),
                }

                let next = cur + last;
                last = cur;
                cur = next;
            }

            assert_eq!(cur, 377);
        }

        // delete everything else
        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            index_writer.delete_term(term_1);

            assert!(index_writer.commit().is_ok());
        }
        index.load_searchers().unwrap();

        let searcher = index.searcher();
        let segment_reader = searcher.segment_reader(0);

        // finally, check that it's empty
        {
            let mut segment_postings = segment_reader
                .read_postings(&term_2, SegmentPostingsOption::NoFreq)
                .unwrap();

            assert_eq!(segment_postings.skip_next(0), SkipResult::End);

            let mut segment_postings = segment_reader
                .read_postings(&term_2, SegmentPostingsOption::NoFreq)
                .unwrap();

            assert_eq!(segment_postings.skip_next(num_docs), SkipResult::End);
        }
    }


    lazy_static! {
        static ref TERM_A: Term = {
            let field = Field(0);
            Term::from_field_text(field, "a")
        };
        static ref TERM_B: Term = {
            let field = Field(0);
            Term::from_field_text(field, "b")
        };
        static ref TERM_C: Term = {
            let field = Field(0);
            Term::from_field_text(field, "c")
        };
        static ref TERM_D: Term = {
            let field = Field(0);
            Term::from_field_text(field, "d")
        };
        static ref INDEX: Index = {
            let mut schema_builder = SchemaBuilder::default();
            let text_field = schema_builder.add_text_field("text", STRING);
            let schema = schema_builder.build();

            let seed: &[u32; 4] = &[1, 2, 3, 4];
            let mut rng: XorShiftRng = XorShiftRng::from_seed(*seed);

            let index = Index::create_in_ram(schema);
            let posting_list_size = 1_000_000;
            {
                let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
                for _ in 0 .. posting_list_size {
                    let mut doc = Document::default();
                    if rng.gen_weighted_bool(15) {
                        doc.add_text(text_field, "a");
                    }
                    if rng.gen_weighted_bool(10) {
                        doc.add_text(text_field, "b");
                    }
                    if rng.gen_weighted_bool(5) {
                        doc.add_text(text_field, "c");
                    }
                    if rng.gen_weighted_bool(1) {
                        doc.add_text(text_field, "d");
                    }
                    index_writer.add_document(doc);
                }
                assert!(index_writer.commit().is_ok());
            }
            index.load_searchers().unwrap();
            index
        };
    }

    #[bench]
    fn bench_segment_postings(b: &mut Bencher) {
        let searcher = INDEX.searcher();
        let segment_reader = searcher.segment_reader(0);

        b.iter(|| {
                   let mut segment_postings = segment_reader
                       .read_postings(&*TERM_A, SegmentPostingsOption::NoFreq)
                       .unwrap();
                   while segment_postings.advance() {}
               });
    }

    #[bench]
    fn bench_segment_intersection(b: &mut Bencher) {
        let searcher = INDEX.searcher();
        let segment_reader = searcher.segment_reader(0);
        b.iter(|| {
            let segment_postings_a = segment_reader
                .read_postings(&*TERM_A, SegmentPostingsOption::NoFreq)
                .unwrap();
            let segment_postings_b = segment_reader
                .read_postings(&*TERM_B, SegmentPostingsOption::NoFreq)
                .unwrap();
            let segment_postings_c = segment_reader
                .read_postings(&*TERM_C, SegmentPostingsOption::NoFreq)
                .unwrap();
            let segment_postings_d = segment_reader
                .read_postings(&*TERM_D, SegmentPostingsOption::NoFreq)
                .unwrap();
            let mut intersection = IntersectionDocSet::from(vec![segment_postings_a,
                                                                 segment_postings_b,
                                                                 segment_postings_c,
                                                                 segment_postings_d]);
            while intersection.advance() {}
        });
    }

    fn bench_skip_next(p: f32, b: &mut Bencher) {
        let searcher = INDEX.searcher();
        let segment_reader = searcher.segment_reader(0);
        let docs = tests::sample(segment_reader.num_docs(), p);

        let mut segment_postings = segment_reader
            .read_postings(&*TERM_A, SegmentPostingsOption::NoFreq)
            .unwrap();

        let mut existing_docs = Vec::new();
        segment_postings.advance();
        for doc in &docs {
            if *doc >= segment_postings.doc() {
                existing_docs.push(*doc);
                if segment_postings.skip_next(*doc) == SkipResult::End {
                    break;
                }
            }
        }

        b.iter(|| {
            let mut segment_postings = segment_reader
                .read_postings(&*TERM_A, SegmentPostingsOption::NoFreq)
                .unwrap();
            for doc in &existing_docs {
                if segment_postings.skip_next(*doc) == SkipResult::End {
                    break;
                }
            }
        });
    }

    #[bench]
    fn bench_skip_next_p01(b: &mut Bencher) {
        bench_skip_next(0.001, b);
    }

    #[bench]
    fn bench_skip_next_p1(b: &mut Bencher) {
        bench_skip_next(0.01, b);
    }

    #[bench]
    fn bench_skip_next_p10(b: &mut Bencher) {
        bench_skip_next(0.1, b);
    }

    #[bench]
    fn bench_skip_next_p90(b: &mut Bencher) {
        bench_skip_next(0.9, b);
    }

    #[bench]
    fn bench_iterate_segment_postings(b: &mut Bencher) {
        let searcher = INDEX.searcher();
        let segment_reader = searcher.segment_reader(0);
        b.iter(|| {
            let n: u32 = test::black_box(17);
            let mut segment_postings = segment_reader
                .read_postings(&*TERM_A, SegmentPostingsOption::NoFreq)
                .unwrap();
            let mut s = 0u32;
            while segment_postings.advance() {
                s += (segment_postings.doc() & n) % 1024;
            }
            s
        });
    }

}
