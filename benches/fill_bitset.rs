use binggan::{black_box, BenchRunner, PeakMemAlloc, INSTRUMENTED_SYSTEM};
use common::BitSet;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tantivy::postings::BlockSegmentPostings;
use tantivy::schema::*;
use tantivy::{
    doc, DocSet, Index, InvertedIndexReader, TantivyDocument, TantivyInvertedIndexReader,
};

#[global_allocator]
pub static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

fn main() {
    let index = build_test_index();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    let segment_reader = &searcher.segment_readers()[0];
    let text_field = index.schema().get_field("text").unwrap();
    let inverted_index = segment_reader.inverted_index(text_field).unwrap();
    let max_doc = segment_reader.max_doc();

    let term = Term::from_field_text(text_field, "hello");
    let term_info = inverted_index.get_term_info(&term).unwrap().unwrap();

    let mut runner = BenchRunner::new();
    runner.set_name("fill_bitset");

    let mut group = runner.new_group();
    {
        let inverted_index = &inverted_index;
        let term_info = &term_info;
        // This is the path used by queries (AutomatonWeight, RangeQuery, etc.)
        // It dispatches via DynInvertedIndexReader::fill_bitset_from_terminfo.
        group.register("fill_bitset_from_terminfo (via trait)", move |_| {
            let mut bitset = BitSet::with_max_value(max_doc);
            inverted_index
                .fill_bitset_from_terminfo(term_info, &mut bitset)
                .unwrap();
            black_box(bitset);
        });
    }
    {
        let inverted_index = &inverted_index;
        let term_info = &term_info;
        // This constructs a SegmentPostings via read_docset_from_terminfo and calls fill_bitset.
        group.register("read_docset + fill_bitset", move |_| {
            let mut postings = inverted_index.read_docset_from_terminfo(term_info).unwrap();
            let mut bitset = BitSet::with_max_value(max_doc);
            postings.fill_bitset(&mut bitset);
            black_box(bitset);
        });
    }
    {
        let inverted_index = &inverted_index;
        let term_info = &term_info;
        // This uses BlockSegmentPostings directly, bypassing SegmentPostings entirely.
        let concrete_reader = inverted_index
            .as_any()
            .downcast_ref::<TantivyInvertedIndexReader>()
            .expect("expected TantivyInvertedIndexReader");
        group.register("BlockSegmentPostings direct", move |_| {
            let raw = concrete_reader
                .read_raw_postings_data(term_info, IndexRecordOption::Basic)
                .unwrap();
            let mut block_postings = BlockSegmentPostings::open(
                term_info.doc_freq,
                raw.postings_data,
                raw.record_option,
                raw.effective_option,
            )
            .unwrap();
            let mut bitset = BitSet::with_max_value(max_doc);
            loop {
                let docs = block_postings.docs();
                if docs.is_empty() {
                    break;
                }
                for &doc in docs {
                    bitset.insert(doc);
                }
                block_postings.advance();
            }
            black_box(bitset);
        });
    }
    group.run();
}

fn build_test_index() -> Index {
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("text", TEXT);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let text_field = schema.get_field("text").unwrap();

    let mut writer = index.writer::<TantivyDocument>(250_000_000).unwrap();
    let mut rng = StdRng::from_seed([42u8; 32]);
    for _ in 0..100_000 {
        if rng.random_bool(0.5) {
            writer
                .add_document(doc!(text_field => "hello world"))
                .unwrap();
        } else {
            writer
                .add_document(doc!(text_field => "goodbye world"))
                .unwrap();
        }
    }
    writer.commit().unwrap();
    index
}
