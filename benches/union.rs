use criterion::{criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, STRING};
use tantivy::{Document, Index};

fn bench_union(criterion: &mut Criterion) {
    criterion.bench_function_over_inputs(
        "union_docset_fulladvance",
        |bench, (ratio_left, ratio_right)| {
            let mut schema_builder = Schema::builder();
            let field = schema_builder.add_text_field("val", STRING);
            let schema = schema_builder.build();
            let index = Index::create_in_ram(schema);
            let mut index_writer = index.writer_with_num_threads(1, 80_000_000).unwrap();
            let mut stdrng = StdRng::from_seed([0u8; 32]);
            for _ in 0u32..100_000u32 {
                let mut doc = Document::default();
                if stdrng.gen_bool(*ratio_left) {
                    doc.add_text(field, "left");
                }
                if stdrng.gen_bool(*ratio_right) {
                    doc.add_text(field, "right");
                }
                index_writer.add_document(doc);
            }
            index_writer.commit().unwrap();
            let reader = index.reader().unwrap();
            let searcher = reader.searcher();

            let query = QueryParser::for_index(&index, vec![field])
                .parse_query("left right")
                .unwrap();

            bench.iter(move || {
                let weight = query.weight(&searcher, false).unwrap();
                let mut scorer = weight.scorer(searcher.segment_reader(0u32)).unwrap();
                let mut sum_docs = 0u64;
                scorer.for_each(&mut |doc_id, _score| {
                    sum_docs += doc_id as u64;
                });
            });
        },
        vec![(0.2, 0.1), (0.2, 0.02)],
    );
}

criterion_group!(benches, bench_union);
criterion_main!(benches);
