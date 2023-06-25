use criterion::{criterion_group, criterion_main, Criterion};
use tantivy::tokenizer::{
    BoxTokenFilter, LowerCaser, RemoveLongFilter, SimpleTokenizer, TextAnalyzer, TokenizerManager,
};

const ALICE_TXT: &str = include_str!("alice.txt");

pub fn criterion_benchmark(c: &mut Criterion) {
    let tokenizer_manager = TokenizerManager::default();
    let mut tokenizer = tokenizer_manager.get("default").unwrap();
    c.bench_function("default-tokenize-alice", |b| {
        b.iter(|| {
            let mut word_count = 0;
            let mut token_stream = tokenizer.token_stream(ALICE_TXT);
            while token_stream.advance() {
                word_count += 1;
            }
            assert_eq!(word_count, 30_731);
        })
    });
    let token_filters = vec![
        BoxTokenFilter::from(RemoveLongFilter::limit(40)),
        BoxTokenFilter::from(LowerCaser),
    ];
    let mut dynamic_analyzer = TextAnalyzer::new(SimpleTokenizer::default(), token_filters);
    c.bench_function("default-dynamic-tokenize-alice", |b| {
        b.iter(|| {
            let mut word_count = 0;
            let mut token_stream = dynamic_analyzer.token_stream(ALICE_TXT);
            while token_stream.advance() {
                word_count += 1;
            }
            assert_eq!(word_count, 30_731);
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(200);
    targets = criterion_benchmark
}
criterion_main!(benches);
