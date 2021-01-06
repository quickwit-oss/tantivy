use criterion::{criterion_group, criterion_main, Criterion};
use tantivy::tokenizer::TokenizerManager;

const ALICE_TXT: &'static str = include_str!("alice.txt");

pub fn criterion_benchmark(c: &mut Criterion) {
    let tokenizer_manager = TokenizerManager::default();
    let tokenizer = tokenizer_manager.get("default").unwrap();
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
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
