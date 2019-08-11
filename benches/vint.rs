use criterion::{criterion_group, criterion_main, Criterion, ParameterizedBenchmark};
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use tantivy::forbench::compression::{compressed_block_size, BlockDecoder};
use tantivy::forbench::compression::{BlockEncoder, VIntEncoder};
use tantivy::forbench::compression::{VIntDecoder, COMPRESSION_BLOCK_SIZE};

fn generate_array_with_seed(n: usize, ratio: f64, seed_val: u8) -> Vec<u32> {
    let seed: [u8; 32] = [seed_val; 32];
    let mut rng = StdRng::from_seed(seed);
    (0u32..).filter(|_| rng.gen_bool(ratio)).take(n).collect()
}

pub fn generate_array(n: usize, ratio: f64) -> Vec<u32> {
    generate_array_with_seed(n, ratio, 4)
}

fn bench_compress(criterion: &mut Criterion) {
    criterion.bench(
        "compress_sorted",
        ParameterizedBenchmark::new(
            "bitpack",
            |bench, ratio| {
                let mut encoder = BlockEncoder::new();
                let data = generate_array(COMPRESSION_BLOCK_SIZE, *ratio);
                bench.iter(|| {
                    encoder.compress_block_sorted(&data, 0u32);
                });
            },
            vec![0.1],
        )
        .with_function("vint", |bench, ratio| {
            let mut encoder = BlockEncoder::new();
            let data = generate_array(COMPRESSION_BLOCK_SIZE, *ratio);
            bench.iter(|| {
                encoder.compress_vint_sorted(&data, 0u32);
            });
        }),
    );
}

fn bench_uncompress(criterion: &mut Criterion) {
    criterion.bench(
        "uncompress_sorted",
        ParameterizedBenchmark::new(
            "bitpack",
            |bench, ratio| {
                let mut encoder = BlockEncoder::new();
                let data = generate_array(COMPRESSION_BLOCK_SIZE, *ratio);
                let (num_bits, compressed) = encoder.compress_block_sorted(&data, 0u32);
                let mut decoder = BlockDecoder::new();
                bench.iter(|| {
                    decoder.uncompress_block_sorted(compressed, 0u32, num_bits);
                });
            },
            vec![0.1],
        )
        .with_function("vint", |bench, ratio| {
            let mut encoder = BlockEncoder::new();
            let data = generate_array(COMPRESSION_BLOCK_SIZE, *ratio);
            let compressed = encoder.compress_vint_sorted(&data, 0u32);
            let mut decoder = BlockDecoder::new();
            bench.iter(move || {
                decoder.uncompress_vint_sorted(compressed, 0u32, COMPRESSION_BLOCK_SIZE);
            });
        }),
    );
}

criterion_group!(benches, bench_compress, bench_uncompress);
criterion_main!(benches);
