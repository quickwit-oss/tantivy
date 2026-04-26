// Benchmarks top-K intersection of term scorers (block_wand_intersection).
//
// What's measured:
// - Conjunctive queries (+a +b, +a +b +c) with top-10 by score
// - Varying doc-frequency balance between terms (balanced, skewed, very skewed)
// - Realistic term frequencies (geometric distribution, mostly low)
// - 1M-doc single segment
//
// Run with: cargo bench --bench intersection_bench

use binggan::{black_box, BenchRunner};
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, TEXT};
use tantivy::{doc, Index, ReloadPolicy, Searcher};

const NUM_DOCS: usize = 1_000_000;

struct BenchIndex {
    searcher: Searcher,
    query_parser: QueryParser,
}

/// Generate term frequency from a geometric-like distribution.
/// Most values are 1, a few are 2-3, rarely higher.
/// p controls the decay: higher p → more weight on tf=1.
fn random_term_freq(rng: &mut StdRng, p: f64) -> u32 {
    let mut tf = 1u32;
    while tf < 10 && rng.random_bool(1.0 - p) {
        tf += 1;
    }
    tf
}

/// Build an index with three terms (a, b, c) with given doc-frequency probabilities.
/// Each term occurrence has a realistic term frequency (geometric distribution).
/// Field length is padded with filler tokens to create varied fieldnorms.
fn build_index(p_a: f64, p_b: f64, p_c: f64) -> BenchIndex {
    let mut schema_builder = Schema::builder();
    let body = schema_builder.add_text_field("body", TEXT);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);

    let mut rng = StdRng::from_seed([42u8; 32]);

    {
        let mut writer = index.writer_with_num_threads(1, 500_000_000).unwrap();
        for _ in 0..NUM_DOCS {
            let mut tokens: Vec<String> = Vec::new();

            if rng.random_bool(p_a) {
                let tf = random_term_freq(&mut rng, 0.7);
                for _ in 0..tf {
                    tokens.push("aaa".to_string());
                }
            }
            if rng.random_bool(p_b) {
                let tf = random_term_freq(&mut rng, 0.7);
                for _ in 0..tf {
                    tokens.push("bbb".to_string());
                }
            }
            if rng.random_bool(p_c) {
                let tf = random_term_freq(&mut rng, 0.7);
                for _ in 0..tf {
                    tokens.push("ccc".to_string());
                }
            }

            // Pad with filler to create varied field lengths (5-30 tokens).
            let filler_count = rng.random_range(5u32..30u32);
            for _ in 0..filler_count {
                tokens.push("filler".to_string());
            }

            let text = tokens.join(" ");
            writer.add_document(doc!(body => text)).unwrap();
        }
        writer.commit().unwrap();
    }

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();
    let searcher = reader.searcher();
    let query_parser = QueryParser::for_index(&index, vec![body]);

    BenchIndex {
        searcher,
        query_parser,
    }
}

fn main() {
    // Scenarios: (label, p_a, p_b, p_c)
    //
    // "balanced":    all terms ~10% → intersection ~1% of docs
    // "skewed":      one common (50%), one rare (2%) → intersection ~1%
    // "very_skewed": one very common (80%), one very rare (0.5%) → intersection ~0.4%
    // "three_balanced": three terms ~20% each → intersection ~0.8%
    // "three_skewed":   50% / 10% / 2% → intersection ~0.1%
    let scenarios: Vec<(&str, f64, f64, f64)> = vec![
        ("balanced_10%_10%", 0.10, 0.10, 0.0),
        ("skewed_50%_2%", 0.50, 0.02, 0.0),
        ("very_skewed_80%_0.5%", 0.80, 0.005, 0.0),
        ("three_balanced_20%_20%_20%", 0.20, 0.20, 0.20),
        ("three_skewed_50%_10%_2%", 0.50, 0.10, 0.02),
    ];

    let mut runner = BenchRunner::new();

    for (label, p_a, p_b, p_c) in &scenarios {
        let bench_index = build_index(*p_a, *p_b, *p_c);

        let mut group = runner.new_group();
        group.set_name(format!("intersection — {label}"));

        // Two-term intersection
        if *p_a > 0.0 && *p_b > 0.0 {
            let query_str = "+aaa +bbb";
            let query = bench_index.query_parser.parse_query(query_str).unwrap();
            let searcher = bench_index.searcher.clone();
            group.register(format!("{query_str} top10"), move |_| {
                let collector = TopDocs::with_limit(10).order_by_score();
                black_box(searcher.search(&query, &collector).unwrap());
                1usize
            });
        }

        // Three-term intersection
        if *p_c > 0.0 {
            let query_str = "+aaa +bbb +ccc";
            let query = bench_index.query_parser.parse_query(query_str).unwrap();
            let searcher = bench_index.searcher.clone();
            group.register(format!("{query_str} top10"), move |_| {
                let collector = TopDocs::with_limit(10).order_by_score();
                black_box(searcher.search(&query, &collector).unwrap());
                1usize
            });
        }

        group.run();
    }
}
