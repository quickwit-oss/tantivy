// Benchmarks boolean conjunction queries using binggan.
//
// What’s measured:
// - Or and And queries with varying selectivity (only `Term` queries for now on leafs)
// - Nested AND/OR combinations (on multiple fields)
// - No-scoring path using the Count collector (focus on iterator/skip performance)
// - Top-K retrieval (k=10) using the TopDocs collector
//
// Corpus model:
// - Synthetic docs; each token a/b/c is independently included per doc
// - If none of a/b/c are included, emit a neutral filler token to keep doc length similar
//
// Notes:
// - After optimization, when scoring is disabled Tantivy reads doc-only postings
//   (IndexRecordOption::Basic), avoiding frequency decoding overhead.
// - This bench isolates boolean iteration speed and intersection/union cost.
// - Use `cargo bench --bench boolean_conjunction` to run.

use binggan::{black_box, BenchRunner};
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, TEXT};
use tantivy::{doc, Index, ReloadPolicy, Searcher};

#[derive(Clone)]
struct BenchIndex {
    #[allow(dead_code)]
    index: Index,
    searcher: Searcher,
    query_parser: QueryParser,
}

impl BenchIndex {
    #[inline(always)]
    fn count_query(&self, query_str: &str) -> usize {
        let query = self.query_parser.parse_query(query_str).unwrap();
        self.searcher.search(&query, &Count).unwrap()
    }

    #[inline(always)]
    fn topk_len(&self, query_str: &str, k: usize) -> usize {
        let query = self.query_parser.parse_query(query_str).unwrap();
        self.searcher
            .search(&query, &TopDocs::with_limit(k))
            .unwrap()
            .len()
    }
}

/// Build a single index containing both fields (title, body) and
/// return two BenchIndex views:
/// - single_field: QueryParser defaults to only "body"
/// - multi_field:  QueryParser defaults to ["title", "body"]
fn build_shared_indices(num_docs: usize, p_a: f32, p_b: f32, p_c: f32) -> (BenchIndex, BenchIndex) {
    // Unified schema (two text fields)
    let mut schema_builder = Schema::builder();
    let f_title = schema_builder.add_text_field("title", TEXT);
    let f_body = schema_builder.add_text_field("body", TEXT);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());

    // Populate index with stable RNG for reproducibility.
    let mut rng = StdRng::from_seed([7u8; 32]);

    // Populate: spread each present token 90/10 to body/title
    {
        let mut writer = index.writer(500_000_000).unwrap();
        for _ in 0..num_docs {
            let has_a = rng.gen_bool(p_a as f64);
            let has_b = rng.gen_bool(p_b as f64);
            let has_c = rng.gen_bool(p_c as f64);
            let mut title_tokens: Vec<&str> = Vec::new();
            let mut body_tokens: Vec<&str> = Vec::new();
            if has_a {
                if rng.gen_bool(0.1) {
                    title_tokens.push("a");
                } else {
                    body_tokens.push("a");
                }
            }
            if has_b {
                if rng.gen_bool(0.1) {
                    title_tokens.push("b");
                } else {
                    body_tokens.push("b");
                }
            }
            if has_c {
                if rng.gen_bool(0.1) {
                    title_tokens.push("c");
                } else {
                    body_tokens.push("c");
                }
            }
            if title_tokens.is_empty() && body_tokens.is_empty() {
                body_tokens.push("z");
            }
            writer
                .add_document(doc!(
                    f_title=>title_tokens.join(" "),
                    f_body=>body_tokens.join(" ")
                ))
                .unwrap();
        }
        writer.commit().unwrap();
    }

    // Prepare reader/searcher once.
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();
    let searcher = reader.searcher();

    // Build two query parsers with different default fields.
    let qp_single = QueryParser::for_index(&index, vec![f_body]);
    let qp_multi = QueryParser::for_index(&index, vec![f_title, f_body]);

    let single_view = BenchIndex {
        index: index.clone(),
        searcher: searcher.clone(),
        query_parser: qp_single,
    };
    let multi_view = BenchIndex {
        index,
        searcher,
        query_parser: qp_multi,
    };
    (single_view, multi_view)
}

fn main() {
    // Prepare corpora with varying selectivity. Build one index per corpus
    // and derive two views (single-field vs multi-field) from it.
    let scenarios = vec![
        (
            "N=1M, p(a)=5%, p(b)=1%, p(c)=15%".to_string(),
            1_000_000,
            0.05,
            0.01,
            0.15,
        ),
        (
            "N=1M, p(a)=1%, p(b)=1%, p(c)=15%".to_string(),
            1_000_000,
            0.01,
            0.01,
            0.15,
        ),
    ];

    let mut runner = BenchRunner::new();
    for (label, n, pa, pb, pc) in scenarios {
        let (single_view, multi_view) = build_shared_indices(n, pa, pb, pc);

        // Single-field group: default field is body only
        {
            let mut group = runner.new_group();
            group.set_name(format!("single_field — {}", label));
            group.register_with_input("+a_+b_count", &single_view, |benv: &BenchIndex| {
                black_box(benv.count_query("+a +b"))
            });
            group.register_with_input("+a_+b_+c_count", &single_view, |benv: &BenchIndex| {
                black_box(benv.count_query("+a +b +c"))
            });
            group.register_with_input("+a_+b_top10", &single_view, |benv: &BenchIndex| {
                black_box(benv.topk_len("+a +b", 10))
            });
            group.register_with_input("+a_+b_+c_top10", &single_view, |benv: &BenchIndex| {
                black_box(benv.topk_len("+a +b +c", 10))
            });
            // OR queries
            group.register_with_input("a_OR_b_count", &single_view, |benv: &BenchIndex| {
                black_box(benv.count_query("a OR b"))
            });
            group.register_with_input("a_OR_b_OR_c_count", &single_view, |benv: &BenchIndex| {
                black_box(benv.count_query("a OR b OR c"))
            });
            group.register_with_input("a_OR_b_top10", &single_view, |benv: &BenchIndex| {
                black_box(benv.topk_len("a OR b", 10))
            });
            group.register_with_input("a_OR_b_OR_c_top10", &single_view, |benv: &BenchIndex| {
                black_box(benv.topk_len("a OR b OR c", 10))
            });
            group.run();
        }

        // Multi-field group: default fields are [title, body]
        {
            let mut group = runner.new_group();
            group.set_name(format!("multi_field — {}", label));
            group.register_with_input("+a_+b_count", &multi_view, |benv: &BenchIndex| {
                black_box(benv.count_query("+a +b"))
            });
            group.register_with_input("+a_+b_+c_count", &multi_view, |benv: &BenchIndex| {
                black_box(benv.count_query("+a +b +c"))
            });
            group.register_with_input("+a_+b_top10", &multi_view, |benv: &BenchIndex| {
                black_box(benv.topk_len("+a +b", 10))
            });
            group.register_with_input("+a_+b_+c_top10", &multi_view, |benv: &BenchIndex| {
                black_box(benv.topk_len("+a +b +c", 10))
            });
            // OR queries
            group.register_with_input("a_OR_b_count", &multi_view, |benv: &BenchIndex| {
                black_box(benv.count_query("a OR b"))
            });
            group.register_with_input("a_OR_b_OR_c_count", &multi_view, |benv: &BenchIndex| {
                black_box(benv.count_query("a OR b OR c"))
            });
            group.register_with_input("a_OR_b_top10", &multi_view, |benv: &BenchIndex| {
                black_box(benv.topk_len("a OR b", 10))
            });
            group.register_with_input("a_OR_b_OR_c_top10", &multi_view, |benv: &BenchIndex| {
                black_box(benv.topk_len("a OR b OR c", 10))
            });
            group.run();
        }
    }
}
