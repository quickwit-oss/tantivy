// Benchmarks `InvertedIndexReader` term-info lookup memoization across:
// - Cold readers: each timed iteration pops a pre-opened `Searcher` whose `InvertedIndexReader`s
//   have an empty memo, measuring intra-query deduplication (`warm_postings` +
//   `Bm25Weight::doc_freq` + `TermWeight`).
// - Warm readers: a persistent `Searcher` serves repeated query batches, measuring cross-query
//   reuse of remembered `TermInfo` entries (including absent terms).
//
// Run with:
//   cargo bench --features quickwit --bench term_info_lookup
//   cargo bench --bench term_info_lookup

use std::cell::RefCell;

use binggan::{black_box, BenchRunner};
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tantivy::collector::TopDocs;
use tantivy::indexer::NoMergePolicy;
use tantivy::query::BooleanQuery;
use tantivy::schema::{Field, Schema, TEXT};
use tantivy::{doc, Index, IndexReader, ReloadPolicy, Searcher, Term};

const NUM_SEGMENTS: usize = 8;
const DOCS_PER_SEGMENT: usize = 4_000;
const VOCAB_SIZE: usize = 32_000;
const TERMS_PER_DOC: usize = 8;
const NUM_QUERIES: usize = 512;
const COLD_GROUP_ITERS: usize = 10;

struct PreparedQuery {
    #[cfg_attr(not(feature = "quickwit"), allow(dead_code))]
    terms: Vec<Term>,
    query: BooleanQuery,
}

struct BenchFixture {
    field: Field,
    reader: IndexReader,
    queries: Vec<PreparedQuery>,
}

fn build_fixture() -> BenchFixture {
    let mut schema_builder = Schema::builder();
    let field = schema_builder.add_text_field("body", TEXT);
    let index = Index::create_in_ram(schema_builder.build());

    let mut rng = StdRng::from_seed([17u8; 32]);
    {
        let mut writer = index.writer_with_num_threads(1, 200_000_000).unwrap();
        writer.set_merge_policy(Box::new(NoMergePolicy));
        for _ in 0..NUM_SEGMENTS {
            for _ in 0..DOCS_PER_SEGMENT {
                let mut words = String::with_capacity(TERMS_PER_DOC * 10);
                for i in 0..TERMS_PER_DOC {
                    if i > 0 {
                        words.push(' ');
                    }
                    let term_id = rng.random_range(0..VOCAB_SIZE);
                    words.push_str(&format!("term{term_id:05}"));
                }
                writer.add_document(doc!(field => words)).unwrap();
            }
            writer.commit().unwrap();
        }
    }

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();

    // 512 distinct 4-term queries (3 present terms + 1 absent term = 2,048 distinct terms),
    // so a cold batch never repeats a term across queries while staying under the memo limit.
    let mut queries = Vec::with_capacity(NUM_QUERIES);
    for q in 0..NUM_QUERIES {
        let base = (q * 3) % VOCAB_SIZE;
        let terms = vec![
            Term::from_field_text(field, &format!("term{:05}", base)),
            Term::from_field_text(field, &format!("term{:05}", (base + 10_007) % VOCAB_SIZE)),
            Term::from_field_text(field, &format!("term{:05}", (base + 20_011) % VOCAB_SIZE)),
            Term::from_field_text(field, &format!("miss{q:05}")),
        ];
        let query = BooleanQuery::new_multiterms_query(terms.clone());
        queries.push(PreparedQuery { terms, query });
    }

    BenchFixture {
        field,
        reader,
        queries,
    }
}

/// Pre-opens `count` independent `Searcher`s outside the timed loop so `reload()` itself is not
/// included in cold-reader measurements.
fn preopen_cold_searchers(reader: &IndexReader, count: usize) -> RefCell<Vec<Searcher>> {
    let mut searchers = Vec::with_capacity(count);
    for _ in 0..count {
        reader.reload().unwrap();
        searchers.push(reader.searcher());
    }
    RefCell::new(searchers)
}

fn next_cold_searcher(pool: &RefCell<Vec<Searcher>>, reader: &IndexReader) -> Searcher {
    pool.borrow_mut().pop().unwrap_or_else(|| {
        reader.reload().unwrap();
        reader.searcher()
    })
}

fn run_bm25_batch(searcher: &Searcher, queries: &[PreparedQuery]) -> usize {
    let mut hits = 0usize;
    for prepared in queries {
        let top_docs = searcher
            .search(&prepared.query, &TopDocs::with_limit(10).order_by_score())
            .unwrap();
        hits += top_docs.len();
    }
    hits
}

#[cfg(feature = "quickwit")]
fn run_warm_and_bm25_batch(searcher: &Searcher, field: Field, queries: &[PreparedQuery]) -> usize {
    let mut hits = 0usize;
    for prepared in queries {
        futures::executor::block_on(async {
            for segment_reader in searcher.segment_readers() {
                let inv_idx = segment_reader.inverted_index(field).unwrap();
                for term in &prepared.terms {
                    inv_idx.warm_postings(term, false).await.unwrap();
                }
            }
        });
        let top_docs = searcher
            .search(&prepared.query, &TopDocs::with_limit(10).order_by_score())
            .unwrap();
        hits += top_docs.len();
    }
    hits
}

fn main() {
    let fixture = build_fixture();
    let _ = fixture.field;

    {
        // `binggan` runs `COLD_GROUP_ITERS` timed calls plus 1 output-check call per bench.
        let cold_bm25_pool = preopen_cold_searchers(&fixture.reader, COLD_GROUP_ITERS + 4);
        #[cfg(feature = "quickwit")]
        let cold_warm_pool = preopen_cold_searchers(&fixture.reader, COLD_GROUP_ITERS + 4);

        let mut cold_runner = BenchRunner::new();
        cold_runner.config().num_iter_group = Some(COLD_GROUP_ITERS);
        cold_runner.config().num_iter_bench = Some(1);
        let mut group = cold_runner.new_group();
        group.set_name("cold_reader_512q");

        let reader = fixture.reader.clone();
        let queries_ref = &fixture.queries;
        group.register("bm25_top10", move |_| {
            let searcher = next_cold_searcher(&cold_bm25_pool, &reader);
            black_box(run_bm25_batch(&searcher, queries_ref))
        });

        #[cfg(feature = "quickwit")]
        {
            let reader = fixture.reader.clone();
            let field = fixture.field;
            group.register("warm_postings_and_bm25_top10", move |_| {
                let searcher = next_cold_searcher(&cold_warm_pool, &reader);
                black_box(run_warm_and_bm25_batch(&searcher, field, queries_ref))
            });
        }

        group.run();
    }

    {
        let warm_searcher = fixture.reader.searcher();
        // Populate `TermInfoMemo` once before timed iterations.
        black_box(run_bm25_batch(&warm_searcher, &fixture.queries));

        let mut warm_runner = BenchRunner::new();
        let mut group = warm_runner.new_group();
        group.set_name("warm_reader_512q");

        let searcher = warm_searcher.clone();
        let queries_ref = &fixture.queries;
        group.register("bm25_top10", move |_| {
            black_box(run_bm25_batch(&searcher, queries_ref))
        });

        #[cfg(feature = "quickwit")]
        {
            let searcher = warm_searcher.clone();
            let field = fixture.field;
            group.register("warm_postings_and_bm25_top10", move |_| {
                black_box(run_warm_and_bm25_batch(&searcher, field, queries_ref))
            });
        }

        group.run();
    }
}
