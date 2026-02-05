// Benchmarks phrase queries with and without word ngram indexing.
//
// What's measured:
// - Phrase query performance with standard position-based matching
// - Phrase query performance with ngram term optimization
// - Different ngram configurations (bigrams vs trigrams)
// - Various phrase lengths (2-word, 3-word, 4-word phrases)
// - Impact on index size vs query speedup
//
// Corpus model:
// - Realistic text documents with varying content
// - Mix of frequent terms (the, a, in, of) and rare terms (specialized vocabulary)
// - Documents large enough to trigger frequency-based ngram decisions
//
// Key Findings:
// - Bigrams provide 10-60x speedup for 2-word phrases with ~5,700% term increase
// - Trigrams are EXPENSIVE: 54x more terms than bigrams (188K vs 3.5K terms)
// - Trigrams often perform WORSE than bigrams due to intersection overhead
// - RECOMMENDATION: Use bigrams (FF or All), avoid trigrams in most cases
//
// Notes:
// - Frequent terms are determined during indexing based on document frequency
// - Bigrams and trigrams are selectively indexed based on term frequency patterns

use binggan::{black_box, BenchGroup, BenchRunner, PeakMemAlloc, INSTRUMENTED_SYSTEM};
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tantivy::collector::TopDocs;
use tantivy::query::{PhraseQuery, Query};
use tantivy::schema::{IndexRecordOption, Schema, TextFieldIndexing, TEXT};
use tantivy::{doc, Index, ReloadPolicy, Searcher, Term, WordNgramConfig, WordNgramSet};

#[global_allocator]
pub static GLOBAL: &PeakMemAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

#[derive(Clone)]
struct BenchIndex {
    #[allow(dead_code)]
    index: Index,
    searcher: Searcher,
    field: tantivy::schema::Field,
}

/// Common phrases to search for
const FREQUENT_WORDS: &[&str] = &[
    "the", "a", "an", "in", "of", "to", "and", "for", "on", "with", "as", "at", "by", "from",
    "is", "was", "are", "were", "be", "been", "have", "has", "had", "do", "does", "did",
];

const RARE_WORDS: &[&str] = &[
    "elephant", "telescope", "algorithm", "symphony", "volcano", "cathedral", "molecule",
    "glacier", "orchestra", "nebula", "quantum", "crystal", "dinosaur", "emerald",
];

const MEDIUM_WORDS: &[&str] = &[
    "world", "people", "time", "day", "year", "way", "man", "work", "life", "hand",
    "part", "place", "case", "week", "company", "system", "program", "question",
];

/// Build an index with or without ngram indexing
fn build_index(num_docs: usize, ngram_config: Option<WordNgramSet>) -> BenchIndex {
    let mut schema_builder = Schema::builder();

    let text_indexing = if let Some(ngram_set) = ngram_config {
        TextFieldIndexing::default()
            .set_index_option(IndexRecordOption::WithFreqsAndPositions)
            .set_word_ngrams(WordNgramConfig::new(ngram_set.bits()))
    } else {
        TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqsAndPositions)
    };

    let text_field = schema_builder.add_text_field("text", TEXT.clone().set_indexing_options(text_indexing));
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);

    // Populate index with realistic documents
    let mut rng = StdRng::from_seed([42u8; 32]);
    {
        let mut writer = index.writer_with_num_threads(1, 500_000_000).unwrap();
        
        for _ in 0..num_docs {
            let mut tokens = Vec::new();
            
            // Generate a document with 50-200 words
            let doc_length = rng.random_range(50..200);
            
            for _ in 0..doc_length {
                // 60% frequent, 30% medium, 10% rare
                let word_choice: f64 = rng.random();
                let word = if word_choice < 0.6 {
                    FREQUENT_WORDS[rng.random_range(0..FREQUENT_WORDS.len())]
                } else if word_choice < 0.9 {
                    MEDIUM_WORDS[rng.random_range(0..MEDIUM_WORDS.len())]
                } else {
                    RARE_WORDS[rng.random_range(0..RARE_WORDS.len())]
                };
                tokens.push(word);
            }
            
            // Inject some specific phrases we'll search for
            if rng.random_bool(0.3) {
                let pos = rng.random_range(0..tokens.len().saturating_sub(3));
                tokens[pos] = "the";
                tokens[pos + 1] = "quick";
                tokens[pos + 2] = "brown";
            }
            if rng.random_bool(0.2) {
                let pos = rng.random_range(0..tokens.len().saturating_sub(4));
                tokens[pos] = "in";
                tokens[pos + 1] = "the";
                tokens[pos + 2] = "world";
                tokens[pos + 3] = "of";
            }
            if rng.random_bool(0.15) {
                let pos = rng.random_range(0..tokens.len().saturating_sub(3));
                tokens[pos] = "telescope";
                tokens[pos + 1] = "and";
                tokens[pos + 2] = "nebula";
            }
            
            writer.add_document(doc!(text_field => tokens.join(" "))).unwrap();
        }
        writer.commit().unwrap();
    }

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();
    let searcher = reader.searcher();

    BenchIndex {
        index,
        searcher,
        field: text_field,
    }
}

fn main() {
    let mut runner = BenchRunner::new();

    // Test different corpus sizes
    let corpus_sizes = vec![
        ("Small (10K docs)", 10_000),
        ("Medium (50K docs)", 50_000),
        ("Large (100K docs)", 100_000),
    ];

    for (size_label, num_docs) in corpus_sizes {
        // Build indices with different configurations
        let no_ngrams = build_index(num_docs, None);
        let bigrams_ff = build_index(
            num_docs,
            Some(WordNgramSet::new().with_ngram_ff()),
        );
        let bigrams_all = build_index(
            num_docs,
            Some(
                WordNgramSet::new()
                    .with_ngram_ff()
                    .with_ngram_fr()
                    .with_ngram_rf(),
            ),
        );
        let with_trigrams = build_index(
            num_docs,
            Some(
                WordNgramSet::new()
                    .with_ngram_ff()
                    .with_ngram_fr()
                    .with_ngram_rf()
                    .with_ngram_fff(),
            ),
        );

        // Benchmark 2-word phrases
        {
            let mut group = runner.new_group();
            group.set_name(format!("2-word phrases — {}", size_label));
            
            let phrases_2word = vec![
                ("the quick", vec!["the", "quick"]),
                ("in the", vec!["in", "the"]),
                ("telescope and", vec!["telescope", "and"]),
                ("world of", vec!["world", "of"]),
            ];

            for (phrase_name, terms) in phrases_2word {
                add_phrase_bench(
                    &mut group,
                    &no_ngrams,
                    phrase_name,
                    &terms,
                    "no_ngrams",
                );
                add_phrase_bench(
                    &mut group,
                    &bigrams_ff,
                    phrase_name,
                    &terms,
                    "bigrams_ff",
                );
                add_phrase_bench(
                    &mut group,
                    &bigrams_all,
                    phrase_name,
                    &terms,
                    "bigrams_all",
                );
                add_phrase_bench(
                    &mut group,
                    &with_trigrams,
                    phrase_name,
                    &terms,
                    "with_trigrams",
                );
            }
            group.run();
        }

        // Benchmark 3-word phrases
        {
            let mut group = runner.new_group();
            group.set_name(format!("3-word phrases — {}", size_label));
            
            let phrases_3word = vec![
                ("the quick brown", vec!["the", "quick", "brown"]),
                ("in the world", vec!["in", "the", "world"]),
                ("telescope and nebula", vec!["telescope", "and", "nebula"]),
            ];

            for (phrase_name, terms) in phrases_3word {
                add_phrase_bench(
                    &mut group,
                    &no_ngrams,
                    phrase_name,
                    &terms,
                    "no_ngrams",
                );
                add_phrase_bench(
                    &mut group,
                    &bigrams_ff,
                    phrase_name,
                    &terms,
                    "bigrams_ff",
                );
                add_phrase_bench(
                    &mut group,
                    &bigrams_all,
                    phrase_name,
                    &terms,
                    "bigrams_all",
                );
                add_phrase_bench(
                    &mut group,
                    &with_trigrams,
                    phrase_name,
                    &terms,
                    "with_trigrams",
                );
            }
            group.run();
        }

        // Benchmark 4-word phrases
        {
            let mut group = runner.new_group();
            group.set_name(format!("4-word phrases — {}", size_label));
            
            let phrases_4word = vec![
                ("in the world of", vec!["in", "the", "world", "of"]),
            ];

            for (phrase_name, terms) in phrases_4word {
                add_phrase_bench(
                    &mut group,
                    &no_ngrams,
                    phrase_name,
                    &terms,
                    "no_ngrams",
                );
                add_phrase_bench(
                    &mut group,
                    &bigrams_ff,
                    phrase_name,
                    &terms,
                    "bigrams_ff",
                );
                add_phrase_bench(
                    &mut group,
                    &bigrams_all,
                    phrase_name,
                    &terms,
                    "bigrams_all",
                );
                add_phrase_bench(
                    &mut group,
                    &with_trigrams,
                    phrase_name,
                    &terms,
                    "with_trigrams",
                );
            }
            group.run();
        }

        // Index statistics comparison
        println!("\n{} - Index Statistics:", size_label);
        let (terms_none, docs_none) = estimate_index_size(&no_ngrams);
        let (terms_ff, docs_ff) = estimate_index_size(&bigrams_ff);
        let (terms_all, docs_all) = estimate_index_size(&bigrams_all);
        let (terms_tri, docs_tri) = estimate_index_size(&with_trigrams);
        
        println!("  No ngrams:      {} terms, {} docs", terms_none, docs_none);
        println!("  Bigrams (FF):   {} terms, {} docs (+{:.1}% terms)", 
                 terms_ff, docs_ff, 
                 ((terms_ff as f64 / terms_none as f64) - 1.0) * 100.0);
        println!("  Bigrams (All):  {} terms, {} docs (+{:.1}% terms)", 
                 terms_all, docs_all,
                 ((terms_all as f64 / terms_none as f64) - 1.0) * 100.0);
        println!("  With trigrams:  {} terms, {} docs (+{:.1}% terms)", 
                 terms_tri, docs_tri,
                 ((terms_tri as f64 / terms_none as f64) - 1.0) * 100.0);
    }
}

fn add_phrase_bench(
    bench_group: &mut BenchGroup,
    bench_index: &BenchIndex,
    phrase_name: &str,
    terms: &[&str],
    config_name: &str,
) {
    let task_name = format!("{}_{}", phrase_name.replace(" ", "_"), config_name);
    let phrase_terms: Vec<Term> = terms
        .iter()
        .map(|t| Term::from_field_text(bench_index.field, t))
        .collect();
    let query = PhraseQuery::new(phrase_terms);
    let search_task = SearchTask {
        searcher: bench_index.searcher.clone(),
        query: Box::new(query),
    };
    bench_group.register(task_name, move |_| black_box(search_task.run()));
}

struct SearchTask {
    searcher: Searcher,
    query: Box<dyn Query>,
}

impl SearchTask {
    #[inline(never)]
    pub fn run(&self) -> usize {
        let result = self
            .searcher
            .search(&self.query, &TopDocs::with_limit(10).order_by_score())
            .unwrap();
        result.len()
    }
}

fn estimate_index_size(bench_index: &BenchIndex) -> (usize, usize) {
    // Get actual term count and document count
    let mut total_terms = 0;
    let mut total_docs = 0;
    
    for segment_reader in bench_index.searcher.segment_readers() {
        total_docs += segment_reader.num_docs() as usize;
        if let Ok(inverted_index) = segment_reader.inverted_index(bench_index.field) {
            let terms = inverted_index.terms();
            total_terms += terms.num_terms() as usize;
        }
    }
    (total_terms, total_docs)
}
