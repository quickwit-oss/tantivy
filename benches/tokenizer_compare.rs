//! Compares UnicodeSegmenterTokenizer (unicode-segmentation UAX#29) vs alyze (hand-rolled UAX#29 DFA).
//!
//! Both implement UAX#29 word breaking; the difference is implementation strategy:
//! - UnicodeSegmenterTokenizer: `unicode_segmentation::unicode_word_indices()` + tantivy filter chain
//! - alyze: custom DFA with ASCII fast-path + ICU for non-ASCII + ReusableBuffer
//!
//! Corpora:
//! - Wikipedia: 64 MiB of English Wikipedia (same methodology as alyze's own benchmark)
//! - Loghub: up to 64 MiB of real-world logs (Apache, Zookeeper, Linux, Mac, SSH)
//!
//! First run downloads data and caches it under benches/.cache/.
//!
//! Run with: cargo bench --bench tokenizer_compare

use std::{
    fs::File,
    io::{BufRead as _, BufReader, Write as _},
    path::{Path, PathBuf},
};

use alyze::{
    analyze::{AnalysisOptions, Analyzer, ReusableBuffer, TokenizerOptions},
    uax29,
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::{RowAccessor, reader::RowIter},
    schema::types::Type,
};
use tantivy::tokenizer::{LowerCaser, RemoveLongFilter, TextAnalyzer, Token, TokenStream, Tokenizer};
use unicode_segmentation::UnicodeSegmentation;

const TARGET_BYTES: u64 = 64 << 20; // 64 MiB — matches alyze's benchmark
const MAX_TOKEN_LEN: usize = 255; // matches UnicodeSegmenterTokenizer's DEFAULT_REMOVE_TOKEN_LENGTH

// ── UnicodeSegmenterTokenizer ──────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct UnicodeSegmenterTokenizer;

struct UnicodeSegmenterTokenStream<'a> {
    iter: unicode_segmentation::UnicodeWordIndices<'a>,
    token: Token,
}

impl Tokenizer for UnicodeSegmenterTokenizer {
    type TokenStream<'a> = UnicodeSegmenterTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> UnicodeSegmenterTokenStream<'a> {
        UnicodeSegmenterTokenStream {
            iter: text.unicode_word_indices(),
            token: Token::default(),
        }
    }
}

impl<'a> TokenStream for UnicodeSegmenterTokenStream<'a> {
    fn advance(&mut self) -> bool {
        if let Some((offset, word)) = self.iter.next() {
            self.token.offset_from = offset;
            self.token.offset_to = offset + word.len();
            self.token.position = self.token.position.wrapping_add(1);
            self.token.text.clear();
            self.token.text.push_str(word);
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

// ── Corpus loading (mirrors alyze's wikipedia benchmark) ─────────────────────

fn cache_dir() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cache/wikipedia");
    std::fs::create_dir_all(&dir).expect("failed to create cache directory");
    dir
}

fn parquet_files_and_urls() -> Vec<(String, String)> {
    (0..41)
        .map(|i| {
            let file = format!("train-{i:05}-of-00041.parquet");
            let url = format!(
                "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/20231101.en/{file}?download=true"
            );
            (file, url)
        })
        .collect()
}

fn download_and_cache(file_name: &str, url: &str, dir: &Path) -> File {
    let path = dir.join(file_name);
    if !path.exists() {
        println!("downloading '{file_name}' from {url}");
        let resp = ureq::get(url).call().expect("HTTP request failed");
        let mut tmp = tempfile::Builder::new()
            .tempfile_in(dir)
            .expect("failed to create tempfile");
        std::io::copy(&mut resp.into_body().into_reader(), &mut tmp)
            .expect("failed to write response body");
        tmp.as_file_mut().flush().expect("flush failed");
        tmp.persist(&path).expect("rename to cache failed");
    }
    File::open(&path).expect("failed to open cached parquet file")
}

fn iter_text_rows(reader: Box<dyn FileReader>) -> impl Iterator<Item = String> {
    let fields = reader.metadata().file_metadata().schema().get_fields().to_vec();
    let text_fields: Vec<_> = fields.into_iter().filter(|f| f.name() == "text").collect();
    let proj = Type::group_type_builder("schema")
        .with_fields(text_fields)
        .build()
        .unwrap();
    RowIter::from_file_into(reader)
        .project(Some(proj))
        .unwrap()
        .map(|r| r.unwrap().get_string(0).cloned().unwrap())
}

fn load_corpus() -> Vec<String> {
    let dir = cache_dir();
    let mut texts: Vec<String> = Vec::new();
    let mut total: u64 = 0;

    'outer: for (file_name, url) in parquet_files_and_urls() {
        let file = download_and_cache(&file_name, &url, &dir);
        let reader = SerializedFileReader::new(file).expect("parquet reader failed");
        for text in iter_text_rows(Box::new(reader)) {
            total += text.len() as u64;
            texts.push(text);
            if total >= TARGET_BYTES {
                break 'outer;
            }
        }
    }

    assert!(total >= TARGET_BYTES, "not enough Wikipedia data in parquet shards");
    texts
}

// ── Loghub corpus ─────────────────────────────────────────────────────────────

const LOGHUB_DATASETS: &[(&str, &str)] = &[
    ("Apache.tar.gz",   "https://zenodo.org/records/8196385/files/Apache.tar.gz"),
    ("Zookeeper.tar.gz","https://zenodo.org/records/8196385/files/Zookeeper.tar.gz"),
    ("Linux.tar.gz",    "https://zenodo.org/records/8196385/files/Linux.tar.gz"),
    ("Mac.tar.gz",      "https://zenodo.org/records/8196385/files/Mac.tar.gz"),
    ("SSH.tar.gz",      "https://zenodo.org/records/8196385/files/SSH.tar.gz"),
];

fn loghub_cache_dir() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".cache/loghub");
    std::fs::create_dir_all(&dir).expect("failed to create loghub cache dir");
    dir
}

fn load_loghub_corpus() -> Vec<String> {
    let dir = loghub_cache_dir();
    let mut lines: Vec<String> = Vec::new();
    let mut total: u64 = 0;

    'outer: for (file_name, url) in LOGHUB_DATASETS {
        let archive = download_and_cache(file_name, url, &dir);
        let gz = flate2::read::GzDecoder::new(archive);
        let mut tar = tar::Archive::new(gz);

        for entry in tar.entries().expect("failed to read tar") {
            let mut entry = entry.expect("bad tar entry");
            let is_log = entry
                .path()
                .map(|p| p.extension().and_then(|e| e.to_str()) == Some("log"))
                .unwrap_or(false);
            if !is_log {
                continue;
            }
            let mut reader = BufReader::new(&mut entry);
            let mut buf = Vec::new();
            loop {
                buf.clear();
                let n = reader.read_until(b'\n', &mut buf).expect("read failed");
                if n == 0 {
                    break;
                }
                let line = match std::str::from_utf8(&buf) {
                    Ok(s) => s.trim_end_matches(['\n', '\r']),
                    Err(_) => continue, // skip non-UTF-8 lines
                };
                if line.is_empty() {
                    continue;
                }
                total += line.len() as u64;
                lines.push(line.to_owned());
                if total >= TARGET_BYTES {
                    break 'outer;
                }
            }
        }
    }

    eprintln!(
        "loghub corpus: {} lines, {:.1} MiB",
        lines.len(),
        total as f64 / (1u64 << 20) as f64,
    );
    lines
}

// ── Benchmarks ────────────────────────────────────────────────────────────────

fn to_ascii_corpus(texts: &[String]) -> Vec<String> {
    texts
        .iter()
        .map(|t| t.chars().filter(|c| c.is_ascii()).collect())
        .collect()
}

fn bench_unicode_seg(c: &mut Criterion, label: &str, texts: &[String]) {
    let bytes: u64 = texts.iter().map(|t| t.len() as u64).sum();
    let mut analyzer = TextAnalyzer::builder(UnicodeSegmenterTokenizer)
        .filter(LowerCaser)
        .filter(RemoveLongFilter::limit(MAX_TOKEN_LEN))
        .build();

    let mut group = c.benchmark_group(format!("unicode_seg{label}"));
    group.throughput(Throughput::Bytes(bytes));
    group.sample_size(16);

    // Raw unicode_word_indices() with no filters — measures pure tokenization cost.
    group.bench_function("tokenize_only", |b| {
        b.iter(|| {
            let mut count = 0u64;
            for text in texts {
                for _ in text.unicode_word_indices() {
                    count += 1;
                }
            }
            std::hint::black_box(count)
        })
    });

    // Full UnicodeSegmenterTokenizer pipeline: tokenize + lowercase + remove_long(255).
    group.bench_function("full_pipeline", |b| {
        b.iter(|| {
            let mut count = 0u64;
            for text in texts {
                let mut stream = analyzer.token_stream(text);
                while stream.advance() {
                    count += 1;
                }
            }
            std::hint::black_box(count)
        })
    });

    group.finish();
}

fn bench_alyze(c: &mut Criterion, label: &str, texts: &[String]) {
    let bytes: u64 = texts.iter().map(|t| t.len() as u64).sum();

    let base = AnalysisOptions {
        tokenizer: TokenizerOptions::UAX29Word(uax29::word::Options::default()),
        maximum_token_length: None,
        case_sensitive: true,
        stopword_removal: None,
        stemming: None,
        ascii_folding: false,
    };
    let full = Analyzer::new(AnalysisOptions {
        case_sensitive: false,
        maximum_token_length: Some(MAX_TOKEN_LEN),
        ..base
    });
    let mut buffer = ReusableBuffer::new();

    let mut group = c.benchmark_group(format!("alyze{label}"));
    group.throughput(Throughput::Bytes(bytes));
    group.sample_size(16);

    // Raw UAX#29 DFA with is_word_like() filter — equivalent to unicode_word_indices().
    group.bench_function("tokenize_only", |b| {
        b.iter(|| {
            let mut count = 0u64;
            for text in texts {
                uax29::word::tokenize(text, uax29::word::Options::default(), |_, props| {
                    if props.is_word_like() {
                        count += 1;
                    }
                    true
                });
            }
            std::hint::black_box(count)
        })
    });

    // alyze pipeline matching UnicodeSegmenterTokenizer: lowercase + remove_long(255).
    group.bench_function("full_pipeline", |b| {
        b.iter(|| {
            let mut count = 0u64;
            for text in texts {
                full.analyze(text, &mut buffer, |_| {
                    count += 1;
                    true
                });
            }
            std::hint::black_box(count)
        })
    });

    group.finish();
}

fn tokenizer_compare(c: &mut Criterion) {
    let texts = load_corpus();
    let bytes: u64 = texts.iter().map(|t| t.len() as u64).sum();
    let ascii_texts = to_ascii_corpus(&texts);
    let ascii_bytes: u64 = ascii_texts.iter().map(|t| t.len() as u64).sum();
    eprintln!(
        "wikipedia corpus: {} articles, {:.1} MiB ({:.1} MiB ascii-only)",
        texts.len(),
        bytes as f64 / (1u64 << 20) as f64,
        ascii_bytes as f64 / (1u64 << 20) as f64,
    );
    bench_unicode_seg(c, "", &texts);
    bench_alyze(c, "", &texts);
    bench_unicode_seg(c, "_ascii", &ascii_texts);
    bench_alyze(c, "_ascii", &ascii_texts);

    let log_texts = load_loghub_corpus();
    bench_unicode_seg(c, "_loghub", &log_texts);
    bench_alyze(c, "_loghub", &log_texts);
}

criterion_group!(benches, tokenizer_compare);
criterion_main!(benches);
