use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use pprof::criterion::{Output, PProfProfiler};
use tantivy::schema::{FAST, INDEXED, STORED, STRING, TEXT};
use tantivy::Index;

const HDFS_LOGS: &str = include_str!("hdfs.json");
const GH_LOGS: &str = include_str!("gh.json");
const WIKI: &str = include_str!("wiki.json");

fn get_lines(input: &str) -> Vec<&str> {
    input.trim().split('\n').collect()
}

pub fn hdfs_index_benchmark(c: &mut Criterion) {
    let schema = {
        let mut schema_builder = tantivy::schema::SchemaBuilder::new();
        schema_builder.add_u64_field("timestamp", INDEXED);
        schema_builder.add_text_field("body", TEXT);
        schema_builder.add_text_field("severity", STRING);
        schema_builder.build()
    };
    let schema_with_store = {
        let mut schema_builder = tantivy::schema::SchemaBuilder::new();
        schema_builder.add_u64_field("timestamp", INDEXED | STORED);
        schema_builder.add_text_field("body", TEXT | STORED);
        schema_builder.add_text_field("severity", STRING | STORED);
        schema_builder.build()
    };
    let dynamic_schema = {
        let mut schema_builder = tantivy::schema::SchemaBuilder::new();
        schema_builder.add_json_field("json", TEXT);
        schema_builder.build()
    };

    let mut group = c.benchmark_group("index-hdfs");
    group.throughput(Throughput::Bytes(HDFS_LOGS.len() as u64));
    group.sample_size(20);
    group.bench_function("index-hdfs-no-commit", |b| {
        let lines = get_lines(HDFS_LOGS);
        b.iter(|| {
            let index = Index::create_in_ram(schema.clone());
            let index_writer = index.writer_with_num_threads(1, 100_000_000).unwrap();
            for doc_json in &lines {
                let doc = schema.parse_document(doc_json).unwrap();
                index_writer.add_document(doc).unwrap();
            }
        })
    });
    group.bench_function("index-hdfs-with-commit", |b| {
        let lines = get_lines(HDFS_LOGS);
        b.iter(|| {
            let index = Index::create_in_ram(schema.clone());
            let mut index_writer = index.writer_with_num_threads(1, 100_000_000).unwrap();
            for doc_json in &lines {
                let doc = schema.parse_document(doc_json).unwrap();
                index_writer.add_document(doc).unwrap();
            }
            index_writer.commit().unwrap();
        })
    });
    group.bench_function("index-hdfs-no-commit-with-docstore", |b| {
        let lines = get_lines(HDFS_LOGS);
        b.iter(|| {
            let index = Index::create_in_ram(schema_with_store.clone());
            let index_writer = index.writer_with_num_threads(1, 100_000_000).unwrap();
            for doc_json in &lines {
                let doc = schema.parse_document(doc_json).unwrap();
                index_writer.add_document(doc).unwrap();
            }
        })
    });
    group.bench_function("index-hdfs-with-commit-with-docstore", |b| {
        let lines = get_lines(HDFS_LOGS);
        b.iter(|| {
            let index = Index::create_in_ram(schema_with_store.clone());
            let mut index_writer = index.writer_with_num_threads(1, 100_000_000).unwrap();
            for doc_json in &lines {
                let doc = schema.parse_document(doc_json).unwrap();
                index_writer.add_document(doc).unwrap();
            }
            index_writer.commit().unwrap();
        })
    });
    group.bench_function("index-hdfs-no-commit-json-without-docstore", |b| {
        let lines = get_lines(HDFS_LOGS);
        b.iter(|| {
            let index = Index::create_in_ram(dynamic_schema.clone());
            let json_field = dynamic_schema.get_field("json").unwrap();
            let mut index_writer = index.writer_with_num_threads(1, 100_000_000).unwrap();
            for doc_json in &lines {
                let json_val: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(doc_json).unwrap();
                let doc = tantivy::doc!(json_field=>json_val);
                index_writer.add_document(doc).unwrap();
            }
            index_writer.commit().unwrap();
        })
    });
}

pub fn gh_index_benchmark(c: &mut Criterion) {
    let dynamic_schema = {
        let mut schema_builder = tantivy::schema::SchemaBuilder::new();
        schema_builder.add_json_field("json", TEXT | FAST);
        schema_builder.build()
    };

    let mut group = c.benchmark_group("index-gh");
    group.throughput(Throughput::Bytes(GH_LOGS.len() as u64));

    group.bench_function("index-gh-no-commit", |b| {
        let lines = get_lines(GH_LOGS);
        b.iter(|| {
            let json_field = dynamic_schema.get_field("json").unwrap();
            let index = Index::create_in_ram(dynamic_schema.clone());
            let index_writer = index.writer_with_num_threads(1, 100_000_000).unwrap();
            for doc_json in &lines {
                let json_val: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(doc_json).unwrap();
                let doc = tantivy::doc!(json_field=>json_val);
                index_writer.add_document(doc).unwrap();
            }
        })
    });
    group.bench_function("index-gh-with-commit", |b| {
        let lines = get_lines(GH_LOGS);
        b.iter(|| {
            let json_field = dynamic_schema.get_field("json").unwrap();
            let index = Index::create_in_ram(dynamic_schema.clone());
            let mut index_writer = index.writer_with_num_threads(1, 100_000_000).unwrap();
            for doc_json in &lines {
                let json_val: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(doc_json).unwrap();
                let doc = tantivy::doc!(json_field=>json_val);
                index_writer.add_document(doc).unwrap();
            }
            index_writer.commit().unwrap();
        })
    });
}

pub fn wiki_index_benchmark(c: &mut Criterion) {
    let dynamic_schema = {
        let mut schema_builder = tantivy::schema::SchemaBuilder::new();
        schema_builder.add_json_field("json", TEXT | FAST);
        schema_builder.build()
    };

    let mut group = c.benchmark_group("index-wiki");
    group.throughput(Throughput::Bytes(WIKI.len() as u64));

    group.bench_function("index-wiki-no-commit", |b| {
        let lines = get_lines(WIKI);
        b.iter(|| {
            let json_field = dynamic_schema.get_field("json").unwrap();
            let index = Index::create_in_ram(dynamic_schema.clone());
            let index_writer = index.writer_with_num_threads(1, 100_000_000).unwrap();
            for doc_json in &lines {
                let json_val: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(doc_json).unwrap();
                let doc = tantivy::doc!(json_field=>json_val);
                index_writer.add_document(doc).unwrap();
            }
        })
    });
    group.bench_function("index-wiki-with-commit", |b| {
        let lines = get_lines(WIKI);
        b.iter(|| {
            let json_field = dynamic_schema.get_field("json").unwrap();
            let index = Index::create_in_ram(dynamic_schema.clone());
            let mut index_writer = index.writer_with_num_threads(1, 100_000_000).unwrap();
            for doc_json in &lines {
                let json_val: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(doc_json).unwrap();
                let doc = tantivy::doc!(json_field=>json_val);
                index_writer.add_document(doc).unwrap();
            }
            index_writer.commit().unwrap();
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = hdfs_index_benchmark
}
criterion_group! {
    name = gh_benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = gh_index_benchmark
}
criterion_group! {
    name = wiki_benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = wiki_index_benchmark
}
criterion_main!(benches, gh_benches, wiki_benches);
