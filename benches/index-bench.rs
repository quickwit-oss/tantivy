use criterion::{criterion_group, criterion_main, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use tantivy::schema::{INDEXED, STORED, STRING, TEXT};
use tantivy::Index;

const HDFS_LOGS: &str = include_str!("hdfs.json");
const NUM_REPEATS: usize = 2;

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
    group.sample_size(20);
    group.bench_function("index-hdfs-no-commit", |b| {
        b.iter(|| {
            let index = Index::create_in_ram(schema.clone());
            let index_writer = index.writer_with_num_threads(1, 100_000_000).unwrap();
            for _ in 0..NUM_REPEATS {
                for doc_json in HDFS_LOGS.trim().split("\n") {
                    let doc = schema.parse_document(doc_json).unwrap();
                    index_writer.add_document(doc).unwrap();
                }
            }
        })
    });
    group.bench_function("index-hdfs-with-commit", |b| {
        b.iter(|| {
            let index = Index::create_in_ram(schema.clone());
            let mut index_writer = index.writer_with_num_threads(1, 100_000_000).unwrap();
            for _ in 0..NUM_REPEATS {
                for doc_json in HDFS_LOGS.trim().split("\n") {
                    let doc = schema.parse_document(doc_json).unwrap();
                    index_writer.add_document(doc).unwrap();
                }
            }
            index_writer.commit().unwrap();
        })
    });
    group.bench_function("index-hdfs-no-commit-with-docstore", |b| {
        b.iter(|| {
            let index = Index::create_in_ram(schema_with_store.clone());
            let index_writer = index.writer_with_num_threads(1, 100_000_000).unwrap();
            for _ in 0..NUM_REPEATS {
                for doc_json in HDFS_LOGS.trim().split("\n") {
                    let doc = schema.parse_document(doc_json).unwrap();
                    index_writer.add_document(doc).unwrap();
                }
            }
        })
    });
    group.bench_function("index-hdfs-with-commit-with-docstore", |b| {
        b.iter(|| {
            let index = Index::create_in_ram(schema_with_store.clone());
            let mut index_writer = index.writer_with_num_threads(1, 100_000_000).unwrap();
            for _ in 0..NUM_REPEATS {
                for doc_json in HDFS_LOGS.trim().split("\n") {
                    let doc = schema.parse_document(doc_json).unwrap();
                    index_writer.add_document(doc).unwrap();
                }
            }
            index_writer.commit().unwrap();
        })
    });
    group.bench_function("index-hdfs-no-commit-json-without-docstore", |b| {
        b.iter(|| {
            let index = Index::create_in_ram(dynamic_schema.clone());
            let json_field = dynamic_schema.get_field("json").unwrap();
            let mut index_writer = index.writer_with_num_threads(1, 100_000_000).unwrap();
            for _ in 0..NUM_REPEATS {
                for doc_json in HDFS_LOGS.trim().split("\n") {
                    let json_val: serde_json::Map<String, serde_json::Value> =
                        serde_json::from_str(doc_json).unwrap();
                    let doc = tantivy::doc!(json_field=>json_val);
                    index_writer.add_document(doc).unwrap();
                }
            }
            index_writer.commit().unwrap();
        })
    });
    group.bench_function("index-hdfs-with-commit-json-without-docstore", |b| {
        b.iter(|| {
            let index = Index::create_in_ram(dynamic_schema.clone());
            let json_field = dynamic_schema.get_field("json").unwrap();
            let mut index_writer = index.writer_with_num_threads(1, 100_000_000).unwrap();
            for _ in 0..NUM_REPEATS {
                for doc_json in HDFS_LOGS.trim().split("\n") {
                    let json_val: serde_json::Map<String, serde_json::Value> =
                        serde_json::from_str(doc_json).unwrap();
                    let doc = tantivy::doc!(json_field=>json_val);
                    index_writer.add_document(doc).unwrap();
                }
            }
            index_writer.commit().unwrap();
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = hdfs_index_benchmark
}
criterion_main!(benches);
