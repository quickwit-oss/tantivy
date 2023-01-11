use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use pprof::criterion::{Output, PProfProfiler};
use serde_json::{self, Value as JsonValue};
use tantivy::directory::RamDirectory;
use tantivy::schema::{
    FieldValue, TextFieldIndexing, TextOptions, Value, INDEXED, STORED, STRING, TEXT,
};
use tantivy::{Document, Index, IndexBuilder};

const HDFS_LOGS: &str = include_str!("hdfs.json");
const NUM_REPEATS: usize = 20;

pub fn hdfs_index_benchmark(c: &mut Criterion) {
    let mut schema_builder = tantivy::schema::SchemaBuilder::new();
    let text_indexing_options = TextFieldIndexing::default()
        .set_tokenizer("default")
        .set_fieldnorms(false)
        .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions);
    let mut text_options = TextOptions::default().set_indexing_options(text_indexing_options);
    let text_field = schema_builder.add_text_field("body", text_options);
    let schema = schema_builder.build();

    // prepare doc
    let mut documents_no_array = Vec::new();
    let mut documents_with_array = Vec::new();
    for doc_json in HDFS_LOGS.trim().split("\n") {
        let json_obj: serde_json::Map<String, JsonValue> = serde_json::from_str(doc_json).unwrap();
        let text = json_obj.get("body").unwrap().as_str().unwrap();
        let mut doc_no_array = Document::new();
        doc_no_array.add_text(text_field, text);
        documents_no_array.push(doc_no_array);
        let mut doc_with_array = Document::new();
        doc_with_array.add_borrowed_values(text.to_owned(), |text| {
            text.split(' ')
                .map(|text| FieldValue::new(text_field, text.into()))
                .collect()
        });
        documents_with_array.push(doc_with_array);
    }

    let mut group = c.benchmark_group("index-hdfs");
    group.sample_size(20);
    group.bench_function("index-hdfs-no-commit", |b| {
        b.iter(|| {
            let ram_directory = RamDirectory::create();
            let mut index_writer = IndexBuilder::new()
                .schema(schema.clone())
                .single_segment_index_writer(ram_directory, 100_000_000)
                .unwrap();
            for _ in 0..NUM_REPEATS {
                let documents_cloned = documents_no_array.clone();
                for doc in documents_cloned {
                    index_writer.add_document(doc).unwrap();
                }
            }
        })
    });
    group.bench_function("index-hdfs-with-array-no-commit", |b| {
        b.iter(|| {
            let ram_directory = RamDirectory::create();
            let mut index_writer = IndexBuilder::new()
                .schema(schema.clone())
                .single_segment_index_writer(ram_directory, 100_000_000)
                .unwrap();
            for _ in 0..NUM_REPEATS {
                let documents_with_array_cloned = documents_with_array.clone();
                for doc in documents_with_array_cloned {
                    index_writer.add_document(doc).unwrap();
                }
            }
        })
    });
    // group.bench_function("index-hdfs-with-commit", |b| {
    //     b.iter(|| {
    //         let ram_directory = RamDirectory::create();
    //         let mut index_writer = IndexBuilder::new()
    //             .schema(schema.clone())
    //             .single_segment_index_writer(ram_directory, 100_000_000)
    //             .unwrap();
    //         for _ in 0..NUM_REPEATS {
    //             for doc_json in HDFS_LOGS.trim().split("\n") {
    //                 let doc = schema.parse_document(doc_json).unwrap();
    //                 index_writer.add_document(doc).unwrap();
    //             }
    //         }
    //         index_writer.commit().unwrap();
    //     })
    // });
    // group.bench_function("index-hdfs-no-commit-with-docstore", |b| {
    //     b.iter(|| {
    //         let ram_directory = RamDirectory::create();
    //         let mut index_writer = IndexBuilder::new()
    //             .schema(schema.clone())
    //             .single_segment_index_writer(ram_directory, 100_000_000)
    //             .unwrap();
    //         for _ in 0..NUM_REPEATS {
    //             for doc_json in HDFS_LOGS.trim().split("\n") {
    //                 let doc = schema.parse_document(doc_json).unwrap();
    //                 index_writer.add_document(doc).unwrap();
    //             }
    //         }
    //     })
    // });
    // group.bench_function("index-hdfs-with-commit-with-docstore", |b| {
    //     b.iter(|| {
    //         let ram_directory = RamDirectory::create();
    //         let mut index_writer = IndexBuilder::new()
    //             .schema(schema.clone())
    //             .single_segment_index_writer(ram_directory, 100_000_000)
    //             .unwrap();
    //         for _ in 0..NUM_REPEATS {
    //             for doc_json in HDFS_LOGS.trim().split("\n") {
    //                 let doc = schema.parse_document(doc_json).unwrap();
    //                 index_writer.add_document(doc).unwrap();
    //             }
    //         }
    //         index_writer.commit().unwrap();
    //     })
    // });
    // group.bench_function("index-hdfs-no-commit-json-without-docstore", |b| {
    //     b.iter(|| {
    //         let ram_directory = RamDirectory::create();
    //         let mut index_writer = IndexBuilder::new()
    //             .schema(schema.clone())
    //             .single_segment_index_writer(ram_directory, 100_000_000)
    //             .unwrap();
    //         for _ in 0..NUM_REPEATS {
    //             for doc_json in HDFS_LOGS.trim().split("\n") {
    //                 let json_val: serde_json::Map<String, serde_json::Value> =
    //                     serde_json::from_str(doc_json).unwrap();
    //                 let doc = tantivy::doc!(json_field=>json_val);
    //                 index_writer.add_document(doc).unwrap();
    //             }
    //         }
    //         index_writer.commit().unwrap();
    //     })
    // });
    // group.bench_function("index-hdfs-with-commit-json-without-docstore", |b| {
    //     b.iter(|| {
    //         let ram_directory = RamDirectory::create();
    //         let mut index_writer = IndexBuilder::new()
    //             .schema(schema.clone())
    //             .single_segment_index_writer(ram_directory, 100_000_000)
    //             .unwrap();
    //         for _ in 0..NUM_REPEATS {
    //             for doc_json in HDFS_LOGS.trim().split("\n") {
    //                 let json_val: serde_json::Map<String, serde_json::Value> =
    //                     serde_json::from_str(doc_json).unwrap();
    //                 let doc = tantivy::doc!(json_field=>json_val);
    //                 index_writer.add_document(doc).unwrap();
    //             }
    //         }
    //         index_writer.commit().unwrap();
    //     })
    //});
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = hdfs_index_benchmark
}
criterion_main!(benches);
