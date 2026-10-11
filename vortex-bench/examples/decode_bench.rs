//! Measures the cost of `ValueSource::load_block` alone, for the tantivy columnar and Vortex.
//!
//! Usage: `cargo run --release --example decode_bench -- <output.json> <index_dir>...`
//!
//! Two access patterns, by blocks of 64 docs (like the aggregation collector):
//! - `all`: every doc (AllQuery).
//! - `filtered`: the docs matching `filter_field:a` (~1% of docs, in small clusters).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use serde_json::json;
use tantivy::aggregation::{ValueSource, ValueSourceProvider};
use tantivy::collector::DocSetCollector;
use tantivy::columnar::{ColumnType, DynamicColumn};
use tantivy::query::TermQuery;
use tantivy::schema::IndexRecordOption;
use tantivy::{DocId, Index, Term};
use vortex_bench::{VortexPlugin, VortexSegmentCache, VortexValueSourceProvider};

const BLOCK_LEN: usize = 64;
const NUM_REPEATS: usize = 15;

fn scan(source: &mut dyn ValueSource, docs: &[DocId]) -> (u64, u64) {
    let mut values = Vec::new();
    let mut docids = Vec::new();
    let mut row_ids = Vec::new();
    let mut checksum = 0u64;
    let mut num_values = 0u64;
    for block in docs.chunks(BLOCK_LEN) {
        source.load_block(block, &mut values, &mut docids, &mut row_ids);
        num_values += values.len() as u64;
        for &value in &values {
            checksum = checksum.wrapping_add(value);
        }
    }
    (checksum, num_values)
}

/// Returns the best time (in ns) over `NUM_REPEATS` runs, and the checksum.
fn time_scan(make_source: &dyn Fn() -> Box<dyn ValueSource>, docs: &[DocId]) -> (u64, u64, u64) {
    let mut best_ns = u64::MAX;
    let mut result = (0, 0);
    for _ in 0..NUM_REPEATS {
        // Like an aggregation, each run creates its own source (and thus starts cold).
        let mut source = make_source();
        let start = Instant::now();
        result = std::hint::black_box(scan(&mut *source, docs));
        best_ns = best_ns.min(start.elapsed().as_nanos() as u64);
    }
    (best_ns, result.0, result.1)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let output_path = &args[1];
    let mut report = Vec::new();
    for index_dir in &args[2..] {
        let mut index = Index::open_in_dir(index_dir).unwrap();
        index.register_plugin(Arc::new(VortexPlugin));
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let reader = searcher.segment_reader(0);
        let all_docs: Vec<DocId> = (0..reader.max_doc()).collect();
        let filter_field = index.schema().get_field("filter_field").unwrap();
        let filter_query = TermQuery::new(
            Term::from_field_text(filter_field, "a"),
            IndexRecordOption::Basic,
        );
        let mut filtered_docs: Vec<DocId> = searcher
            .search(&filter_query, &DocSetCollector)
            .unwrap()
            .into_iter()
            .map(|doc_address| doc_address.doc_id)
            .collect();
        filtered_docs.sort_unstable();
        let cache = Arc::new(VortexSegmentCache::default());
        let mut columns = BTreeMap::new();
        for field_name in [
            "score",
            "score_f64",
            "score_i64",
            "timestamp",
            "timestamp_26_bits",
            "text_few_terms_status",
            "text_1000_terms_zipf",
            "text_many_terms",
            "text_all_unique_terms",
        ] {
            let handle = reader
                .fast_fields()
                .dynamic_column_handles(field_name)
                .unwrap()
                .pop()
                .unwrap();
            let column_type = handle.column_type();
            let make_columnar = || -> Box<dyn ValueSource> {
                if column_type == ColumnType::Str {
                    let DynamicColumn::Str(str_column) = handle.open().unwrap() else {
                        panic!()
                    };
                    Box::new(str_column)
                } else {
                    Box::new((handle.open_u64_lenient().unwrap().unwrap(), column_type))
                }
            };
            let provider = VortexValueSourceProvider::new(field_name, cache.clone());
            let make_vortex = || -> Box<dyn ValueSource> {
                provider.for_segment(reader, None).unwrap().unwrap()
            };
            let mut column_report = serde_json::Map::new();
            for (pattern, docs) in [("all", &all_docs), ("filtered", &filtered_docs)] {
                let (columnar_ns, columnar_checksum, num_values) = time_scan(&make_columnar, docs);
                let (vortex_ns, vortex_checksum, vortex_num_values) =
                    time_scan(&make_vortex, docs);
                assert_eq!(columnar_checksum, vortex_checksum, "{field_name} {pattern}");
                assert_eq!(num_values, vortex_num_values);
                column_report.insert(
                    pattern.to_string(),
                    json!({
                        "num_docs": docs.len(),
                        "num_values": num_values,
                        "columnar_ns": columnar_ns,
                        "vortex_ns": vortex_ns,
                    }),
                );
            }
            column_report.insert("column_type".to_string(), json!(format!("{column_type:?}")));
            let vortex_segment = cache.get_or_open(reader).unwrap();
            let column_data = &vortex_segment.columns[field_name];
            column_report.insert(
                "point_accessible_chunks".to_string(),
                json!(column_data.num_point_accessible_chunks()),
            );
            column_report.insert("num_chunks".to_string(), json!(column_data.chunks.len()));
            let mut best_ns = u64::MAX;
            let mut num_point_values = 0;
            for _ in 0..NUM_REPEATS {
                let (ns, num_values) = column_data.time_point_access(100);
                best_ns = best_ns.min(ns);
                num_point_values = num_values;
            }
            column_report.insert(
                "point_access_ns_per_value".to_string(),
                json!(best_ns as f64 / num_point_values.max(1) as f64),
            );
            columns.insert(field_name.to_string(), column_report);
            eprintln!("{index_dir} {field_name} done");
        }
        report.push(json!({ "index": index_dir, "columns": columns }));
    }
    std::fs::write(output_path, serde_json::to_string_pretty(&report).unwrap()).unwrap();
}
