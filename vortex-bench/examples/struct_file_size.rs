//! Compares one Vortex file per column (the `.vortex` layout) with a single Vortex file per
//! segment holding all the columns as a struct.
//!
//! Usage: `cargo run --release --example struct_file_size -- <output.json> <max_segments> <index_dir>...`

use std::sync::Arc;

use serde_json::json;
use tantivy::columnar::{ColumnType, ColumnarReader};
use tantivy::index::SegmentComponent;
use tantivy::Index;
use vortex::array::arrays::StructArray;
use vortex::array::{ArrayRef, IntoArray};
use vortex_bench::{convert_column, write_vortex_file, VortexPlugin};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let output_path = &args[1];
    let max_segments: usize = args[2].parse().unwrap();
    let mut report = Vec::new();
    for index_dir in &args[3..] {
        let mut index = Index::open_in_dir(index_dir).unwrap();
        index.register_plugin(Arc::new(VortexPlugin));
        let searcher = index.reader().unwrap().searcher();
        let mut per_column_bytes = 0u64;
        let mut struct_bytes = 0u64;
        let mut num_segments = 0;
        for reader in searcher.segment_readers().iter().take(max_segments) {
            let columnar =
                ColumnarReader::open(reader.open_read(SegmentComponent::FastFields).unwrap())
                    .unwrap();
            let mut fields: Vec<(String, ArrayRef)> = Vec::new();
            for (column_name, handle) in columnar.list_columns().unwrap() {
                if column_name.contains('\u{1}') || handle.column_type() == ColumnType::Bool {
                    continue;
                }
                let Some(converted) = convert_column(&handle).unwrap() else {
                    continue;
                };
                per_column_bytes += write_vortex_file(converted.array.clone()).unwrap().len() as u64;
                fields.push((column_name, converted.array));
            }
            let struct_array = StructArray::from_fields(&fields).unwrap().into_array();
            struct_bytes += write_vortex_file(struct_array).unwrap().len() as u64;
            num_segments += 1;
        }
        eprintln!(
            "{index_dir}: {num_segments} segments, per column {per_column_bytes}, struct {struct_bytes}"
        );
        report.push(json!({
            "index": index_dir,
            "num_segments_measured": num_segments,
            "per_column_files_bytes": per_column_bytes,
            "single_struct_file_bytes": struct_bytes,
        }));
    }
    std::fs::write(output_path, serde_json::to_string_pretty(&report).unwrap()).unwrap();
}
