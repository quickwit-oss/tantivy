//! Reports, for each tantivy fast field column, the codec chosen for its values and the split
//! between the column index (optional / multivalued) and the values.
//!
//! Usage: `cargo run --release --example tantivy_codecs -- <output.json> <index_dir>...`
//!
//! Only the first segment of each index is inspected.
//!
//! Hidden contract: this parses the columnar layout of tantivy's columnar format v2:
//! - text column: `[dictionary][ords column][dictionary len: u32]`
//! - column: `[column index][column values][column index len: u32]`
//! - column index starts with its cardinality code, column values with their codec code.

use std::collections::BTreeMap;

use serde_json::json;
use tantivy::columnar::{ColumnType, ColumnarReader};
use tantivy::index::SegmentComponent;
use tantivy::Index;

fn split_trailing_len(bytes: &[u8]) -> (&[u8], usize) {
    let (body, len_bytes) = bytes.split_at(bytes.len() - 4);
    (body, u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize)
}

fn codec_name(code: u8) -> &'static str {
    match code {
        0 => "bitpacked",
        1 => "linear",
        2 => "blockwise linear",
        _ => "unknown",
    }
}

fn cardinality_name(code: u8) -> &'static str {
    match code {
        0 => "full",
        1 => "optional",
        2 => "multivalued",
        _ => "unknown",
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut report = Vec::new();
    for index_dir in &args[2..] {
        let index = Index::open_in_dir(index_dir).unwrap();
        let searcher = index.reader().unwrap().searcher();
        let reader = searcher.segment_reader(0);
        let columnar =
            ColumnarReader::open(reader.open_read(SegmentComponent::FastFields).unwrap()).unwrap();
        let mut columns = BTreeMap::new();
        for (column_name, handle) in columnar.list_columns().unwrap() {
            if column_name.contains('\u{1}') {
                continue;
            }
            let bytes = handle.file_slice().read_bytes().unwrap();
            let mut column_bytes: &[u8] = bytes.as_slice();
            let mut dictionary_bytes = 0;
            if matches!(handle.column_type(), ColumnType::Str | ColumnType::Bytes) {
                let (body, dictionary_len) = split_trailing_len(column_bytes);
                dictionary_bytes = dictionary_len;
                column_bytes = &body[dictionary_len..];
            }
            let (body, column_index_len) = split_trailing_len(column_bytes);
            let (column_index, column_values) = body.split_at(column_index_len);
            columns.insert(
                column_name,
                json!({
                    "index": cardinality_name(column_index[0]),
                    "codec": codec_name(column_values[0]),
                    "index_bytes": column_index.len(),
                    "values_bytes": column_values.len(),
                    "dictionary_bytes": dictionary_bytes,
                }),
            );
        }
        report.push(json!({ "index": index_dir, "columns": columns }));
    }
    std::fs::write(&args[1], serde_json::to_string_pretty(&report).unwrap()).unwrap();
}
