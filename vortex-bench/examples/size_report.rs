//! Compares the on-disk size of the tantivy columnar and of the `.vortex` component.
//!
//! Usage: `cargo run --release --example size_report -- <output.json> <index_dir>...`
//!
//! For each column, reports:
//! - `columnar_column`: tantivy column (values + optional/multivalued index).
//! - `columnar_dictionary`: tantivy sstable dictionary (text columns only).
//! - `vortex_file`: Vortex file in the `.vortex` component (text columns: term ords only).
//! - `vortex_compressed`: the same array compressed in memory by BtrBlocks, without the file
//!   format overhead (zone maps, footer...).
//! - `vortex_native_str_file` (text only): Vortex file storing the strings themselves, instead of
//!   ords + tantivy dictionary.
//! - `vortex_plain_i64_file` (dates only): Vortex file storing the timestamps as plain i64,
//!   instead of the temporal extension type.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::json;
use tantivy::columnar::{ColumnType, ColumnarReader, DynamicColumn, StrColumn};
use tantivy::index::SegmentComponent;
use tantivy::{Index, SegmentReader};
use vortex::array::arrays::{ListArray, PrimitiveArray, VarBinViewArray};
use vortex::array::validity::Validity;
use vortex::array::{ArrayRef, IntoArray};
use vortex::buffer::Buffer;
use vortex_bench::{
    compressed_nbytes, convert_doc_values, encoding_tree, open_vortex_segment, read_doc_values,
    write_vortex_file, DocValues, StoredCardinality, StoredType, VortexPlugin, VORTEX_EXTENSION,
};

fn stored_type(column_type: ColumnType) -> Option<StoredType> {
    match column_type {
        ColumnType::U64 => Some(StoredType::U64),
        ColumnType::I64 => Some(StoredType::I64),
        ColumnType::F64 => Some(StoredType::F64),
        ColumnType::DateTime => Some(StoredType::DateTime),
        ColumnType::Str => Some(StoredType::Str),
        _ => None,
    }
}

/// Builds an array holding the strings of a text column (instead of its term ords).
fn native_str_array(str_column: &StrColumn, doc_values: &DocValues) -> ArrayRef {
    let dictionary = str_column.dictionary();
    let num_terms = dictionary.num_terms();
    let all_ords: Vec<u64> = (0..num_terms as u64).collect();
    let mut terms: Vec<String> = Vec::with_capacity(num_terms);
    dictionary
        .sorted_ords_to_term_cb(&all_ords, &mut |term: &[u8]| {
            terms.push(String::from_utf8(term.to_vec()).unwrap());
        })
        .unwrap();
    let term = |ord: u64| terms[ord as usize].as_str();
    match doc_values.cardinality {
        StoredCardinality::Full => {
            VarBinViewArray::from_iter_str(doc_values.values.iter().map(|&ord| term(ord)))
                .into_array()
        }
        StoredCardinality::Optional => VarBinViewArray::from_iter_nullable_str(
            doc_values
                .values
                .iter()
                .zip(doc_values.present.iter())
                .map(|(&ord, &present)| present.then(|| term(ord))),
        )
        .into_array(),
        StoredCardinality::Multivalued => {
            let elements =
                VarBinViewArray::from_iter_str(doc_values.values.iter().map(|&ord| term(ord)))
                    .into_array();
            let offsets = PrimitiveArray::new(
                Buffer::<u32>::from(doc_values.offsets.clone()),
                Validity::NonNullable,
            )
            .into_array();
            ListArray::try_new(elements, offsets, Validity::NonNullable)
                .unwrap()
                .into_array()
        }
    }
}

/// Builds an array holding dates as plain i64 nanos (instead of the temporal extension type).
fn plain_i64_array(doc_values: &DocValues) -> ArrayRef {
    // Dates and i64 share the same monotonic mapping.
    convert_doc_values(StoredType::I64, doc_values).array
}

fn file_sizes(index_dir: &str, reader: &SegmentReader) -> BTreeMap<String, u64> {
    let segment_id = reader.segment_id().uuid_string();
    let mut sizes = BTreeMap::new();
    for dir_entry in std::fs::read_dir(index_dir).unwrap() {
        let dir_entry = dir_entry.unwrap();
        let file_name = dir_entry.file_name().to_string_lossy().to_string();
        let Some(extension) = file_name.strip_prefix(&format!("{segment_id}.")) else {
            continue;
        };
        sizes.insert(extension.to_string(), dir_entry.metadata().unwrap().len());
    }
    sizes
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let output_path = &args[1];
    let mut report = Vec::new();
    for index_dir in &args[2..] {
        let mut index = Index::open_in_dir(index_dir).unwrap();
        index.register_plugin(Arc::new(VortexPlugin));
        let searcher = index.reader().unwrap().searcher();
        let mut file_totals: BTreeMap<String, u64> = BTreeMap::new();
        let mut columns: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        let num_segments = searcher.segment_readers().len();
        // Detailed per column measurements are done on the first segments only for indexes
        // with many segments.
        let num_detailed_segments = num_segments.min(3);
        for (segment_ord, reader) in searcher.segment_readers().iter().enumerate() {
            for (extension, size) in file_sizes(index_dir, reader) {
                *file_totals.entry(extension).or_default() += size;
            }
            let vortex_file = reader
                .open_read(SegmentComponent::Custom(VORTEX_EXTENSION.to_string()))
                .unwrap()
                .read_bytes()
                .unwrap();
            let entries = vortex_bench::read_footer(vortex_file.as_slice()).unwrap();
            let vortex_segment = if segment_ord < num_detailed_segments {
                Some(open_vortex_segment(reader).unwrap())
            } else {
                None
            };
            let columnar = ColumnarReader::open(
                reader.open_read(SegmentComponent::FastFields).unwrap(),
            )
            .unwrap();
            for (column_name, handle) in columnar.list_columns().unwrap() {
                let Some(stored_type) = stored_type(handle.column_type()) else {
                    continue;
                };
                let Some(entry) = entries.iter().find(|entry| entry.name == column_name) else {
                    continue;
                };
                let space_usage = handle.column_and_dictionary_num_bytes().unwrap();
                let column_report = columns.entry(column_name.clone()).or_insert_with(|| {
                    json!({
                        "stored_type": format!("{stored_type:?}"),
                        "cardinality": format!("{:?}", entry.cardinality),
                        "columnar_column": 0u64,
                        "columnar_dictionary": 0u64,
                        "vortex_file": 0u64,
                    })
                });
                let add = |report: &mut serde_json::Value, key: &str, value: u64| {
                    let previous = report.get(key).and_then(|v| v.as_u64()).unwrap_or(0);
                    report[key] = json!(previous + value);
                };
                add(
                    column_report,
                    "columnar_column",
                    space_usage.column_num_bytes().get_bytes(),
                );
                add(
                    column_report,
                    "columnar_dictionary",
                    space_usage
                        .dictionary_num_bytes()
                        .map(|num_bytes| num_bytes.get_bytes())
                        .unwrap_or(0),
                );
                add(column_report, "vortex_file", entry.end - entry.start);
                let Some(vortex_segment) = vortex_segment.as_ref() else {
                    continue;
                };
                add(column_report, "detailed_segments", 1);
                add(
                    column_report,
                    "detailed_columnar_total",
                    space_usage.total_num_bytes().get_bytes(),
                );
                add(column_report, "detailed_vortex_file", entry.end - entry.start);
                let column_u64 = handle.open_u64_lenient().unwrap().unwrap();
                let doc_values = read_doc_values(&column_u64);
                let converted = convert_doc_values(stored_type, &doc_values);
                add(
                    column_report,
                    "detailed_vortex_compressed",
                    compressed_nbytes(&converted.array).unwrap(),
                );
                if stored_type == StoredType::Str {
                    let DynamicColumn::Str(str_column) = handle.open().unwrap() else {
                        panic!("expected a text column");
                    };
                    let native = native_str_array(&str_column, &doc_values);
                    add(
                        column_report,
                        "detailed_vortex_native_str_file",
                        write_vortex_file(native.clone()).unwrap().len() as u64,
                    );
                    add(
                        column_report,
                        "detailed_vortex_native_str_compressed",
                        compressed_nbytes(&native).unwrap(),
                    );
                }
                if stored_type == StoredType::DateTime {
                    let plain = plain_i64_array(&doc_values);
                    add(
                        column_report,
                        "detailed_vortex_plain_i64_file",
                        write_vortex_file(plain).unwrap().len() as u64,
                    );
                }
                if segment_ord == 0 {
                    let column_data = &vortex_segment.columns[&column_name];
                    let mut encodings: BTreeMap<String, usize> = BTreeMap::new();
                    for chunk in &column_data.chunks {
                        *encodings.entry(encoding_tree(chunk, 3)).or_default() += 1;
                    }
                    column_report["num_chunks"] = json!(column_data.chunks.len());
                    column_report["chunk_rows"] = json!(column_data
                        .chunk_starts
                        .windows(2)
                        .map(|window| window[1] - window[0])
                        .max()
                        .unwrap_or(0));
                    column_report["encodings"] = json!(encodings);
                    column_report["num_values"] = json!(entry.num_values);
                    column_report["num_docs"] = json!(reader.num_docs());
                }
            }
        }
        report.push(json!({
            "index": index_dir,
            "num_segments": num_segments,
            "num_docs": searcher.num_docs(),
            "files": file_totals,
            "columns": columns,
        }));
        eprintln!("done {index_dir}");
    }
    std::fs::write(output_path, serde_json::to_string_pretty(&report).unwrap()).unwrap();
}
