use std::path::PathBuf;

use crate::{Column, ColumnarReader, DynamicColumn, CURRENT_VERSION};

const NUM_DOCS: u32 = u16::MAX as u32;

fn generate_columnar(num_docs: u32) -> Vec<u8> {
    use crate::ColumnarWriter;

    let mut columnar_writer = ColumnarWriter::default();

    for i in 0..num_docs {
        if i % 100 == 0 {
            columnar_writer.record_numerical(i, "sparse", i as u64);
        }
        if i % 2 == 0 {
            columnar_writer.record_numerical(i, "dense", i as u64);
        }
        columnar_writer.record_numerical(i, "full", i as u64);
        columnar_writer.record_numerical(i, "multi", i as u64);
        columnar_writer.record_numerical(i, "multi", i as u64);
    }

    let mut wrt: Vec<u8> = Vec::new();
    columnar_writer.serialize(num_docs, None, &mut wrt).unwrap();

    wrt
}

#[test]
/// Writes a columnar for the CURRENT_VERSION to disk.
fn create_format() {
    let version = CURRENT_VERSION.to_string();
    let file_path = path_for_version(&version);
    if PathBuf::from(file_path.clone()).exists() {
        return;
    }
    let columnar = generate_columnar(NUM_DOCS);
    std::fs::write(file_path, columnar).unwrap();
}

fn path_for_version(version: &str) -> String {
    format!("./compat_tests_data/{}.columnar", version)
}

#[test]
fn test_format_v1() {
    let path = path_for_version("v1");
    test_format(&path);
}

fn test_format(path: &str) {
    let file_content = std::fs::read(path).unwrap();
    let reader = ColumnarReader::open(file_content).unwrap();

    let column = open_column(&reader, "full");
    assert_eq!(column.first(0).unwrap(), 0);
    assert_eq!(column.first(NUM_DOCS - 1).unwrap(), NUM_DOCS as u64 - 1);

    let column = open_column(&reader, "multi");
    assert_eq!(column.first(0).unwrap(), 0);
    assert_eq!(column.first(NUM_DOCS - 1).unwrap(), NUM_DOCS as u64 - 1);

    let column = open_column(&reader, "sparse");
    assert_eq!(column.first(0).unwrap(), 0);
    assert_eq!(column.first(NUM_DOCS - 1), None);
    assert_eq!(column.first(65000), Some(65000));

    let column = open_column(&reader, "dense");
    assert_eq!(column.first(0).unwrap(), 0);
    assert_eq!(column.first(NUM_DOCS - 1).unwrap(), NUM_DOCS as u64 - 1);
    assert_eq!(column.first(NUM_DOCS - 2), None);
}

fn open_column(reader: &ColumnarReader, name: &str) -> Column<u64> {
    let column = reader.read_columns(name).unwrap()[0]
        .open()
        .unwrap()
        .coerce_numerical(crate::NumericalType::U64)
        .unwrap();
    let DynamicColumn::U64(column) = column else {
        panic!();
    };
    column
}
