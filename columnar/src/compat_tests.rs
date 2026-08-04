use std::path::PathBuf;

use itertools::Itertools;

use crate::{
    CURRENT_VERSION, Cardinality, Column, ColumnarReader, DictionaryEncodedBytesColumn,
    DictionaryEncodedStrColumn, DynamicColumn, PayloadEncoding, StackMergeOrder, merge_columnar,
};

const NUM_DOCS: u32 = u16::MAX as u32;
const STRING_BYTES_NUM_DOCS: u32 = 4;

fn generate_columnar(num_docs: u32, value_offset: u64) -> Vec<u8> {
    use crate::ColumnarWriter;

    let mut columnar_writer = ColumnarWriter::default();

    for i in 0..num_docs {
        if i % 100 == 0 {
            columnar_writer.record_numerical(i, "sparse", value_offset + i as u64);
        }
        if i % 5 == 0 {
            columnar_writer.record_numerical(i, "dense", value_offset + i as u64);
        }
        columnar_writer.record_numerical(i, "full", value_offset + i as u64);
        columnar_writer.record_numerical(i, "multi", value_offset + i as u64);
        columnar_writer.record_numerical(i, "multi", value_offset + i as u64);
    }

    let mut wrt: Vec<u8> = Vec::new();
    columnar_writer.serialize(num_docs, None, &mut wrt).unwrap();

    wrt
}

fn generate_string_bytes_columnar() -> Vec<u8> {
    use crate::ColumnarWriter;

    let mut columnar_writer = ColumnarWriter::default();
    for doc in 0..STRING_BYTES_NUM_DOCS {
        columnar_writer.record_str(doc, "str_full", &format!("str-full-{doc}"));
        columnar_writer.record_bytes(doc, "bytes_full", &[doc as u8, 0, 255]);

        if doc.is_multiple_of(2) {
            columnar_writer.record_str(doc, "str_optional", &format!("str-optional-{doc}"));
            columnar_writer.record_bytes(doc, "bytes_optional", &[doc as u8, 1, 254]);
        }

        columnar_writer.record_str(doc, "str_multi", &format!("str-multi-{doc}-a"));
        columnar_writer.record_str(doc, "str_multi", &format!("str-multi-{doc}-b"));
        columnar_writer.record_bytes(doc, "bytes_multi", &[doc as u8, 2, 0]);
        columnar_writer.record_bytes(doc, "bytes_multi", &[doc as u8, 2, 255]);
    }

    let mut output = Vec::new();
    columnar_writer
        .serialize(STRING_BYTES_NUM_DOCS, None, &mut output)
        .unwrap();
    output
}

#[test]
/// Writes a columnar for the CURRENT_VERSION to disk.
fn create_format() {
    let version = CURRENT_VERSION.to_string();
    let file_path = path_for_version(&version);
    if PathBuf::from(file_path.clone())
        .try_exists()
        .expect("Failed to check whether the compatibility columnar file exists")
    {
        return;
    }
    let columnar = generate_columnar(NUM_DOCS, 0);
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

#[test]
fn test_format_v2() {
    let path = path_for_version("v2");
    test_format(&path);
}

#[test]
fn test_format_v3() {
    let path = path_for_version("v3");
    test_format(&path);
}

#[test]
fn test_string_bytes_format_v1() {
    test_string_bytes_format("v1");
}

#[test]
fn test_string_bytes_format_v2() {
    test_string_bytes_format("v2");
}

fn test_string_bytes_format(version: &str) {
    let fixture_path = format!("./compat_tests_data/{version}_string_bytes.columnar");
    let fixture_reader = ColumnarReader::open(std::fs::read(fixture_path).unwrap()).unwrap();
    check_string_bytes_columns(&fixture_reader, 1);

    let current_reader = ColumnarReader::open(generate_string_bytes_columnar()).unwrap();
    check_string_bytes_columns(&current_reader, 1);

    let readers = [&fixture_reader, &current_reader];
    let merge_row_order = StackMergeOrder::stack(&readers);
    let mut output = Vec::new();
    merge_columnar(&readers, &[], merge_row_order.into(), &mut output).unwrap();
    let merged_reader = ColumnarReader::open(output).unwrap();
    check_string_bytes_columns(&merged_reader, 2);
}

fn check_string_bytes_columns(reader: &ColumnarReader, repetitions: u32) {
    let num_docs = STRING_BYTES_NUM_DOCS * repetitions;

    let str_full = open_str_column(reader, "str_full");
    assert_eq!(str_full.get_cardinality(), Cardinality::Full);
    let str_optional = open_str_column(reader, "str_optional");
    assert_eq!(str_optional.get_cardinality(), Cardinality::Optional);
    let str_multi = open_str_column(reader, "str_multi");
    assert_eq!(str_multi.get_cardinality(), Cardinality::Multivalued);

    let bytes_full = open_bytes_column(reader, "bytes_full");
    assert_eq!(bytes_full.get_cardinality(), Cardinality::Full);
    let bytes_optional = open_bytes_column(reader, "bytes_optional");
    assert_eq!(bytes_optional.get_cardinality(), Cardinality::Optional);
    let bytes_multi = open_bytes_column(reader, "bytes_multi");
    assert_eq!(bytes_multi.get_cardinality(), Cardinality::Multivalued);

    for row_id in 0..num_docs {
        let doc = row_id % STRING_BYTES_NUM_DOCS;
        assert_eq!(
            str_values(&str_full, row_id),
            vec![format!("str-full-{doc}")]
        );
        assert_eq!(
            str_values(&str_optional, row_id),
            if doc.is_multiple_of(2) {
                vec![format!("str-optional-{doc}")]
            } else {
                Vec::new()
            }
        );
        assert_eq!(
            str_values(&str_multi, row_id),
            vec![format!("str-multi-{doc}-a"), format!("str-multi-{doc}-b")]
        );

        assert_eq!(
            bytes_values(&bytes_full, row_id),
            vec![vec![doc as u8, 0, 255]]
        );
        assert_eq!(
            bytes_values(&bytes_optional, row_id),
            if doc.is_multiple_of(2) {
                vec![vec![doc as u8, 1, 254]]
            } else {
                Vec::new()
            }
        );
        assert_eq!(
            bytes_values(&bytes_multi, row_id),
            vec![vec![doc as u8, 2, 0], vec![doc as u8, 2, 255]]
        );
    }
}

fn open_str_column(reader: &ColumnarReader, name: &str) -> DictionaryEncodedStrColumn {
    let DynamicColumn::Str(column) = reader.read_columns(name).unwrap()[0].open().unwrap() else {
        panic!("expected a string column")
    };
    assert_eq!(column.payload_encoding(), PayloadEncoding::Dictionary);
    column.as_dictionary_encoded().unwrap().clone()
}

fn open_bytes_column(reader: &ColumnarReader, name: &str) -> DictionaryEncodedBytesColumn {
    let DynamicColumn::Bytes(column) = reader.read_columns(name).unwrap()[0].open().unwrap() else {
        panic!("expected a byte column")
    };
    assert_eq!(column.payload_encoding(), PayloadEncoding::Dictionary);
    column.as_dictionary_encoded().unwrap().clone()
}

fn str_values(column: &DictionaryEncodedStrColumn, row_id: u32) -> Vec<String> {
    column
        .term_ords(row_id)
        .map(|term_ord| {
            let mut output = String::new();
            assert!(column.ord_to_str(term_ord, &mut output).unwrap());
            output
        })
        .collect()
}

fn bytes_values(column: &DictionaryEncodedBytesColumn, row_id: u32) -> Vec<Vec<u8>> {
    column
        .term_ords(row_id)
        .map(|term_ord| {
            let mut output = Vec::new();
            assert!(column.ord_to_bytes(term_ord, &mut output).unwrap());
            output
        })
        .collect()
}

fn test_format(path: &str) {
    let file_content = std::fs::read(path).unwrap();
    let reader = ColumnarReader::open(file_content).unwrap();

    check_columns(&reader);

    // Test merge
    let reader2 = ColumnarReader::open(generate_columnar(NUM_DOCS, NUM_DOCS as u64)).unwrap();
    let columnar_readers = vec![&reader, &reader2];
    let merge_row_order = StackMergeOrder::stack(&columnar_readers[..]);
    let mut out = Vec::new();
    merge_columnar(&columnar_readers, &[], merge_row_order.into(), &mut out).unwrap();
    let reader = ColumnarReader::open(out).unwrap();
    check_columns(&reader);
}

fn check_columns(reader: &ColumnarReader) {
    let column = open_column(reader, "full");
    check_column(&column, |doc_id| vec![(doc_id, doc_id as u64).into()]);
    assert_eq!(column.get_cardinality(), Cardinality::Full);

    let column = open_column(reader, "multi");
    check_column(&column, |doc_id| {
        vec![
            (doc_id * 2, doc_id as u64).into(),
            (doc_id * 2 + 1, doc_id as u64).into(),
        ]
    });
    assert_eq!(column.get_cardinality(), Cardinality::Multivalued);

    let column = open_column(reader, "sparse");
    check_column(&column, |doc_id| {
        if doc_id % 100 == 0 {
            vec![(doc_id / 100, doc_id as u64).into()]
        } else {
            vec![]
        }
    });
    assert_eq!(column.get_cardinality(), Cardinality::Optional);

    let column = open_column(reader, "dense");
    check_column(&column, |doc_id| {
        if doc_id % 5 == 0 {
            vec![(doc_id / 5, doc_id as u64).into()]
        } else {
            vec![]
        }
    });
    assert_eq!(column.get_cardinality(), Cardinality::Optional);
}

struct RowIdAndValue {
    row_id: u32,
    value: u64,
}
impl From<(u32, u64)> for RowIdAndValue {
    fn from((row_id, value): (u32, u64)) -> Self {
        Self { row_id, value }
    }
}

fn check_column<F: Fn(u32) -> Vec<RowIdAndValue>>(column: &Column<u64>, expected: F) {
    let num_docs = column.num_docs();
    let test_doc = |doc: u32| {
        if expected(doc).is_empty() {
            assert_eq!(column.first(doc), None);
        } else {
            assert_eq!(column.first(doc), Some(expected(doc)[0].value));
        }
        let values = column.values_for_doc(doc).collect_vec();
        assert_eq!(values, expected(doc).iter().map(|x| x.value).collect_vec());
        let mut row_ids = Vec::new();
        column.row_ids_for_docs(&[doc], &mut vec![], &mut row_ids);
        assert_eq!(
            row_ids,
            expected(doc).iter().map(|x| x.row_id).collect_vec()
        );
        let values = column.values_for_doc(doc).collect_vec();
        assert_eq!(values, expected(doc).iter().map(|x| x.value).collect_vec());

        // Docid rowid conversion
        let mut row_ids = Vec::new();
        let safe_next_doc = |doc: u32| (doc + 1).min(num_docs - 1);
        column
            .index
            .docids_to_rowids(&[doc, safe_next_doc(doc)], &mut vec![], &mut row_ids);
        let expected_rowids = expected(doc)
            .iter()
            .map(|x| x.row_id)
            .chain(expected(safe_next_doc(doc)).iter().map(|x| x.row_id))
            .collect_vec();
        assert_eq!(row_ids, expected_rowids);
        let rowid_range = column
            .index
            .docid_range_to_rowids(doc..safe_next_doc(doc) + 1);
        if expected_rowids.is_empty() {
            assert!(rowid_range.is_empty());
        } else {
            assert_eq!(
                rowid_range,
                expected_rowids[0]..expected_rowids.last().unwrap() + 1
            );
        }
    };
    test_doc(0);
    test_doc(num_docs - 1);
    test_doc(num_docs - 2);
    test_doc(65000);
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
