use super::*;
use crate::{Cardinality, ColumnarWriter, HasAssociatedColumnType, RowId};

fn make_columnar<T: Into<NumericalValue> + HasAssociatedColumnType + Copy>(
    column_name: &str,
    vals: &[T],
) -> ColumnarReader {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_column_type(column_name, T::column_type(), false);
    for (row_id, val) in vals.iter().copied().enumerate() {
        dataframe_writer.record_numerical(row_id as RowId, column_name, val.into());
    }
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer
        .serialize(vals.len() as RowId, &mut buffer)
        .unwrap();
    ColumnarReader::open(buffer).unwrap()
}

#[test]
fn test_column_coercion_to_u64() {
    // i64 type
    let columnar1 = make_columnar("numbers", &[1i64]);
    // u64 type
    let columnar2 = make_columnar("numbers", &[u64::MAX]);
    let column_map: BTreeMap<(String, ColumnType), Vec<Option<DynamicColumn>>> =
        group_columns_for_merge(&[&columnar1, &columnar2]).unwrap();
    assert_eq!(column_map.len(), 1);
    assert!(column_map.contains_key(&("numbers".to_string(), ColumnType::U64)));
}

#[test]
fn test_column_no_coercion_if_all_the_same() {
    let columnar1 = make_columnar("numbers", &[1u64]);
    let columnar2 = make_columnar("numbers", &[2u64]);
    let column_map: BTreeMap<(String, ColumnType), Vec<Option<DynamicColumn>>> =
        group_columns_for_merge(&[&columnar1, &columnar2]).unwrap();
    assert_eq!(column_map.len(), 1);
    assert!(column_map.contains_key(&("numbers".to_string(), ColumnType::U64)));
}

#[test]
fn test_column_coercion_to_i64() {
    let columnar1 = make_columnar("numbers", &[-1i64]);
    let columnar2 = make_columnar("numbers", &[2u64]);
    let column_map: BTreeMap<(String, ColumnType), Vec<Option<DynamicColumn>>> =
        group_columns_for_merge(&[&columnar1, &columnar2]).unwrap();
    assert_eq!(column_map.len(), 1);
    assert!(column_map.contains_key(&("numbers".to_string(), ColumnType::I64)));
}

#[test]
fn test_missing_column() {
    let columnar1 = make_columnar("numbers", &[-1i64]);
    let columnar2 = make_columnar("numbers2", &[2u64]);
    let column_map: BTreeMap<(String, ColumnType), Vec<Option<DynamicColumn>>> =
        group_columns_for_merge(&[&columnar1, &columnar2]).unwrap();
    assert_eq!(column_map.len(), 2);
    assert!(column_map.contains_key(&("numbers".to_string(), ColumnType::I64)));
    {
        let columns = column_map
            .get(&("numbers".to_string(), ColumnType::I64))
            .unwrap();
        assert!(columns[0].is_some());
        assert!(columns[1].is_none());
    }
    {
        let columns = column_map
            .get(&("numbers2".to_string(), ColumnType::U64))
            .unwrap();
        assert!(columns[0].is_none());
        assert!(columns[1].is_some());
    }
}

fn make_numerical_columnar_multiple_columns(
    columns: &[(&str, &[&[NumericalValue]])],
) -> ColumnarReader {
    let mut dataframe_writer = ColumnarWriter::default();
    for (column_name, column_values) in columns {
        for (row_id, vals) in column_values.iter().enumerate() {
            for val in vals.iter() {
                dataframe_writer.record_numerical(row_id as u32, column_name, *val);
            }
        }
    }
    let num_rows = columns
        .iter()
        .map(|(_, val_rows)| val_rows.len() as RowId)
        .max()
        .unwrap_or(0u32);
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(num_rows, &mut buffer).unwrap();
    ColumnarReader::open(buffer).unwrap()
}

fn make_byte_columnar_multiple_columns(columns: &[(&str, &[&[&[u8]]])]) -> ColumnarReader {
    let mut dataframe_writer = ColumnarWriter::default();
    for (column_name, column_values) in columns {
        for (row_id, vals) in column_values.iter().enumerate() {
            for val in vals.iter() {
                dataframe_writer.record_bytes(row_id as u32, column_name, *val);
            }
        }
    }
    let num_rows = columns
        .iter()
        .map(|(_, val_rows)| val_rows.len() as RowId)
        .max()
        .unwrap_or(0u32);
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(num_rows, &mut buffer).unwrap();
    ColumnarReader::open(buffer).unwrap()
}

fn make_text_columnar_multiple_columns(columns: &[(&str, &[&[&str]])]) -> ColumnarReader {
    let mut dataframe_writer = ColumnarWriter::default();
    for (column_name, column_values) in columns {
        for (row_id, vals) in column_values.iter().enumerate() {
            for val in vals.iter() {
                dataframe_writer.record_str(row_id as u32, column_name, *val);
            }
        }
    }
    let num_rows = columns
        .iter()
        .map(|(_, val_rows)| val_rows.len() as RowId)
        .max()
        .unwrap_or(0u32);
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(num_rows, &mut buffer).unwrap();
    ColumnarReader::open(buffer).unwrap()
}

#[test]
fn test_merge_columnar_numbers() {
    let columnar1 =
        make_numerical_columnar_multiple_columns(&[("numbers", &[&[NumericalValue::from(-1f64)]])]);
    let columnar2 = make_numerical_columnar_multiple_columns(&[(
        "numbers",
        &[&[], &[NumericalValue::from(-3f64)]],
    )]);
    let mut buffer = Vec::new();
    let columnars = &[&columnar1, &columnar2];
    let stack_merge_order = StackMergeOrder::from_columnars(columnars);
    crate::columnar::merge_columnar(
        columnars,
        MergeRowOrder::Stack(stack_merge_order),
        &mut buffer,
    )
    .unwrap();
    let columnar_reader = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar_reader.num_rows(), 3);
    assert_eq!(columnar_reader.num_columns(), 1);
    let cols = columnar_reader.read_columns("numbers").unwrap();
    let dynamic_column = cols[0].open().unwrap();
    let DynamicColumn::F64(vals) = dynamic_column else { panic!() };
    assert_eq!(vals.get_cardinality(), Cardinality::Optional);
    assert_eq!(vals.first(0u32), Some(-1f64));
    assert_eq!(vals.first(1u32), None);
    assert_eq!(vals.first(2u32), Some(-3f64));
}

#[test]
fn test_merge_columnar_texts() {
    let columnar1 = make_text_columnar_multiple_columns(&[("texts", &[&["a"]])]);
    let columnar2 = make_text_columnar_multiple_columns(&[("texts", &[&[], &["b"]])]);
    let mut buffer = Vec::new();
    let columnars = &[&columnar1, &columnar2];
    let stack_merge_order = StackMergeOrder::from_columnars(columnars);
    crate::columnar::merge_columnar(
        columnars,
        MergeRowOrder::Stack(stack_merge_order),
        &mut buffer,
    )
    .unwrap();
    let columnar_reader = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar_reader.num_rows(), 3);
    assert_eq!(columnar_reader.num_columns(), 1);
    let cols = columnar_reader.read_columns("texts").unwrap();
    let dynamic_column = cols[0].open().unwrap();
    let DynamicColumn::Str(vals) = dynamic_column else { panic!() };
    let get_str_for_ord = |ord| {
        let mut out = String::new();
        vals.ord_to_str(ord, &mut out).unwrap();
        out
    };

    assert_eq!(vals.dictionary.num_terms(), 2);
    assert_eq!(get_str_for_ord(0), "a");
    assert_eq!(get_str_for_ord(1), "b");

    let get_str_for_row = |row_id| {
        let term_ords: Vec<u64> = vals.term_ords(row_id).collect();
        assert!(term_ords.len() <= 1);
        let mut out = String::new();
        if term_ords.len() == 1 {
            vals.ord_to_str(term_ords[0], &mut out).unwrap();
        }
        out
    };

    assert_eq!(get_str_for_row(0), "a");
    assert_eq!(get_str_for_row(1), "");
    assert_eq!(get_str_for_row(2), "b");
}

#[test]
fn test_merge_columnar_byte() {
    let columnar1 = make_byte_columnar_multiple_columns(&[("bytes", &[&[b"bbbb"], &[b"baaa"]])]);
    let columnar2 = make_byte_columnar_multiple_columns(&[("bytes", &[&[], &[b"a"]])]);
    let mut buffer = Vec::new();
    let columnars = &[&columnar1, &columnar2];
    let stack_merge_order = StackMergeOrder::from_columnars(columnars);
    crate::columnar::merge_columnar(
        columnars,
        MergeRowOrder::Stack(stack_merge_order),
        &mut buffer,
    )
    .unwrap();
    let columnar_reader = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar_reader.num_rows(), 4);
    assert_eq!(columnar_reader.num_columns(), 1);
    let cols = columnar_reader.read_columns("bytes").unwrap();
    let dynamic_column = cols[0].open().unwrap();
    let DynamicColumn::Bytes(vals) = dynamic_column else { panic!() };
    let get_bytes_for_ord = |ord| {
        let mut out = Vec::new();
        vals.ord_to_bytes(ord, &mut out).unwrap();
        out
    };

    assert_eq!(vals.dictionary.num_terms(), 3);
    assert_eq!(get_bytes_for_ord(0), b"a");
    assert_eq!(get_bytes_for_ord(1), b"baaa");
    assert_eq!(get_bytes_for_ord(2), b"bbbb");

    let get_bytes_for_row = |row_id| {
        let term_ords: Vec<u64> = vals.term_ords(row_id).collect();
        assert!(term_ords.len() <= 1);
        let mut out = Vec::new();
        if term_ords.len() == 1 {
            vals.ord_to_bytes(term_ords[0], &mut out).unwrap();
        }
        out
    };

    assert_eq!(get_bytes_for_row(0), b"bbbb");
    assert_eq!(get_bytes_for_row(1), b"baaa");
    assert_eq!(get_bytes_for_row(2), b"");
    assert_eq!(get_bytes_for_row(3), b"a");
}
