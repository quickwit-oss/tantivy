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
        .serialize(vals.len() as RowId, None, &mut buffer)
        .unwrap();
    ColumnarReader::open(buffer).unwrap()
}

#[test]
fn test_column_coercion_to_u64() {
    // i64 type
    let columnar1 = make_columnar("numbers", &[1i64]);
    // u64 type
    let columnar2 = make_columnar("numbers", &[u64::MAX]);
    let columnars = &[&columnar1, &columnar2];
    let merge_order = StackMergeOrder::stack(columnars).into();
    let column_map: BTreeMap<(String, ColumnTypeCategory), GroupedColumnsHandle> =
        group_columns_for_merge(columnars, &[], &merge_order).unwrap();
    assert_eq!(column_map.len(), 1);
    assert!(column_map.contains_key(&("numbers".to_string(), ColumnTypeCategory::Numerical)));
}

#[test]
fn test_column_coercion_to_i64() {
    let columnar1 = make_columnar("numbers", &[-1i64]);
    let columnar2 = make_columnar("numbers", &[2u64]);
    let columnars = &[&columnar1, &columnar2];
    let merge_order = StackMergeOrder::stack(columnars).into();
    let column_map: BTreeMap<(String, ColumnTypeCategory), GroupedColumnsHandle> =
        group_columns_for_merge(columnars, &[], &merge_order).unwrap();
    assert_eq!(column_map.len(), 1);
    assert!(column_map.contains_key(&("numbers".to_string(), ColumnTypeCategory::Numerical)));
}

//#[test]
// fn test_impossible_coercion_returns_an_error() {
// let columnar1 = make_columnar("numbers", &[u64::MAX]);
// let merge_order = StackMergeOrder::stack(&[&columnar1]).into();
// let group_error = group_columns_for_merge_iter(
//&[&columnar1],
//&[("numbers".to_string(), ColumnType::I64)],
//&merge_order,
//)
//.unwrap_err();
// assert_eq!(group_error.kind(), io::ErrorKind::InvalidInput);
//}

#[test]
fn test_group_columns_with_required_column() {
    let columnar1 = make_columnar("numbers", &[1i64]);
    let columnar2 = make_columnar("numbers", &[2u64]);
    let columnars = &[&columnar1, &columnar2];
    let merge_order = StackMergeOrder::stack(columnars).into();
    let column_map: BTreeMap<(String, ColumnTypeCategory), GroupedColumnsHandle> =
        group_columns_for_merge(
            &[&columnar1, &columnar2],
            &[("numbers".to_string(), ColumnType::U64)],
            &merge_order,
        )
        .unwrap();
    assert_eq!(column_map.len(), 1);
    assert!(column_map.contains_key(&("numbers".to_string(), ColumnTypeCategory::Numerical)));
}

#[test]
fn test_group_columns_required_column_with_no_existing_columns() {
    let columnar1 = make_columnar("numbers", &[2u64]);
    let columnar2 = make_columnar("numbers", &[2u64]);
    let columnars = &[&columnar1, &columnar2];
    let merge_order = StackMergeOrder::stack(columnars).into();
    let column_map: BTreeMap<_, _> = group_columns_for_merge(
        columnars,
        &[("required_col".to_string(), ColumnType::Str)],
        &merge_order,
    )
    .unwrap();
    assert_eq!(column_map.len(), 2);
    let columns = &column_map
        .get(&("required_col".to_string(), ColumnTypeCategory::Str))
        .unwrap()
        .columns;
    assert_eq!(columns.len(), 2);
    assert!(columns[0].is_none());
    assert!(columns[1].is_none());
}

#[test]
fn test_group_columns_required_column_is_above_all_columns_have_the_same_type_rule() {
    let columnar1 = make_columnar("numbers", &[2i64]);
    let columnar2 = make_columnar("numbers", &[2i64]);
    let columnars = &[&columnar1, &columnar2];
    let merge_order = StackMergeOrder::stack(columnars).into();
    let column_map: BTreeMap<(String, ColumnTypeCategory), GroupedColumnsHandle> =
        group_columns_for_merge(
            columnars,
            &[("numbers".to_string(), ColumnType::U64)],
            &merge_order,
        )
        .unwrap();
    assert_eq!(column_map.len(), 1);
    assert!(column_map.contains_key(&("numbers".to_string(), ColumnTypeCategory::Numerical)));
}

#[test]
fn test_missing_column() {
    let columnar1 = make_columnar("numbers", &[-1i64]);
    let columnar2 = make_columnar("numbers2", &[2u64]);
    let columnars = &[&columnar1, &columnar2];
    let merge_order = StackMergeOrder::stack(columnars).into();
    let column_map: BTreeMap<(String, ColumnTypeCategory), GroupedColumnsHandle> =
        group_columns_for_merge(columnars, &[], &merge_order).unwrap();
    assert_eq!(column_map.len(), 2);
    assert!(column_map.contains_key(&("numbers".to_string(), ColumnTypeCategory::Numerical)));
    {
        let columns = &column_map
            .get(&("numbers".to_string(), ColumnTypeCategory::Numerical))
            .unwrap()
            .columns;
        assert!(columns[0].is_some());
        assert!(columns[1].is_none());
    }
    {
        let columns = &column_map
            .get(&("numbers2".to_string(), ColumnTypeCategory::Numerical))
            .unwrap()
            .columns;
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
    dataframe_writer
        .serialize(num_rows, None, &mut buffer)
        .unwrap();
    ColumnarReader::open(buffer).unwrap()
}

#[track_caller]
fn make_byte_columnar_multiple_columns(
    columns: &[(&str, &[&[&[u8]]])],
    num_rows: u32,
) -> ColumnarReader {
    let mut dataframe_writer = ColumnarWriter::default();
    for (column_name, column_values) in columns {
        assert_eq!(
            column_values.len(),
            num_rows as usize,
            "All columns must have `{num_rows}` rows"
        );
        for (row_id, vals) in column_values.iter().enumerate() {
            for val in vals.iter() {
                dataframe_writer.record_bytes(row_id as u32, column_name, val);
            }
        }
    }
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer
        .serialize(num_rows, None, &mut buffer)
        .unwrap();
    ColumnarReader::open(buffer).unwrap()
}

fn make_text_columnar_multiple_columns(columns: &[(&str, &[&[&str]])]) -> ColumnarReader {
    let mut dataframe_writer = ColumnarWriter::default();
    for (column_name, column_values) in columns {
        for (row_id, vals) in column_values.iter().enumerate() {
            for val in vals.iter() {
                dataframe_writer.record_str(row_id as u32, column_name, val);
            }
        }
    }
    let num_rows = columns
        .iter()
        .map(|(_, val_rows)| val_rows.len() as RowId)
        .max()
        .unwrap_or(0u32);
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer
        .serialize(num_rows, None, &mut buffer)
        .unwrap();
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
    let stack_merge_order = StackMergeOrder::stack(columnars);
    crate::columnar::merge_columnar(
        columnars,
        &[],
        MergeRowOrder::Stack(stack_merge_order),
        &mut buffer,
    )
    .unwrap();
    let columnar_reader = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar_reader.num_rows(), 3);
    assert_eq!(columnar_reader.num_columns(), 1);
    let cols = columnar_reader.read_columns("numbers").unwrap();
    let dynamic_column = cols[0].open().unwrap();
    let DynamicColumn::F64(vals) = dynamic_column else {
        panic!()
    };
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
    let stack_merge_order = StackMergeOrder::stack(columnars);
    crate::columnar::merge_columnar(
        columnars,
        &[],
        MergeRowOrder::Stack(stack_merge_order),
        &mut buffer,
    )
    .unwrap();
    let columnar_reader = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar_reader.num_rows(), 3);
    assert_eq!(columnar_reader.num_columns(), 1);
    let cols = columnar_reader.read_columns("texts").unwrap();
    let dynamic_column = cols[0].open().unwrap();
    let DynamicColumn::Str(vals) = dynamic_column else {
        panic!()
    };
    assert_eq!(vals.ords().get_cardinality(), Cardinality::Optional);

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
    let columnar1 = make_byte_columnar_multiple_columns(&[("bytes", &[&[b"bbbb"], &[b"baaa"]])], 2);
    let columnar2 = make_byte_columnar_multiple_columns(&[("bytes", &[&[], &[b"a"]])], 2);
    let mut buffer = Vec::new();
    let columnars = &[&columnar1, &columnar2];
    let stack_merge_order = StackMergeOrder::stack(columnars);
    crate::columnar::merge_columnar(
        columnars,
        &[],
        MergeRowOrder::Stack(stack_merge_order),
        &mut buffer,
    )
    .unwrap();
    let columnar_reader = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar_reader.num_rows(), 4);
    assert_eq!(columnar_reader.num_columns(), 1);
    let cols = columnar_reader.read_columns("bytes").unwrap();
    let dynamic_column = cols[0].open().unwrap();
    let DynamicColumn::Bytes(vals) = dynamic_column else {
        panic!()
    };
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

#[test]
fn test_merge_columnar_byte_with_missing() {
    let columnar1 = make_byte_columnar_multiple_columns(&[], 3);
    let columnar2 = make_byte_columnar_multiple_columns(&[("col", &[&[b"b"], &[]])], 2);
    let columnar3 = make_byte_columnar_multiple_columns(
        &[
            ("col", &[&[], &[b"b"], &[b"a", b"b"]]),
            ("col2", &[&[b"hello"], &[], &[b"a", b"b"]]),
        ],
        3,
    );
    let mut buffer = Vec::new();
    let columnars = &[&columnar1, &columnar2, &columnar3];
    let stack_merge_order = StackMergeOrder::stack(columnars);
    crate::columnar::merge_columnar(
        columnars,
        &[],
        MergeRowOrder::Stack(stack_merge_order),
        &mut buffer,
    )
    .unwrap();
    let columnar_reader = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar_reader.num_rows(), 3 + 2 + 3);
    assert_eq!(columnar_reader.num_columns(), 2);
    let cols = columnar_reader.read_columns("col").unwrap();
    let dynamic_column = cols[0].open().unwrap();
    let DynamicColumn::Bytes(vals) = dynamic_column else {
        panic!()
    };
    let get_bytes_for_ord = |ord| {
        let mut out = Vec::new();
        vals.ord_to_bytes(ord, &mut out).unwrap();
        out
    };
    assert_eq!(vals.dictionary.num_terms(), 2);
    assert_eq!(get_bytes_for_ord(0), b"a");
    assert_eq!(get_bytes_for_ord(1), b"b");
    let get_bytes_for_row = |row_id| {
        let terms: Vec<Vec<u8>> = vals
            .term_ords(row_id)
            .map(|term_ord| {
                let mut out = Vec::new();
                vals.ord_to_bytes(term_ord, &mut out).unwrap();
                out
            })
            .collect();
        terms
    };
    assert!(get_bytes_for_row(0).is_empty());
    assert!(get_bytes_for_row(1).is_empty());
    assert!(get_bytes_for_row(2).is_empty());
    assert_eq!(get_bytes_for_row(3), vec![b"b".to_vec()]);
    assert!(get_bytes_for_row(4).is_empty());
    assert!(get_bytes_for_row(5).is_empty());
    assert_eq!(get_bytes_for_row(6), vec![b"b".to_vec()]);
    assert_eq!(get_bytes_for_row(7), vec![b"a".to_vec(), b"b".to_vec()]);
}

#[test]
fn test_merge_columnar_different_types() {
    let columnar1 = make_text_columnar_multiple_columns(&[("mixed", &[&["a"]])]);
    let columnar2 = make_text_columnar_multiple_columns(&[("mixed", &[&[], &["b"]])]);
    let columnar3 = make_columnar("mixed", &[1i64]);
    let mut buffer = Vec::new();
    let columnars = &[&columnar1, &columnar2, &columnar3];
    let stack_merge_order = StackMergeOrder::stack(columnars);
    crate::columnar::merge_columnar(
        columnars,
        &[],
        MergeRowOrder::Stack(stack_merge_order),
        &mut buffer,
    )
    .unwrap();
    let columnar_reader = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar_reader.num_rows(), 4);
    assert_eq!(columnar_reader.num_columns(), 2);
    let cols = columnar_reader.read_columns("mixed").unwrap();

    // numeric column
    let dynamic_column = cols[0].open().unwrap();
    let DynamicColumn::I64(vals) = dynamic_column else {
        panic!()
    };
    assert_eq!(vals.get_cardinality(), Cardinality::Optional);
    assert_eq!(vals.values_for_doc(0).collect_vec(), vec![]);
    assert_eq!(vals.values_for_doc(1).collect_vec(), vec![]);
    assert_eq!(vals.values_for_doc(2).collect_vec(), vec![]);
    assert_eq!(vals.values_for_doc(3).collect_vec(), vec![1]);
    assert_eq!(vals.values_for_doc(4).collect_vec(), vec![]);

    // text column
    let dynamic_column = cols[1].open().unwrap();
    let DynamicColumn::Str(vals) = dynamic_column else {
        panic!()
    };
    assert_eq!(vals.ords().get_cardinality(), Cardinality::Optional);
    let get_str_for_ord = |ord| {
        let mut out = String::new();
        vals.ord_to_str(ord, &mut out).unwrap();
        out
    };

    assert_eq!(vals.dictionary.num_terms(), 2);
    assert_eq!(get_str_for_ord(0), "a");
    assert_eq!(get_str_for_ord(1), "b");

    let get_str_for_row = |row_id| {
        let term_ords: Vec<String> = vals
            .term_ords(row_id)
            .map(|el| {
                let mut out = String::new();
                vals.ord_to_str(el, &mut out).unwrap();
                out
            })
            .collect();
        term_ords
    };

    assert_eq!(get_str_for_row(0), vec!["a".to_string()]);
    assert_eq!(get_str_for_row(1), Vec::<String>::new());
    assert_eq!(get_str_for_row(2), vec!["b".to_string()]);
    assert_eq!(get_str_for_row(3), Vec::<String>::new());
}

#[test]
fn test_merge_columnar_different_empty_cardinality() {
    let columnar1 = make_text_columnar_multiple_columns(&[("mixed", &[&["a"]])]);
    let columnar2 = make_columnar("mixed", &[1i64]);
    let mut buffer = Vec::new();
    let columnars = &[&columnar1, &columnar2];
    let stack_merge_order = StackMergeOrder::stack(columnars);
    crate::columnar::merge_columnar(
        columnars,
        &[],
        MergeRowOrder::Stack(stack_merge_order),
        &mut buffer,
    )
    .unwrap();
    let columnar_reader = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar_reader.num_rows(), 2);
    assert_eq!(columnar_reader.num_columns(), 2);
    let cols = columnar_reader.read_columns("mixed").unwrap();

    // numeric column
    let dynamic_column = cols[0].open().unwrap();
    assert_eq!(dynamic_column.get_cardinality(), Cardinality::Optional);

    // text column
    let dynamic_column = cols[1].open().unwrap();
    assert_eq!(dynamic_column.get_cardinality(), Cardinality::Optional);
}
