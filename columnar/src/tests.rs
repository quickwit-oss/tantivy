use std::net::Ipv6Addr;

use crate::column_values::MonotonicallyMappableToU128;
use crate::columnar::ColumnType;
use crate::dynamic_column::{DynamicColumn, DynamicColumnHandle};
use crate::value::NumericalValue;
use crate::{Cardinality, ColumnarReader, ColumnarWriter};

#[test]
fn test_dataframe_writer_str() {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_str(1u32, "my_string", "hello");
    dataframe_writer.record_str(3u32, "my_string", "helloeee");
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(5, None, &mut buffer).unwrap();
    let columnar = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar.num_columns(), 1);
    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("my_string").unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].num_bytes(), 89);
}

#[test]
fn test_dataframe_writer_bytes() {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_bytes(1u32, "my_string", b"hello");
    dataframe_writer.record_bytes(3u32, "my_string", b"helloeee");
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(5, None, &mut buffer).unwrap();
    let columnar = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar.num_columns(), 1);
    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("my_string").unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].num_bytes(), 89);
}

#[test]
fn test_dataframe_writer_bool() {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_bool(1u32, "bool.value", false);
    dataframe_writer.record_bool(3u32, "bool.value", true);
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(5, None, &mut buffer).unwrap();
    let columnar = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar.num_columns(), 1);
    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("bool.value").unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].num_bytes(), 22);
    assert_eq!(cols[0].column_type(), ColumnType::Bool);
    let dyn_bool_col = cols[0].open().unwrap();
    let DynamicColumn::Bool(bool_col) = dyn_bool_col else { panic!(); };
    let vals: Vec<Option<bool>> = (0..5).map(|row_id| bool_col.first(row_id)).collect();
    assert_eq!(&vals, &[None, Some(false), None, Some(true), None,]);
}

#[test]
fn test_dataframe_writer_u64_multivalued() {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_numerical(2u32, "divisor", 2u64);
    dataframe_writer.record_numerical(3u32, "divisor", 3u64);
    dataframe_writer.record_numerical(4u32, "divisor", 2u64);
    dataframe_writer.record_numerical(5u32, "divisor", 5u64);
    dataframe_writer.record_numerical(6u32, "divisor", 2u64);
    dataframe_writer.record_numerical(6u32, "divisor", 3u64);
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(7, None, &mut buffer).unwrap();
    let columnar = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar.num_columns(), 1);
    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("divisor").unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].num_bytes(), 29);
    let dyn_i64_col = cols[0].open().unwrap();
    let DynamicColumn::I64(divisor_col) = dyn_i64_col else { panic!(); };
    assert_eq!(
        divisor_col.get_cardinality(),
        crate::Cardinality::Multivalued
    );
    assert_eq!(divisor_col.num_docs(), 7);
}

#[test]
fn test_dataframe_writer_ip_addr() {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_ip_addr(1, "ip_addr", Ipv6Addr::from_u128(1001));
    dataframe_writer.record_ip_addr(3, "ip_addr", Ipv6Addr::from_u128(1050));
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(5, None, &mut buffer).unwrap();
    let columnar = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar.num_columns(), 1);
    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("ip_addr").unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].num_bytes(), 42);
    assert_eq!(cols[0].column_type(), ColumnType::IpAddr);
    let dyn_bool_col = cols[0].open().unwrap();
    let DynamicColumn::IpAddr(ip_col) = dyn_bool_col else { panic!(); };
    let vals: Vec<Option<Ipv6Addr>> = (0..5).map(|row_id| ip_col.first(row_id)).collect();
    assert_eq!(
        &vals,
        &[
            None,
            Some(Ipv6Addr::from_u128(1001)),
            None,
            Some(Ipv6Addr::from_u128(1050)),
            None,
        ]
    );
}

#[test]
fn test_dataframe_writer_numerical() {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_numerical(1u32, "srical.value", NumericalValue::U64(12u64));
    dataframe_writer.record_numerical(2u32, "srical.value", NumericalValue::U64(13u64));
    dataframe_writer.record_numerical(4u32, "srical.value", NumericalValue::U64(15u64));
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(6, None, &mut buffer).unwrap();
    let columnar = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar.num_columns(), 1);
    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("srical.value").unwrap();
    assert_eq!(cols.len(), 1);
    // Right now this 31 bytes are spent as follows
    //
    // - header 14 bytes
    // - vals  8 //< due to padding? could have been 1byte?.
    // - null footer 6 bytes
    assert_eq!(cols[0].num_bytes(), 33);
    let column = cols[0].open().unwrap();
    let DynamicColumn::I64(column_i64) = column else { panic!(); };
    assert_eq!(column_i64.idx.get_cardinality(), Cardinality::Optional);
    assert_eq!(column_i64.first(0), None);
    assert_eq!(column_i64.first(1), Some(12i64));
    assert_eq!(column_i64.first(2), Some(13i64));
    assert_eq!(column_i64.first(3), None);
    assert_eq!(column_i64.first(4), Some(15i64));
    assert_eq!(column_i64.first(5), None);
    assert_eq!(column_i64.first(6), None); //< we can change the spec for that one.
}

#[test]
fn test_dictionary_encoded_str() {
    let mut buffer = Vec::new();
    let mut columnar_writer = ColumnarWriter::default();
    columnar_writer.record_str(1, "my.column", "a");
    columnar_writer.record_str(3, "my.column", "c");
    columnar_writer.record_str(3, "my.column2", "different_column!");
    columnar_writer.record_str(4, "my.column", "b");
    columnar_writer.serialize(5, None, &mut buffer).unwrap();
    let columnar_reader = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar_reader.num_columns(), 2);
    let col_handles = columnar_reader.read_columns("my.column").unwrap();
    assert_eq!(col_handles.len(), 1);
    let DynamicColumn::Str(str_col) = col_handles[0].open().unwrap() else  { panic!(); };
    let index: Vec<Option<u64>> = (0..5).map(|row_id| str_col.ords().first(row_id)).collect();
    assert_eq!(index, &[None, Some(0), None, Some(2), Some(1)]);
    assert_eq!(str_col.num_rows(), 5);
    let mut term_buffer = String::new();
    let term_ords = str_col.ords();
    assert_eq!(term_ords.first(0), None);
    assert_eq!(term_ords.first(1), Some(0));
    str_col.ord_to_str(0u64, &mut term_buffer).unwrap();
    assert_eq!(term_buffer, "a");
    assert_eq!(term_ords.first(2), None);
    assert_eq!(term_ords.first(3), Some(2));
    str_col.ord_to_str(2u64, &mut term_buffer).unwrap();
    assert_eq!(term_buffer, "c");
    assert_eq!(term_ords.first(4), Some(1));
    str_col.ord_to_str(1u64, &mut term_buffer).unwrap();
    assert_eq!(term_buffer, "b");
}

#[test]
fn test_dictionary_encoded_bytes() {
    let mut buffer = Vec::new();
    let mut columnar_writer = ColumnarWriter::default();
    columnar_writer.record_bytes(1, "my.column", b"a");
    columnar_writer.record_bytes(3, "my.column", b"c");
    columnar_writer.record_bytes(3, "my.column2", b"different_column!");
    columnar_writer.record_bytes(4, "my.column", b"b");
    columnar_writer.serialize(5, None, &mut buffer).unwrap();
    let columnar_reader = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar_reader.num_columns(), 2);
    let col_handles = columnar_reader.read_columns("my.column").unwrap();
    assert_eq!(col_handles.len(), 1);
    let DynamicColumn::Bytes(bytes_col) = col_handles[0].open().unwrap() else  { panic!(); };
    let index: Vec<Option<u64>> = (0..5)
        .map(|row_id| bytes_col.ords().first(row_id))
        .collect();
    assert_eq!(index, &[None, Some(0), None, Some(2), Some(1)]);
    assert_eq!(bytes_col.num_rows(), 5);
    let mut term_buffer = Vec::new();
    let term_ords = bytes_col.ords();
    assert_eq!(term_ords.first(0), None);
    assert_eq!(term_ords.first(1), Some(0));
    bytes_col
        .dictionary
        .ord_to_term(0u64, &mut term_buffer)
        .unwrap();
    assert_eq!(term_buffer, b"a");
    assert_eq!(term_ords.first(2), None);
    assert_eq!(term_ords.first(3), Some(2));
    bytes_col
        .dictionary
        .ord_to_term(2u64, &mut term_buffer)
        .unwrap();
    assert_eq!(term_buffer, b"c");
    assert_eq!(term_ords.first(4), Some(1));
    bytes_col
        .dictionary
        .ord_to_term(1u64, &mut term_buffer)
        .unwrap();
    assert_eq!(term_buffer, b"b");
}
