use std::collections::HashMap;
use std::io;

use super::writer::ColumnarSerializer;
use crate::columnar::ColumnarReader;
use crate::dynamic_column::DynamicColumn;
use crate::{Cardinality, ColumnType};

pub enum MergeDocOrder {
    /// Columnar tables are simply stacked one above the other.
    /// If the i-th columnar_readers has n_rows_i rows, then
    /// in the resulting columnar,
    /// rows [r0..n_row_0) contains the row of columnar_readers[0], in ordder
    /// rows [n_row_0..n_row_0 + n_row_1 contains the row of columnar_readers[1], in order.
    /// ..
    Stack,
    /// Some more complex mapping, that can interleaves rows from the different readers and
    /// possibly drop rows.
    Complex(()),
}

pub fn merge_columnar(
    columnar_readers: &[ColumnarReader],
    mapping: MergeDocOrder,
    output: &mut impl io::Write,
) -> io::Result<()> {
    let mut serializer = ColumnarSerializer::new(output);

    // TODO handle dictionary merge for Str/Bytes column
    let field_name_to_group = group_columns_for_merge(columnar_readers)?;
    for (column_name, category_to_columns) in field_name_to_group {
        for (_category, columns_to_merge) in category_to_columns {
            let column_type = columns_to_merge[0].column_type();
            let mut column_serialzier =
                serializer.serialize_column(column_name.as_bytes(), column_type);
            merge_columns(
                column_type,
                &columns_to_merge,
                &mapping,
                &mut column_serialzier,
            )?;
        }
    }
    serializer.finalize()?;

    Ok(())
}

/// Column types are grouped into different categories.
/// After merge, all columns belonging to the same category are coerced to
/// the same column type.
///
/// In practise, today, only Numerical colummns are coerced into one type today.
///
/// See also [README.md].
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
#[repr(u8)]
pub enum ColumnTypeCategory {
    Bool,
    Str,
    Numerical,
    DateTime,
    Bytes,
    IpAddr,
}

impl From<ColumnType> for ColumnTypeCategory {
    fn from(column_type: ColumnType) -> Self {
        match column_type {
            ColumnType::I64 => ColumnTypeCategory::Numerical,
            ColumnType::U64 => ColumnTypeCategory::Numerical,
            ColumnType::F64 => ColumnTypeCategory::Numerical,
            ColumnType::Bytes => ColumnTypeCategory::Bytes,
            ColumnType::Str => ColumnTypeCategory::Str,
            ColumnType::Bool => ColumnTypeCategory::Bool,
            ColumnType::IpAddr => ColumnTypeCategory::IpAddr,
            ColumnType::DateTime => ColumnTypeCategory::DateTime,
        }
    }
}

pub fn detect_cardinality(columns: &[DynamicColumn]) -> Cardinality {
    if columns
        .iter()
        .any(|column| column.get_cardinality().is_multivalue())
    {
        return Cardinality::Multivalued;
    }
    if columns
        .iter()
        .any(|column| column.get_cardinality().is_optional())
    {
        return Cardinality::Optional;
    }
    Cardinality::Full
}

pub fn compute_num_docs(columns: &[DynamicColumn], mapping: &MergeDocOrder) -> usize {
    // TODO handle deletes

    0
}

pub fn merge_columns(
    column_type: ColumnType,
    columns: &[DynamicColumn],
    mapping: &MergeDocOrder,
    column_serializer: &mut impl io::Write,
) -> io::Result<()> {
    let cardinality = detect_cardinality(columns);

    Ok(())
}

pub fn group_columns_for_merge(
    columnar_readers: &[ColumnarReader],
) -> io::Result<HashMap<String, HashMap<ColumnTypeCategory, Vec<DynamicColumn>>>> {
    // Each column name may have multiple types of column associated.
    // For merging we are interested in the same column type category since they can be merged.
    let mut field_name_to_group: HashMap<String, HashMap<ColumnTypeCategory, Vec<DynamicColumn>>> =
        HashMap::new();

    for columnar_reader in columnar_readers {
        let column_name_and_handle = columnar_reader.list_columns()?;
        for (column_name, handle) in column_name_and_handle {
            let column_type_to_handles = field_name_to_group
                .entry(column_name.to_string())
                .or_default();

            let columns = column_type_to_handles
                .entry(handle.column_type().into())
                .or_default();
            columns.push(handle.open()?);
        }
    }

    normalize_columns(&mut field_name_to_group);

    Ok(field_name_to_group)
}

/// Coerce numerical type columns to the same type
/// TODO rename to `coerce_columns`
fn normalize_columns(map: &mut HashMap<String, HashMap<ColumnTypeCategory, Vec<DynamicColumn>>>) {
    for (_field_name, type_category_to_columns) in map.iter_mut() {
        for (type_category, columns) in type_category_to_columns {
            if type_category == &ColumnTypeCategory::Numerical {
                let casted_columns = cast_to_common_numerical_column(&columns);
                *columns = casted_columns;
            }
        }
    }
}

/// Receives a list of columns of numerical types (u64, i64, f64)
///
/// Returns a list of `DynamicColumn` which are all of the same numerical type
fn cast_to_common_numerical_column(columns: &[DynamicColumn]) -> Vec<DynamicColumn> {
    assert!(columns
        .iter()
        .all(|column| column.column_type().numerical_type().is_some()));
    let coerce_to_i64: Vec<_> = columns
        .iter()
        .filter_map(|column| column.clone().coerce_to_i64())
        .collect();

    if coerce_to_i64.len() == columns.len() {
        return coerce_to_i64;
    }

    let coerce_to_u64: Vec<_> = columns
        .iter()
        .filter_map(|column| column.clone().coerce_to_u64())
        .collect();

    if coerce_to_u64.len() == columns.len() {
        return coerce_to_u64;
    }

    columns
        .iter()
        .map(|column| {
            column
                .clone()
                .coerce_to_f64()
                .expect("couldn't cast column to f64")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnarWriter;

    #[test]
    fn test_column_coercion() {
        // i64 type
        let columnar1 = {
            let mut dataframe_writer = ColumnarWriter::default();
            dataframe_writer.record_numerical(1u32, "numbers", 1i64);
            let mut buffer: Vec<u8> = Vec::new();
            dataframe_writer.serialize(2, &mut buffer).unwrap();
            ColumnarReader::open(buffer).unwrap()
        };
        // u64 type
        let columnar2 = {
            let mut dataframe_writer = ColumnarWriter::default();
            dataframe_writer.record_numerical(1u32, "numbers", u64::MAX - 100);
            let mut buffer: Vec<u8> = Vec::new();
            dataframe_writer.serialize(2, &mut buffer).unwrap();
            ColumnarReader::open(buffer).unwrap()
        };

        // f64 type
        let columnar3 = {
            let mut dataframe_writer = ColumnarWriter::default();
            dataframe_writer.record_numerical(1u32, "numbers", 30.5);
            let mut buffer: Vec<u8> = Vec::new();
            dataframe_writer.serialize(2, &mut buffer).unwrap();
            ColumnarReader::open(buffer).unwrap()
        };

        let column_map =
            group_columns_for_merge(&[columnar1.clone(), columnar2.clone(), columnar3.clone()])
                .unwrap();
        assert_eq!(column_map.len(), 1);
        let cat_to_columns = column_map.get("numbers").unwrap();
        assert_eq!(cat_to_columns.len(), 1);

        let numerical = cat_to_columns.get(&ColumnTypeCategory::Numerical).unwrap();
        assert!(numerical.iter().all(|column| column.is_f64()));

        let column_map = group_columns_for_merge(&[columnar1.clone(), columnar1.clone()]).unwrap();
        assert_eq!(column_map.len(), 1);
        let cat_to_columns = column_map.get("numbers").unwrap();
        assert_eq!(cat_to_columns.len(), 1);
        let numerical = cat_to_columns.get(&ColumnTypeCategory::Numerical).unwrap();
        assert!(numerical.iter().all(|column| column.is_i64()));

        let column_map = group_columns_for_merge(&[columnar2.clone(), columnar2.clone()]).unwrap();
        assert_eq!(column_map.len(), 1);
        let cat_to_columns = column_map.get("numbers").unwrap();
        assert_eq!(cat_to_columns.len(), 1);
        let numerical = cat_to_columns.get(&ColumnTypeCategory::Numerical).unwrap();
        assert!(numerical.iter().all(|column| column.is_u64()));
    }
}
