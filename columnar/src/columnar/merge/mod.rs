mod merge_mapping;

// mod sorted_doc_id_column;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::net::Ipv6Addr;
use std::sync::Arc;

pub use merge_mapping::{MergeRowOrder, StackMergeOrder};

use super::writer::ColumnarSerializer;
use crate::column::{serialize_column_mappable_to_u128, serialize_column_mappable_to_u64};
use crate::columnar::column_type::ColumnTypeCategory;
use crate::columnar::writer::CompatibleNumericalTypes;
use crate::columnar::ColumnarReader;
use crate::dynamic_column::DynamicColumn;
use crate::{
    BytesColumn, Column, ColumnIndex, ColumnType, ColumnValues, NumericalType, NumericalValue,
};

pub fn merge_columnar(
    columnar_readers: &[ColumnarReader],
    mapping: MergeRowOrder,
    output: &mut impl io::Write,
) -> io::Result<()> {
    let mut serializer = ColumnarSerializer::new(output);

    // TODO handle dictionary merge for Str/Bytes column
    let columns_to_merge = group_columns_for_merge(columnar_readers)?;
    for ((column_name, column_type), columns) in columns_to_merge {
        let mut column_serializer =
            serializer.serialize_column(column_name.as_bytes(), column_type);
        merge_column(column_type, columns, &mapping, &mut column_serializer)?;
    }
    serializer.finalize(mapping.num_rows())?;

    Ok(())
}

fn dynamic_column_to_u64_monotonic(dynamic_column: DynamicColumn) -> Option<Column<u64>> {
    match dynamic_column {
        DynamicColumn::Bool(column) => Some(column.to_u64_monotonic()),
        DynamicColumn::I64(column) => Some(column.to_u64_monotonic()),
        DynamicColumn::U64(column) => Some(column.to_u64_monotonic()),
        DynamicColumn::F64(column) => Some(column.to_u64_monotonic()),
        DynamicColumn::DateTime(column) => Some(column.to_u64_monotonic()),
        DynamicColumn::IpAddr(_) | DynamicColumn::Bytes(_) | DynamicColumn::Str(_) => None,
    }
}

pub fn merge_column(
    column_type: ColumnType,
    columns: Vec<Option<DynamicColumn>>,
    merge_row_order: &MergeRowOrder,
    wrt: &mut impl io::Write,
) -> io::Result<()> {
    match column_type {
        ColumnType::I64
        | ColumnType::U64
        | ColumnType::F64
        | ColumnType::DateTime
        | ColumnType::Bool => {
            let mut column_indexes: Vec<Option<ColumnIndex>> = Vec::with_capacity(columns.len());
            let mut column_values: Vec<Arc<dyn ColumnValues>> = Vec::with_capacity(columns.len());
            for dynamic_column_opt in columns {
                if let Some(Column { idx, values }) =
                    dynamic_column_opt.and_then(dynamic_column_to_u64_monotonic)
                {
                    column_indexes.push(Some(idx));
                    column_values.push(values);
                } else {
                    column_indexes.push(None);
                }
            }
            let merged_column_index =
                crate::column_index::stack_column_index(&column_indexes[..], merge_row_order);
            serialize_column_mappable_to_u64(merged_column_index, &&column_values[..], wrt)?;
        }
        ColumnType::IpAddr => {
            let mut column_indexes: Vec<Option<ColumnIndex>> = Vec::with_capacity(columns.len());
            let mut column_values: Vec<Arc<dyn ColumnValues<Ipv6Addr>>> =
                Vec::with_capacity(columns.len());
            let mut num_values = 0;
            for dynamic_column_opt in columns {
                if let Some(DynamicColumn::IpAddr(Column { idx, values })) = dynamic_column_opt {
                    num_values += values.num_vals();
                    column_indexes.push(Some(idx));
                    column_values.push(values);
                } else {
                    column_indexes.push(None);
                }
            }
            let merged_column_index =
                crate::column_index::stack_column_index(&column_indexes[..], merge_row_order);
            serialize_column_mappable_to_u128(
                merged_column_index,
                &&column_values[..],
                num_values,
                wrt,
            )?;
        }
        ColumnType::Bytes | ColumnType::Str => {
            let mut bytes_columns: Vec<Option<BytesColumn>> = Vec::with_capacity(columns.len());
            for dynamic_column_opt in columns {
                match dynamic_column_opt {
                    Some(DynamicColumn::Str(str_column)) => {
                        bytes_columns.push(Some(str_column.into()));
                    }
                    Some(DynamicColumn::Bytes(bytes_column)) => {
                        bytes_columns.push(Some(bytes_column));
                    }
                    None => bytes_columns.push(None),
                    _ => {
                        panic!("This should never happen.");
                    }
                }
            }
            todo!();
        }
    }
    Ok(())
}

fn group_columns_for_merge(
    columnar_readers: &[ColumnarReader],
) -> io::Result<BTreeMap<(String, ColumnType), Vec<Option<DynamicColumn>>>> {
    // Each column name may have multiple types of column associated.
    // For merging we are interested in the same column type category since they can be merged.
    let mut columns_grouped: HashMap<(String, ColumnTypeCategory), Vec<Option<DynamicColumn>>> =
        HashMap::new();

    let num_columnars = columnar_readers.len();

    for (columnar_id, columnar_reader) in columnar_readers.iter().enumerate() {
        let column_name_and_handle = columnar_reader.list_columns()?;
        for (column_name, handle) in column_name_and_handle {
            let column_type_category: ColumnTypeCategory = handle.column_type().into();
            let columns = columns_grouped
                .entry((column_name, column_type_category))
                .or_insert_with(|| vec![None; num_columnars]);
            let column = handle.open()?;
            columns[columnar_id] = Some(column);
        }
    }

    let mut merge_columns: BTreeMap<(String, ColumnType), Vec<Option<DynamicColumn>>> =
        BTreeMap::default();

    for ((column_name, col_category), mut columns) in columns_grouped {
        if col_category == ColumnTypeCategory::Numerical {
            coerce_numerical_columns_to_same_type(&mut columns);
        }
        let column_type = columns
            .iter()
            .flatten()
            .map(|col| col.column_type())
            .next()
            .unwrap();
        merge_columns.insert((column_name, column_type), columns);
    }

    Ok(merge_columns)
}

/// Coerce a set of numerical columns to the same type.
///
/// If all columns are already from the same type, keep this type
/// (even if they could all be coerced to i64).
fn coerce_numerical_columns_to_same_type(columns: &mut [Option<DynamicColumn>]) {
    let mut column_types: HashSet<NumericalType> = HashSet::default();
    let mut compatible_numerical_types = CompatibleNumericalTypes::default();
    for column in columns.iter().flatten() {
        let min_value: NumericalValue;
        let max_value: NumericalValue;
        match column {
            DynamicColumn::I64(column) => {
                min_value = column.min_value().into();
                max_value = column.max_value().into();
            }
            DynamicColumn::U64(column) => {
                min_value = column.min_value().into();
                max_value = column.min_value().into();
            }
            DynamicColumn::F64(column) => {
                min_value = column.min_value().into();
                max_value = column.min_value().into();
            }
            DynamicColumn::Bool(_)
            | DynamicColumn::IpAddr(_)
            | DynamicColumn::DateTime(_)
            | DynamicColumn::Bytes(_)
            | DynamicColumn::Str(_) => {
                panic!("We expected only numerical columns.");
            }
        }
        column_types.insert(column.column_type().numerical_type().unwrap());
        compatible_numerical_types.accept_value(min_value);
        compatible_numerical_types.accept_value(max_value);
    }
    if column_types.len() <= 1 {
        // No need to do anything. The columns are already all from the same type.
        // This is necessary to let use force a given type.

        // TODO This works in a world where we do not allow a change of schema,
        // but in the future, we will have to pass some kind of schema to enforce
        // the logic.
        return;
    }
    let coerce_type = compatible_numerical_types.to_numerical_type();
    for column_opt in columns.iter_mut() {
        if let Some(column) = column_opt.take() {
            *column_opt = column.coerce_numerical(coerce_type);
        }
    }
}

#[cfg(test)]
mod tests;
