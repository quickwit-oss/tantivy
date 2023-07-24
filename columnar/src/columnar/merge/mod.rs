mod merge_dict_column;
mod merge_mapping;
mod term_merger;

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::Ipv6Addr;
use std::rc::Rc;
use std::sync::Arc;

use common::GroupByIteratorExtended;
use itertools::{EitherOrBoth, Itertools};
pub use merge_mapping::{MergeRowOrder, ShuffleMergeOrder, StackMergeOrder};

use super::writer::ColumnarSerializer;
use crate::column::{serialize_column_mappable_to_u128, serialize_column_mappable_to_u64};
use crate::column_values::MergedColumnValues;
use crate::columnar::merge::merge_dict_column::merge_bytes_or_str_column;
use crate::columnar::writer::CompatibleNumericalTypes;
use crate::columnar::ColumnarReader;
use crate::dynamic_column::DynamicColumn;
use crate::{
    BytesColumn, Column, ColumnIndex, ColumnType, ColumnValues, DynamicColumnHandle, NumericalType,
    NumericalValue,
};

/// Column types are grouped into different categories.
/// After merge, all columns belonging to the same category are coerced to
/// the same column type.
///
/// In practise, today, only Numerical colummns are coerced into one type today.
///
/// See also [README.md].
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(crate) enum ColumnTypeCategory {
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

/// Merge several columnar table together.
///
/// If several columns with the same name are conflicting with the numerical types in the
/// input columnars, the first type compatible out of i64, u64, f64 in that order will be used.
///
/// `require_columns` makes it possible to ensure that some columns will be present in the
/// resulting columnar. When a required column is a numerical column type, one of two things can
/// happen:
/// - If the required column type is compatible with all of the input columnar, the resulsting
///   merged
/// columnar will simply coerce the input column and use the required column type.
/// - If the required column type is incompatible with one of the input columnar, the merged
/// will fail with an InvalidData error.
///
/// `merge_row_order` makes it possible to remove or reorder row in the resulting
/// `Columnar` table.
///
/// Reminder: a string and a numerical column may bare the same column name. This is not
/// considered a conflict.
pub fn merge_columnar(
    columnar_readers: &[&ColumnarReader],
    required_columns: &[(String, ColumnType)],
    merge_row_order: MergeRowOrder,
    output: &mut impl io::Write,
) -> io::Result<()> {
    let mut serializer = ColumnarSerializer::new(output);
    let num_rows_per_columnar = columnar_readers
        .iter()
        .map(|reader| reader.num_rows())
        .collect::<Vec<u32>>();

    let columns_to_merge_iter =
        group_columns_for_merge_iter(columnar_readers, required_columns, &merge_row_order)?;
    for res in columns_to_merge_iter {
        let (column_name, column_type, grouped_columns) = res?;
        let columns = grouped_columns.columns;

        let mut column_serializer =
            serializer.start_serialize_column(column_name.as_bytes(), column_type);
        merge_column(
            column_type,
            &num_rows_per_columnar,
            columns,
            &merge_row_order,
            &mut column_serializer,
        )?;
        column_serializer.finalize()?;
    }

    serializer.finalize(merge_row_order.num_rows())?;
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

fn merge_column(
    column_type: ColumnType,
    num_docs_per_column: &[u32],
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
            let mut column_indexes: Vec<ColumnIndex> = Vec::with_capacity(columns.len());
            let mut column_values: Vec<Option<Arc<dyn ColumnValues>>> =
                Vec::with_capacity(columns.len());
            for (i, dynamic_column_opt) in columns.into_iter().enumerate() {
                if let Some(Column { index: idx, values }) =
                    dynamic_column_opt.and_then(dynamic_column_to_u64_monotonic)
                {
                    column_indexes.push(idx);
                    column_values.push(Some(values));
                } else {
                    column_indexes.push(ColumnIndex::Empty {
                        num_docs: num_docs_per_column[i],
                    });
                    column_values.push(None);
                }
            }
            let merged_column_index =
                crate::column_index::merge_column_index(&column_indexes[..], merge_row_order);
            let merge_column_values = MergedColumnValues {
                column_indexes: &column_indexes[..],
                column_values: &column_values[..],
                merge_row_order,
            };
            serialize_column_mappable_to_u64(merged_column_index, &merge_column_values, wrt)?;
        }
        ColumnType::IpAddr => {
            let mut column_indexes: Vec<ColumnIndex> = Vec::with_capacity(columns.len());
            let mut column_values: Vec<Option<Arc<dyn ColumnValues<Ipv6Addr>>>> =
                Vec::with_capacity(columns.len());
            for (i, dynamic_column_opt) in columns.into_iter().enumerate() {
                if let Some(DynamicColumn::IpAddr(Column { index: idx, values })) =
                    dynamic_column_opt
                {
                    column_indexes.push(idx);
                    column_values.push(Some(values));
                } else {
                    column_indexes.push(ColumnIndex::Empty {
                        num_docs: num_docs_per_column[i],
                    });
                    column_values.push(None);
                }
            }

            let merged_column_index =
                crate::column_index::merge_column_index(&column_indexes[..], merge_row_order);
            let merge_column_values = MergedColumnValues {
                column_indexes: &column_indexes[..],
                column_values: &column_values,
                merge_row_order,
            };

            serialize_column_mappable_to_u128(merged_column_index, &merge_column_values, wrt)?;
        }
        ColumnType::Bytes | ColumnType::Str => {
            let mut column_indexes: Vec<ColumnIndex> = Vec::with_capacity(columns.len());
            let mut bytes_columns: Vec<Option<BytesColumn>> = Vec::with_capacity(columns.len());
            for (i, dynamic_column_opt) in columns.into_iter().enumerate() {
                match dynamic_column_opt {
                    Some(DynamicColumn::Str(str_column)) => {
                        column_indexes.push(str_column.term_ord_column.index.clone());
                        bytes_columns.push(Some(str_column.into()));
                    }
                    Some(DynamicColumn::Bytes(bytes_column)) => {
                        column_indexes.push(bytes_column.term_ord_column.index.clone());
                        bytes_columns.push(Some(bytes_column));
                    }
                    _ => {
                        column_indexes.push(ColumnIndex::Empty {
                            num_docs: num_docs_per_column[i],
                        });
                        bytes_columns.push(None);
                    }
                }
            }
            let merged_column_index =
                crate::column_index::merge_column_index(&column_indexes[..], merge_row_order);
            merge_bytes_or_str_column(merged_column_index, &bytes_columns, merge_row_order, wrt)?;
        }
    }
    Ok(())
}

struct GroupedColumns {
    required_column_type: Option<ColumnType>,
    columns: Vec<Option<DynamicColumn>>,
}

impl GroupedColumns {
    fn new(num_columnars: usize) -> Self {
        GroupedColumns {
            required_column_type: None,
            columns: vec![None; num_columnars],
        }
    }

    /// Set the dynamic column for a given columnar.
    fn set_column(&mut self, columnar_id: usize, column: DynamicColumn) {
        self.columns[columnar_id] = Some(column);
    }

    /// Force the existence of a column, as well as its type.
    fn require_type(&mut self, required_type: ColumnType) -> io::Result<()> {
        if let Some(existing_required_type) = self.required_column_type {
            if existing_required_type == required_type {
                // This was just a duplicate in the `required_columns`.
                // Nothing to do.
                return Ok(());
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Required column conflicts with another required column of the same type \
                     category.",
                ));
            }
        }
        self.required_column_type = Some(required_type);
        Ok(())
    }

    /// Returns the column type after merge.
    ///
    /// This method does not check if the column types can actually be coerced to
    /// this type.
    fn column_type_after_merge(&self) -> ColumnType {
        if let Some(required_type) = self.required_column_type {
            return required_type;
        }
        let column_type: HashSet<ColumnType> = self
            .columns
            .iter()
            .flatten()
            .map(|column| column.column_type())
            .collect();
        if column_type.len() == 1 {
            return column_type.into_iter().next().unwrap();
        }
        // At the moment, only the numerical categorical column type has more than one possible
        // column type.
        assert!(self
            .columns
            .iter()
            .flatten()
            .all(|el| ColumnTypeCategory::from(el.column_type()) == ColumnTypeCategory::Numerical));
        merged_numerical_columns_type(self.columns.iter().flatten()).into()
    }
}

/// Returns the type of the merged numerical column.
///
/// This function picks the first numerical type out of i64, u64, f64 (order matters
/// here), that is compatible with all the `columns`.
///
/// # Panics
/// Panics if one of the column is not numerical.
fn merged_numerical_columns_type<'a>(
    columns: impl Iterator<Item = &'a DynamicColumn>,
) -> NumericalType {
    let mut compatible_numerical_types = CompatibleNumericalTypes::default();
    for column in columns {
        let (min_value, max_value) =
            min_max_if_numerical(column).expect("All columns re required to be numerical");
        compatible_numerical_types.accept_value(min_value);
        compatible_numerical_types.accept_value(max_value);
    }
    compatible_numerical_types.to_numerical_type()
}

fn is_empty_after_merge(
    merge_row_order: &MergeRowOrder,
    column: &DynamicColumn,
    columnar_ord: usize,
) -> bool {
    if column.num_values() == 0u32 {
        // It was empty before the merge.
        return true;
    }
    match merge_row_order {
        MergeRowOrder::Stack(_) => {
            // If we are stacking the columnar, no rows are being deleted.
            false
        }
        MergeRowOrder::Shuffled(shuffled) => {
            if let Some(alive_bitset) = &shuffled.alive_bitsets[columnar_ord] {
                let column_index = column.column_index();
                match column_index {
                    ColumnIndex::Empty { .. } => true,
                    ColumnIndex::Full => alive_bitset.len() == 0,
                    ColumnIndex::Optional(optional_index) => {
                        for doc in optional_index.iter_rows() {
                            if alive_bitset.contains(doc) {
                                return false;
                            }
                        }
                        true
                    }
                    ColumnIndex::Multivalued(multivalued_index) => {
                        for (doc_id, (start_index, end_index)) in multivalued_index
                            .start_index_column
                            .iter()
                            .tuple_windows()
                            .enumerate()
                        {
                            let doc_id = doc_id as u32;
                            if start_index == end_index {
                                // There are no values in this document
                                continue;
                            }
                            // The document contains values and is present in the alive bitset.
                            // The column is therefore not empty.
                            if alive_bitset.contains(doc_id) {
                                return false;
                            }
                        }
                        true
                    }
                }
            } else {
                // No document is being deleted.
                // The shuffle is applying a permutation.
                false
            }
        }
    }
}

type MergeIter<'a> =
    Box<dyn Iterator<Item = io::Result<(Rc<str>, ColumnType, GroupedColumns)>> + 'a>;

#[derive(Debug, Clone)]
struct MergeColumn {
    column_name: Rc<str>,
    reader_ord: usize,
    column: DynamicColumnHandle,
}
impl MergeColumn {
    fn new(column_name: Rc<str>, reader_ord: usize, column: DynamicColumnHandle) -> Self {
        MergeColumn {
            column_name,
            reader_ord,
            column,
        }
    }
}

/// Iterates over the columns of the columnar readers, grouped by column name.
/// Key functionality is that `open` of the Columns is done lazy per group.
fn group_columns_for_merge_iter<'a>(
    columnar_readers: &'a [&'a ColumnarReader],
    required_columns: &'a [(String, ColumnType)],
    merge_row_order: &'a MergeRowOrder,
) -> io::Result<impl Iterator<Item = io::Result<(Rc<str>, ColumnType, GroupedColumns)>> + 'a> {
    // One iterator per columnar reader.
    let column_iters: Vec<_> = columnar_readers
        .iter()
        .enumerate()
        .map(|(reader_ord, reader)| {
            Ok(reader
                .iter_columns()?
                .map(move |el| MergeColumn::new(Rc::from(el.0), reader_ord, el.1)))
        })
        .collect::<io::Result<_>>()?;
    let required_columns_map: HashMap<String, ColumnType> = required_columns
        .iter()
        .map(|(col_name, typ)| (col_name.to_string(), *typ))
        .collect::<HashMap<String, _>>();
    let mut required_columns_list: Vec<String> = required_columns
        .iter()
        .map(|(col_name, _)| col_name.to_string())
        .collect();
    required_columns_list.sort();

    // Kmerge on column_name
    let kmerge = column_iters
        .into_iter()
        .kmerge_by(|a, b| a.column_name < b.column_name);
    // Group by on column_name.
    let group_iter = GroupByIteratorExtended::group_by(kmerge, |el| el.column_name.clone());

    // Weave in the required columns into the sorted by column name iterator.
    let groups_with_required = required_columns_list
        .into_iter()
        .merge_join_by(group_iter, |left, right| (left.as_str()).cmp(&right.0));

    Ok(groups_with_required.flat_map(move |either| {
        // It should be possible to do the grouping also on the column type in one pass, but some
        // tests are failing.
        let mut force_type: Option<ColumnType> = None;
        let (column_name, group) = match either {
            // set required column
            EitherOrBoth::Both(_required, (key, group)) => {
                force_type = required_columns_map.get(&*key).cloned();
                (key, group)
            }
            // Only required - Return artificial empty column
            EitherOrBoth::Left(column_name) => {
                return generate_require_column(
                    Rc::from(column_name),
                    columnar_readers,
                    &required_columns_map,
                );
            }
            // no required column
            EitherOrBoth::Right((key, group)) => (key, group),
        };
        let mut group: Vec<MergeColumn> = group.collect();
        // We need to create an iterator that returns the columns in the order of `to_code` of
        // ColumnType
        group.sort_by_key(|el| el.column.column_type);
        let group_iter = GroupByIteratorExtended::group_by(group.into_iter(), |el| {
            let cat_type: ColumnTypeCategory = el.column.column_type().into();
            cat_type
        });
        let group_column_iter = group_iter.map(move |(_cat, group)| {
            group_columns_iter(
                column_name.clone(),
                columnar_readers,
                force_type,
                merge_row_order,
                group,
            )
        });
        let iter = group_column_iter.filter(move |res| {
            // Filter out empty columns.
            res.as_ref()
                .map(|(_, _, group)| {
                    let column_is_required = force_type.is_some();
                    if column_is_required {
                        return true;
                    }
                    let all_columns_none = group.columns.iter().all(|column| column.is_none());
                    !all_columns_none
                })
                .unwrap_or(true)
        });
        Box::new(iter)
    }))
}

fn generate_require_column<'a>(
    column_name: Rc<str>,
    columnar_readers: &'a [&'a ColumnarReader],
    required_columns_map: &HashMap<String, ColumnType>,
) -> MergeIter<'a> {
    let mut grouped_columns = GroupedColumns::new(columnar_readers.len());
    let force_type: ColumnType = required_columns_map.get(&*column_name).cloned().unwrap();
    grouped_columns.require_type(force_type).unwrap(); // Can't panic
    Box::new(std::iter::once(Ok((
        column_name,
        force_type,
        grouped_columns,
    )))) as MergeIter<'a>
}

fn group_columns_iter<'a>(
    column_name: Rc<str>,
    columnar_readers: &'a [&'a ColumnarReader],
    force_type: Option<ColumnType>,
    merge_row_order: &'a MergeRowOrder,
    group: impl Iterator<Item = MergeColumn>,
) -> io::Result<(Rc<str>, ColumnType, GroupedColumns)> {
    let mut grouped_columns = GroupedColumns::new(columnar_readers.len());
    if let Some(force_type) = force_type {
        grouped_columns.require_type(force_type)?;
    }
    for col in group {
        let columnar_ord = col.reader_ord;
        let column = col.column.open()?;
        if !is_empty_after_merge(merge_row_order, &column, columnar_ord) {
            grouped_columns.set_column(col.reader_ord, column);
        }
    }

    let column_type = grouped_columns.column_type_after_merge();
    coerce_columns(column_type, &mut grouped_columns.columns)?;

    Ok((column_name, column_type, grouped_columns))
}

fn coerce_columns(
    column_type: ColumnType,
    columns: &mut [Option<DynamicColumn>],
) -> io::Result<()> {
    for column_opt in columns.iter_mut() {
        if let Some(column) = column_opt.take() {
            *column_opt = Some(coerce_column(column_type, column)?);
        }
    }
    Ok(())
}

fn coerce_column(column_type: ColumnType, column: DynamicColumn) -> io::Result<DynamicColumn> {
    if let Some(numerical_type) = column_type.numerical_type() {
        column
            .coerce_numerical(numerical_type)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, ""))
    } else {
        if column.column_type() != column_type {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Cannot coerce column of type `{:?}` to `{column_type:?}`",
                    column.column_type()
                ),
            ));
        }
        Ok(column)
    }
}

/// Returns the (min, max) of a column provided it is numerical (i64, u64. f64).
///
/// The min and the max are simply the numerical value as defined by `ColumnValue::min_value()`,
/// and `ColumnValue::max_value()`.
///
/// It is important to note that these values are only guaranteed to be lower/upper bound
/// (as opposed to min/max value).
/// If a column is empty, the min and max values are currently set to 0.
fn min_max_if_numerical(column: &DynamicColumn) -> Option<(NumericalValue, NumericalValue)> {
    match column {
        DynamicColumn::I64(column) => Some((column.min_value().into(), column.max_value().into())),
        DynamicColumn::U64(column) => Some((column.min_value().into(), column.max_value().into())),
        DynamicColumn::F64(column) => Some((column.min_value().into(), column.max_value().into())),
        DynamicColumn::Bool(_)
        | DynamicColumn::IpAddr(_)
        | DynamicColumn::DateTime(_)
        | DynamicColumn::Bytes(_)
        | DynamicColumn::Str(_) => None,
    }
}

#[cfg(test)]
mod tests;
