mod merge_dict_column;
mod merge_mapping;
mod merge_plain_column;
mod term_merger;

use std::collections::{BTreeMap, HashSet};
use std::io;
use std::net::Ipv6Addr;
use std::sync::Arc;

pub use merge_dict_column::compute_merged_term_ord_mapping;
pub use merge_mapping::{MergeRowOrder, ShuffleMergeOrder, StackMergeOrder};

use super::writer::ColumnarSerializer;
use crate::column::{serialize_column_mappable_to_u64, serialize_column_mappable_to_u128};
use crate::column_values::MergedColumnValues;
use crate::columnar::merge::merge_dict_column::merge_bytes_or_str_column;
use crate::columnar::merge::merge_plain_column::{
    PlainMergeInput, merge_plain_bytes_or_str_column,
};
use crate::columnar::writer::CompatibleNumericalTypes;
use crate::columnar::{ColumnarReader, StrColumnCodec, column_key_code};
use crate::dynamic_column::DynamicColumn;
use crate::{
    BytesColumn, Column, ColumnIndex, ColumnType, ColumnValues, DynamicColumnHandle, NumericalType,
    NumericalValue,
};

/// Column types are grouped into different categories.
/// After merge, all columns belonging to the same category are coerced to
/// the same column type.
///
/// In practise, today, only Numerical columns are coerced into one type today.
///
/// See also [README.md].
///
/// The merged columns are sorted by key before being serialized (see
/// `column_key_code_lower_bound`): the ordering of the categories does not need to match the
/// ordering of the column key codes.
#[derive(Copy, Clone, Eq, PartialOrd, Ord, PartialEq, Hash, Debug)]
pub(crate) enum ColumnTypeCategory {
    Numerical,
    Bytes,
    Str,
    Bool,
    IpAddr,
    DateTime,
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
/// - If the required column type is compatible with all of the input columnar, the resulting merged
///   columnar will simply coerce the input column and use the required column type.
/// - If the required column type is incompatible with one of the input columnar, the merged will
///   fail with an InvalidData error.
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
    let required_columns_with_codecs: Vec<(String, ColumnType, StrColumnCodec)> = required_columns
        .iter()
        .map(|(column_name, column_type)| {
            (
                column_name.clone(),
                *column_type,
                StrColumnCodec::Dictionary,
            )
        })
        .collect();
    merge_columnar_with_codecs(
        columnar_readers,
        &required_columns_with_codecs,
        merge_row_order,
        output,
    )
}

/// Same as [`merge_columnar`], with the codec of the required `Str` and `Bytes` columns.
///
/// A merged `Str` or `Bytes` column uses the plain codec if it is required with the plain
/// codec, or if one of the merged columns uses it. Otherwise, it is dictionary encoded.
pub fn merge_columnar_with_codecs(
    columnar_readers: &[&ColumnarReader],
    required_columns: &[(String, ColumnType, StrColumnCodec)],
    merge_row_order: MergeRowOrder,
    output: &mut impl io::Write,
) -> io::Result<()> {
    let mut serializer = ColumnarSerializer::new(output);
    let num_docs_per_columnar = columnar_readers
        .iter()
        .map(|reader| reader.num_docs())
        .collect::<Vec<u32>>();

    let columns_to_merge = group_columns_for_merge_with_codecs(columnar_readers, required_columns)?;
    // Columns must be serialized in the order of their keys. The key code of a plain column
    // sorts after all of the other column types, so the category order is not enough.
    let mut columns_to_merge: Vec<((String, ColumnTypeCategory), GroupedColumnsHandle)> =
        columns_to_merge.into_iter().collect();
    columns_to_merge.sort_by_cached_key(
        |((column_name, column_type_category), grouped_columns)| {
            let key_code_lower_bound = column_key_code_lower_bound(
                *column_type_category,
                grouped_columns.codec_after_merge(),
            );
            (column_name.clone(), key_code_lower_bound)
        },
    );
    for res in columns_to_merge {
        let ((column_name, _column_type_category), grouped_columns_handle) = res;
        let codec_after_merge = grouped_columns_handle.codec_after_merge();
        let grouped_columns = grouped_columns_handle.open(&merge_row_order)?;
        if grouped_columns.is_empty() {
            continue;
        }

        let column_type_after_merge = grouped_columns.column_type_after_merge();
        let codec = if matches!(column_type_after_merge, ColumnType::Str | ColumnType::Bytes) {
            codec_after_merge
        } else {
            StrColumnCodec::Dictionary
        };
        let mut columns = grouped_columns.columns;
        // Make sure the number of columns is the same as the number of columnar readers.
        // Or num_docs_per_columnar would be incorrect.
        assert_eq!(columns.len(), columnar_readers.len());
        coerce_columns(column_type_after_merge, &mut columns)?;

        let mut column_serializer = serializer.start_serialize_column_with_codec(
            column_name.as_bytes(),
            column_type_after_merge,
            codec,
        );
        merge_column(
            column_type_after_merge,
            codec,
            &num_docs_per_columnar,
            columns,
            &merge_row_order,
            &mut column_serializer,
        )?;
        column_serializer.finalize()?;
    }

    serializer.finalize(merge_row_order.num_rows())?;
    Ok(())
}

/// Returns the smallest key code a merged column of `column_type_category` can have.
///
/// The numerical type of a merged numerical column is only known once the columns are opened.
/// All of the numerical key codes are smaller than the key codes of the other categories, so the
/// order does not depend on it.
fn column_key_code_lower_bound(
    column_type_category: ColumnTypeCategory,
    str_codec: StrColumnCodec,
) -> u8 {
    match column_type_category {
        ColumnTypeCategory::Numerical => {
            column_key_code(ColumnType::I64, StrColumnCodec::Dictionary)
        }
        ColumnTypeCategory::Bytes => column_key_code(ColumnType::Bytes, str_codec),
        ColumnTypeCategory::Str => column_key_code(ColumnType::Str, str_codec),
        ColumnTypeCategory::Bool => column_key_code(ColumnType::Bool, StrColumnCodec::Dictionary),
        ColumnTypeCategory::IpAddr => {
            column_key_code(ColumnType::IpAddr, StrColumnCodec::Dictionary)
        }
        ColumnTypeCategory::DateTime => {
            column_key_code(ColumnType::DateTime, StrColumnCodec::Dictionary)
        }
    }
}

fn dynamic_column_to_u64_monotonic(dynamic_column: DynamicColumn) -> Option<Column<u64>> {
    match dynamic_column {
        DynamicColumn::Bool(column) => Some(column.to_u64_monotonic()),
        DynamicColumn::I64(column) => Some(column.to_u64_monotonic()),
        DynamicColumn::U64(column) => Some(column.to_u64_monotonic()),
        DynamicColumn::F64(column) => Some(column.to_u64_monotonic()),
        DynamicColumn::DateTime(column) => Some(column.to_u64_monotonic()),
        DynamicColumn::IpAddr(_)
        | DynamicColumn::Bytes(_)
        | DynamicColumn::Str(_)
        | DynamicColumn::PlainBytes(_)
        | DynamicColumn::PlainStr(_) => None,
    }
}

fn merge_column(
    column_type: ColumnType,
    codec: StrColumnCodec,
    num_docs_per_column: &[u32],
    columns_to_merge: Vec<Option<DynamicColumn>>,
    merge_row_order: &MergeRowOrder,
    wrt: &mut impl io::Write,
) -> io::Result<()> {
    match column_type {
        ColumnType::I64
        | ColumnType::U64
        | ColumnType::F64
        | ColumnType::DateTime
        | ColumnType::Bool => {
            let mut column_indexes: Vec<ColumnIndex> = Vec::with_capacity(columns_to_merge.len());
            let mut column_values: Vec<Option<Arc<dyn ColumnValues>>> =
                Vec::with_capacity(columns_to_merge.len());
            for (i, dynamic_column_opt) in columns_to_merge.into_iter().enumerate() {
                match dynamic_column_opt.and_then(dynamic_column_to_u64_monotonic) {
                    Some(Column { index: idx, values }) => {
                        column_indexes.push(idx);
                        column_values.push(Some(values));
                    }
                    None => {
                        column_indexes.push(ColumnIndex::Empty {
                            num_docs: num_docs_per_column[i],
                        });
                        column_values.push(None);
                    }
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
            let mut column_indexes: Vec<ColumnIndex> = Vec::with_capacity(columns_to_merge.len());
            let mut column_values: Vec<Option<Arc<dyn ColumnValues<Ipv6Addr>>>> =
                Vec::with_capacity(columns_to_merge.len());
            for (i, dynamic_column_opt) in columns_to_merge.into_iter().enumerate() {
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
        ColumnType::Bytes | ColumnType::Str if codec == StrColumnCodec::Plain => {
            let mut column_indexes: Vec<ColumnIndex> = Vec::with_capacity(columns_to_merge.len());
            let mut inputs: Vec<Option<PlainMergeInput>> =
                Vec::with_capacity(columns_to_merge.len());
            for (i, dynamic_column_opt) in columns_to_merge.into_iter().enumerate() {
                let input_opt: Option<PlainMergeInput> = match dynamic_column_opt {
                    Some(DynamicColumn::PlainStr(plain_str_column)) => {
                        Some(PlainMergeInput::plain(&plain_str_column))
                    }
                    Some(DynamicColumn::PlainBytes(plain_bytes_column)) => {
                        Some(PlainMergeInput::plain(&plain_bytes_column))
                    }
                    Some(DynamicColumn::Str(str_column)) => {
                        Some(PlainMergeInput::Dictionary(str_column.into()))
                    }
                    Some(DynamicColumn::Bytes(bytes_column)) => {
                        Some(PlainMergeInput::Dictionary(bytes_column))
                    }
                    _ => None,
                };
                match &input_opt {
                    Some(input) => column_indexes.push(input.column_index().clone()),
                    None => column_indexes.push(ColumnIndex::Empty {
                        num_docs: num_docs_per_column[i],
                    }),
                }
                inputs.push(input_opt);
            }
            let merged_column_index =
                crate::column_index::merge_column_index(&column_indexes[..], merge_row_order);
            merge_plain_bytes_or_str_column(merged_column_index, inputs, merge_row_order, wrt)?;
        }
        ColumnType::Bytes | ColumnType::Str => {
            let mut column_indexes: Vec<ColumnIndex> = Vec::with_capacity(columns_to_merge.len());
            let mut bytes_columns: Vec<Option<BytesColumn>> =
                Vec::with_capacity(columns_to_merge.len());
            for (i, dynamic_column_opt) in columns_to_merge.into_iter().enumerate() {
                match dynamic_column_opt {
                    Some(DynamicColumn::Str(str_column)) => {
                        column_indexes.push(str_column.term_ord_column.index.clone());
                        bytes_columns.push(Some(str_column.into()));
                    }
                    Some(DynamicColumn::Bytes(bytes_column)) => {
                        column_indexes.push(bytes_column.term_ord_column.index.clone());
                        bytes_columns.push(Some(bytes_column));
                    }
                    Some(DynamicColumn::PlainStr(_) | DynamicColumn::PlainBytes(_)) => {
                        // `codec_after_merge` is plain as soon as one input is plain.
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "a plain-encoded column cannot be merged into a dictionary encoded \
                             column",
                        ));
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
    /// Check is column group can be skipped during serialization.
    fn is_empty(&self) -> bool {
        self.required_column_type.is_none() && self.columns.iter().all(Option::is_none)
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
        // At the moment, only the numerical column type category has more than one possible
        // column type.
        assert!(
            self.columns
                .iter()
                .flatten()
                .all(|el| ColumnTypeCategory::from(el.column_type())
                    == ColumnTypeCategory::Numerical)
        );
        merged_numerical_columns_type(self.columns.iter().flatten()).into()
    }
}

struct GroupedColumnsHandle {
    required_column_type: Option<ColumnType>,
    /// Plain if the column is required with the plain codec.
    required_codec: StrColumnCodec,
    columns: Vec<Option<DynamicColumnHandle>>,
}

impl GroupedColumnsHandle {
    fn new(num_columnars: usize) -> Self {
        GroupedColumnsHandle {
            required_column_type: None,
            required_codec: StrColumnCodec::Dictionary,
            columns: vec![None; num_columnars],
        }
    }

    /// Returns the codec of the merged column, if it is a `Str` or `Bytes` column.
    ///
    /// The plain codec wins: dictionary encoded columns can be converted into plain-encoded
    /// columns, not the other way around.
    fn codec_after_merge(&self) -> StrColumnCodec {
        let has_plain_column = self
            .columns
            .iter()
            .flatten()
            .any(|column_handle| column_handle.str_codec() == Some(StrColumnCodec::Plain));
        if has_plain_column || self.required_codec == StrColumnCodec::Plain {
            StrColumnCodec::Plain
        } else {
            StrColumnCodec::Dictionary
        }
    }
    fn open(self, merge_row_order: &MergeRowOrder) -> io::Result<GroupedColumns> {
        let mut columns: Vec<Option<DynamicColumn>> = Vec::new();
        for (columnar_id, column) in self.columns.iter().enumerate() {
            if let Some(column) = column {
                let column = column.open()?;
                // We skip columns that end up with 0 documents.
                // That way, we make sure they don't end up influencing the merge type or
                // creating empty columns.

                if is_empty_after_merge(merge_row_order, &column, columnar_id) {
                    columns.push(None);
                } else {
                    columns.push(Some(column));
                }
            } else {
                columns.push(None);
            }
        }
        Ok(GroupedColumns {
            required_column_type: self.required_column_type,
            columns,
        })
    }

    /// Set the dynamic column for a given columnar.
    fn set_column(&mut self, columnar_id: usize, column: DynamicColumnHandle) {
        self.columns[columnar_id] = Some(column);
    }

    /// Force the existence of a column, as well as its type and its codec.
    fn require_type(
        &mut self,
        required_type: ColumnType,
        required_codec: StrColumnCodec,
    ) -> io::Result<()> {
        if required_codec == StrColumnCodec::Plain {
            self.required_codec = StrColumnCodec::Plain;
        }
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
                        for doc in optional_index.iter_non_null_docs() {
                            if alive_bitset.contains(doc) {
                                return false;
                            }
                        }
                        true
                    }
                    ColumnIndex::Multivalued(multivalued_index) => {
                        for alive_docid in alive_bitset.iter() {
                            if !multivalued_index.range(alive_docid).is_empty() {
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

/// Same as [`group_columns_for_merge_with_codecs`], with dictionary encoded required columns.
#[cfg(test)]
fn group_columns_for_merge(
    columnar_readers: &[&ColumnarReader],
    required_columns: &[(String, ColumnType)],
) -> io::Result<BTreeMap<(String, ColumnTypeCategory), GroupedColumnsHandle>> {
    let required_columns_with_codecs: Vec<(String, ColumnType, StrColumnCodec)> = required_columns
        .iter()
        .map(|(column_name, column_type)| {
            (
                column_name.clone(),
                *column_type,
                StrColumnCodec::Dictionary,
            )
        })
        .collect();
    group_columns_for_merge_with_codecs(columnar_readers, &required_columns_with_codecs)
}

/// Iterates over the columns of the columnar readers, grouped by column name.
/// Key functionality is that `open` of the Columns is done lazy per group.
fn group_columns_for_merge_with_codecs<'a>(
    columnar_readers: &'a [&'a ColumnarReader],
    required_columns: &'a [(String, ColumnType, StrColumnCodec)],
) -> io::Result<BTreeMap<(String, ColumnTypeCategory), GroupedColumnsHandle>> {
    let mut columns: BTreeMap<(String, ColumnTypeCategory), GroupedColumnsHandle> = BTreeMap::new();

    for &(ref column_name, column_type, codec) in required_columns {
        if codec == StrColumnCodec::Plain
            && !matches!(column_type, ColumnType::Str | ColumnType::Bytes)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the plain codec is only supported for str and bytes columns",
            ));
        }
        columns
            .entry((column_name.clone(), column_type.into()))
            .or_insert_with(|| GroupedColumnsHandle::new(columnar_readers.len()))
            .require_type(column_type, codec)?;
    }

    for (columnar_id, columnar_reader) in columnar_readers.iter().enumerate() {
        let column_name_and_handle = columnar_reader.iter_columns()?;

        for (column_name, handle) in column_name_and_handle {
            let column_category: ColumnTypeCategory = handle.column_type().into();
            columns
                .entry((column_name, column_category))
                .or_insert_with(|| GroupedColumnsHandle::new(columnar_readers.len()))
                .set_column(columnar_id, handle);
        }
    }
    Ok(columns)
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
        | DynamicColumn::Str(_)
        | DynamicColumn::PlainBytes(_)
        | DynamicColumn::PlainStr(_) => None,
    }
}

#[cfg(test)]
mod tests;
