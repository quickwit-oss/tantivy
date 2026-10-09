//! This will enhance the request tree with access to the fastfield and metadata.

use std::io;

use columnar::{BytesColumn, Column, ColumnType, DynamicColumn, DynamicColumnHandle, StrColumn};

use crate::aggregation::value_source::ValueSource;
use crate::aggregation::{f64_to_fastfield_u64, Key, ValueSourceRegistry};
use crate::index::SegmentReader;

/// Get the missing value as internal u64 representation
///
/// For terms we use u64::MAX as sentinel value
/// For numerical data we convert the value into the representation
/// we would get from the fast field, when we open it as u64_lenient_for_type.
///
/// That way we can use it the same way as if it would come from the fastfield.
pub(crate) fn get_missing_val_as_u64_lenient(
    column_type: ColumnType,
    column_max_value: u64,
    missing: &Key,
    field_name: &str,
) -> crate::Result<Option<u64>> {
    let missing_val = match missing {
        Key::Str(_) if column_type == ColumnType::Str => Some(column_max_value + 1),
        // Allow fallback to number on text fields
        Key::F64(_) if column_type == ColumnType::Str => Some(column_max_value + 1),
        Key::U64(_) if column_type == ColumnType::Str => Some(column_max_value + 1),
        Key::I64(_) if column_type == ColumnType::Str => Some(column_max_value + 1),
        Key::F64(val) if column_type.numerical_type().is_some() => {
            f64_to_fastfield_u64(*val, &column_type)
        }
        // NOTE: We may loose precision of the passed missing value by casting i64 and u64 to f64.
        Key::I64(val) if column_type.numerical_type().is_some() => {
            f64_to_fastfield_u64(*val as f64, &column_type)
        }
        Key::U64(val) if column_type.numerical_type().is_some() => {
            f64_to_fastfield_u64(*val as f64, &column_type)
        }
        _ => {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "Missing value {missing:?} for field {field_name} is not supported for column \
                 type {column_type:?}"
            )));
        }
    };
    Ok(missing_val)
}

pub(crate) fn get_numeric_or_date_column_types() -> &'static [ColumnType] {
    &[
        ColumnType::F64,
        ColumnType::U64,
        ColumnType::I64,
        ColumnType::DateTime,
    ]
}

/// Returns the registered source for `field_name` in this segment, if any.
///
/// Returns `None` if the field name is not registered, or if the registered source has a type
/// the aggregation cannot consume in this segment.
fn resolve_registered_source(
    reader: &SegmentReader,
    value_sources: &ValueSourceRegistry,
    field_name: &str,
    allowed_column_types_opt: Option<&[ColumnType]>,
) -> crate::Result<Option<Box<dyn ValueSource>>> {
    let Some(provider) = value_sources.get(field_name) else {
        return Ok(None);
    };
    let source = provider.for_segment(reader, allowed_column_types_opt)?;
    let column_type = source.column_type();
    if let Some(allowed_column_types) = allowed_column_types_opt {
        if !allowed_column_types.contains(&column_type) {
            return Ok(None);
        }
    }
    Ok(Some(source))
}

pub(crate) fn get_value_source(
    reader: &SegmentReader,
    value_sources: &ValueSourceRegistry,
    field_name: &str,
    allowed_column_types: Option<&[ColumnType]>,
) -> crate::Result<Box<dyn ValueSource>> {
    if let Some(registered) =
        resolve_registered_source(reader, value_sources, field_name, allowed_column_types)?
    {
        return Ok(registered);
    }
    if let Some(source) =
        open_physical_column_value_sources(reader, field_name, allowed_column_types, true)?.pop()
    {
        return Ok(source);
    }
    // The empty-column shim stays physical on purpose: several fast paths check
    // `as_column()` and would otherwise degrade for a merely absent field.
    Ok(Box::new((
        Column::build_empty_column(reader.num_docs()),
        ColumnType::U64,
    )))
}

pub(crate) fn get_dynamic_columns(
    reader: &SegmentReader,
    field_name: &str,
) -> crate::Result<Vec<columnar::DynamicColumn>> {
    let dyn_col_handles: Vec<DynamicColumnHandle> =
        reader.fast_fields().dynamic_column_handles(field_name)?;
    let dyn_cols: Vec<DynamicColumn> = dyn_col_handles
        .iter()
        .map(DynamicColumnHandle::open)
        .collect::<io::Result<_>>()?;
    assert!(!dyn_cols.is_empty(), "field {field_name} not found");
    Ok(dyn_cols)
}

/// Get all block_value_sources or empty as default.
///
/// Is guaranteed to return at least one column.
pub(crate) fn get_all_value_sources(
    reader: &SegmentReader,
    value_sources: &ValueSourceRegistry,
    field_name: &str,
    allowed_column_types: Option<&[ColumnType]>,
    fallback_type: ColumnType,
) -> crate::Result<Vec<Box<dyn ValueSource>>> {
    // A registered source shadows the physical type fan-out entirely.
    if let Some(registered) =
        resolve_registered_source(reader, value_sources, field_name, allowed_column_types)?
    {
        return Ok(vec![registered]);
    }
    let mut sources: Vec<Box<dyn ValueSource>> =
        open_physical_column_value_sources(reader, field_name, allowed_column_types, false)?;
    if sources.is_empty() {
        sources.push(build_empty_value_source(reader.num_docs(), fallback_type));
    }
    Ok(sources)
}

/// Builds the value source of a field without any column, with the given type.
///
/// A `Str` shim is an empty `StrColumn`, so that it has an (empty) term dictionary, as required by
/// `ValueSource::term_dictionary`.
fn build_empty_value_source(num_docs: u32, column_type: ColumnType) -> Box<dyn ValueSource> {
    if column_type == ColumnType::Str {
        Box::new(StrColumn::wrap(BytesColumn::empty(num_docs)))
    } else {
        Box::new((Column::build_empty_column(num_docs), column_type))
    }
}

/// Opens the fast-field columns of `field_name` whose type is allowed, in columnar order.
///
/// Text columns are opened as `StrColumn`, so that the source carries its dictionary. Other
/// columns use their monotonic `u64` mapping.
///
/// If `first_only` is true, at most the first allowed column is returned.
fn open_physical_column_value_sources(
    reader: &SegmentReader,
    field_name: &str,
    allowed_column_types: Option<&[ColumnType]>,
    first_only: bool,
) -> crate::Result<Vec<Box<dyn ValueSource>>> {
    let column_handles: Vec<DynamicColumnHandle> =
        reader.fast_fields().dynamic_column_handles(field_name)?;
    let mut sources: Vec<Box<dyn ValueSource>> = Vec::with_capacity(column_handles.len());
    for handle in column_handles {
        // We skip columns with a type that is not allowed.
        if let Some(allowed_column_types) = allowed_column_types {
            if !allowed_column_types.contains(&handle.column_type()) {
                continue;
            }
        }
        if let Some(column) = open_physical_column_value_source(handle)? {
            sources.push(column);
            if first_only {
                break;
            }
        }
    }
    Ok(sources)
}

/// Opens a column as a value source.
///
/// Returns `None` if the column cannot be read as `u64`.
fn open_physical_column_value_source(
    column_handle: DynamicColumnHandle,
) -> crate::Result<Option<Box<dyn ValueSource>>> {
    let column_type = column_handle.column_type();
    if column_type == ColumnType::Str {
        let DynamicColumn::Str(str_column) = column_handle.open()? else {
            return Err(crate::TantivyError::InternalError(
                "the text column could not be opened as a text column".to_string(),
            ));
        };
        return Ok(Some(Box::new(str_column)));
    }
    let Some(column) = column_handle.open_u64_lenient()? else {
        return Ok(None);
    };
    Ok(Some(Box::new((column, column_type))))
}
