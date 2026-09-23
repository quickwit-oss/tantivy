//! This will enhance the request tree with access to the fastfield and metadata.

use std::io;
use std::sync::Arc;

use columnar::{Column, ColumnType, DynamicColumn, DynamicColumnHandle};

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

fn resolve_registered_source(
    reader: &SegmentReader,
    value_sources: &ValueSourceRegistry,
    field_name: &str,
    allowed_column_types_opt: Option<&[ColumnType]>,
) -> crate::Result<Option<Arc<dyn ValueSource>>> {
    let Some(provider) = value_sources.get(field_name) else {
        return Ok(None);
    };
    let source = provider.for_segment(reader)?;
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
) -> crate::Result<Arc<dyn ValueSource>> {
    if let Some(registered) =
        resolve_registered_source(reader, value_sources, field_name, allowed_column_types)?
    {
        return Ok(registered);
    }
    let ff_fields = reader.fast_fields();
    let (column, column_type) = ff_fields
        .u64_lenient_for_type(allowed_column_types, field_name)?
        .unwrap_or_else(|| {
            (
                Column::build_empty_column(reader.num_docs()),
                ColumnType::U64,
            )
        });
    // The empty-column shim stays physical on purpose: several fast paths check
    // `as_column()` and would otherwise degrade for a merely absent field.
    Ok(Arc::new((column, column_type)))
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
) -> crate::Result<Vec<Arc<dyn ValueSource>>> {
    // A registered source shadows the physical type fan-out entirely.
    if let Some(registered) =
        resolve_registered_source(reader, value_sources, field_name, allowed_column_types)?
    {
        return Ok(vec![registered]);
    }
    let ff_fields = reader.fast_fields();
    let mut ff_field_with_type: Vec<(Column, ColumnType)> =
        ff_fields.u64_lenient_for_type_all(allowed_column_types, field_name)?;
    if ff_field_with_type.is_empty() {
        ff_field_with_type.push((Column::build_empty_column(reader.num_docs()), fallback_type));
    }
    Ok(ff_field_with_type
        .into_iter()
        .map(|(column, column_type)| {
            let source: Arc<dyn ValueSource> = Arc::new((column, column_type));
            source
        })
        .collect())
}
