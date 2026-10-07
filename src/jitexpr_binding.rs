//! Binding of jitexpr variables to the fast-field columns of a segment.
//!
//! Shared by the jitexpr query predicate and the jitexpr aggregation value source, so that a
//! variable resolves to the same column in both.

use std::io;

use columnar::{ColumnType, DynamicColumnHandle};
use jitexpr::ast::InferredTypeSet;
use jitexpr::types::VarType;

use crate::index::SegmentReader;

/// Returns the handle of the column a variable named `name` is bound to, if any.
///
/// A fast field can have several columns with different types (e.g. a JSON path). We pick the
/// first column whose type is accepted by the variable. This CAN yield unexpected results for
/// some expressions (e.g. `(IS_NULL mycol)`): a document can get a different result depending on
/// the segment it is in, just because a column with the same name and another type is present.
///
/// A variable that is not a fast field, or has no column of an accepted type, is unbound (`None`).
/// The compiler then treats it as null.
pub(crate) fn find_input_column_handle(
    reader: &SegmentReader,
    name: &str,
    accepted_types: InferredTypeSet,
) -> io::Result<Option<DynamicColumnHandle>> {
    let Ok(column_handles) = reader.fast_fields().dynamic_column_handles(name) else {
        // If the call to dynamic_column_handles fails (for instance because the column is not a
        // fast field) we choose to act as if the column was absent.
        return Ok(None);
    };
    for handle in column_handles {
        let Some(var_type) = var_type_for_column_type(handle.column_type()) else {
            continue;
        };
        if accepted_types.contains(var_type) {
            return Ok(Some(handle));
        }
    }
    Ok(None)
}

/// Returns the jitexpr type of the values of a column, if the column type is supported.
pub(crate) fn var_type_for_column_type(column_type: ColumnType) -> Option<VarType> {
    match column_type {
        ColumnType::Bool => Some(VarType::Bool),
        ColumnType::I64 => Some(VarType::I64),
        ColumnType::U64 => Some(VarType::U64),
        ColumnType::F64 => Some(VarType::F64),
        ColumnType::Str => Some(VarType::Str),
        ColumnType::Bytes | ColumnType::IpAddr | ColumnType::DateTime => None,
    }
}
