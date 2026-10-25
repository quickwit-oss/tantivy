//! The fast-field columns bound to the inputs of a compiled jitexpr expression.

use columnar::{
    Column, ColumnIndex, DynamicColumn, DynamicColumnHandle, MonotonicallyMappableToU64,
};
use jitexpr::types::{VarType, VariableValue};

use super::term_resolver::TermResolver;
use super::var_type_for_column_type;
use crate::{DocId, TantivyError, COLLECT_BLOCK_BUFFER_LEN};

/// A fast-field column bound to an input of the compiled expression.
pub(super) struct InputColumn {
    var_type: VarType,
    /// Values in their monotonic `u64` mapping. Term ords for a `Str` input.
    column: Column<u64>,
    /// `first_values[i]` is the first value of `docs[i]`, for the docs of the last block.
    first_values: Vec<Option<u64>>,
    /// The terms of `first_values`. Not `TermResolver::None` if and only if `var_type` is `Str`.
    term_resolver: TermResolver,
}

impl InputColumn {
    /// Opens the column of `handle`. The type of the input is given by the column type.
    pub(super) fn open(handle: &DynamicColumnHandle) -> crate::Result<InputColumn> {
        let column_type = handle.column_type();
        let Some(var_type) = var_type_for_column_type(column_type) else {
            return Err(TantivyError::InternalError(format!(
                "a column of type {column_type} cannot be bound to an input"
            )));
        };
        if var_type == VarType::Str {
            let DynamicColumn::Str(str_column) = handle.open()? else {
                return Err(TantivyError::InternalError(
                    "a text column could not be opened as a text column".to_string(),
                ));
            };
            let (term_dictionary, term_ords) = str_column.into_parts();
            Ok(InputColumn {
                var_type,
                column: term_ords,
                first_values: Vec::with_capacity(COLLECT_BLOCK_BUFFER_LEN),
                term_resolver: TermResolver::for_dictionary(term_dictionary)?,
            })
        } else {
            let column = handle.open_u64_lenient()?.ok_or_else(|| {
                TantivyError::InternalError(format!(
                    "a column of type {column_type} could not be opened as u64"
                ))
            })?;
            Ok(InputColumn {
                var_type,
                column,
                first_values: Vec::with_capacity(COLLECT_BLOCK_BUFFER_LEN),
                term_resolver: TermResolver::None,
            })
        }
    }

    pub(super) fn var_type(&self) -> VarType {
        self.var_type
    }

    /// Loads the first value of each doc of `docs`.
    ///
    /// Panics if the dictionary of a text column is corrupted.
    pub(super) fn load_block(&mut self, docs: &[DocId]) {
        // `first_vals` does not write the entries of the docs without a value: we reset them.
        self.first_values.clear();
        self.first_values.resize(docs.len(), None);
        self.column.first_vals(docs, &mut self.first_values);
        self.term_resolver.load(&self.first_values);
    }

    /// Returns the value of `docs[doc_pos]`, for the `docs` of the last `load_block` call.
    #[inline]
    pub(super) fn value(&self, doc_pos: usize) -> VariableValue<'_> {
        let Some(value) = self.first_values[doc_pos] else {
            return VariableValue::none();
        };
        match self.var_type {
            VarType::Bool => VariableValue::from(bool::from_u64(value)),
            VarType::I64 => VariableValue::from(i64::from_u64(value)),
            VarType::U64 => VariableValue::from(value),
            VarType::F64 => VariableValue::from(f64::from_u64(value)),
            VarType::Str => VariableValue::from(self.term_resolver.term(doc_pos, value)),
            VarType::None => VariableValue::none(),
        }
    }

    /// Returns the value of `doc`, without going through `load_block`.
    ///
    /// Invalidates the last `load_block` call: `value` must not be called before the next
    /// `load_block`.
    ///
    /// Panics if the dictionary of a text column is corrupted.
    #[inline]
    pub(super) fn value_for_doc(&mut self, doc: DocId) -> VariableValue<'_> {
        let Some(value) = self.column.first(doc) else {
            return VariableValue::none();
        };
        match self.var_type {
            VarType::Bool => VariableValue::from(bool::from_u64(value)),
            VarType::I64 => VariableValue::from(i64::from_u64(value)),
            VarType::U64 => VariableValue::from(value),
            VarType::F64 => VariableValue::from(f64::from_u64(value)),
            VarType::Str => VariableValue::from(self.term_resolver.term_for_ord(value)),
            VarType::None => VariableValue::none(),
        }
    }

    /// Returns the index of the column: which docs have a value.
    pub(super) fn column_index(&self) -> &ColumnIndex {
        &self.column.index
    }
}
