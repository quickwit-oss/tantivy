//! The fast-field columns bound to the inputs of a compiled jitexpr expression.

use std::ops::Range;

use columnar::{
    Column, ColumnIndex, DynamicColumn, DynamicColumnHandle, MonotonicallyMappableToU64,
    PlainStrColumn, PlainValueReader,
};
use jitexpr::types::{VarType, VariableValue};

use super::term_resolver::TermResolver;
use super::var_type_for_column_type;
use crate::{DocId, TantivyError, COLLECT_BLOCK_BUFFER_LEN};

/// A fast-field column bound to an input of the compiled expression.
pub(super) struct InputColumn {
    var_type: VarType,
    values: InputValues,
}

enum InputValues {
    /// Values in their monotonic `u64` mapping. Term ords for a dictionary encoded `Str` input.
    Ordinals {
        column: Column<u64>,
        /// `first_values[i]` is the first value of `docs[i]`, for the docs of the last block.
        first_values: Vec<Option<u64>>,
        /// The terms of `first_values`. Not `TermResolver::None` if and only if `var_type` is
        /// `Str`.
        term_resolver: TermResolver,
    },
    /// A plain-encoded `Str` input: the values are decoded from the column.
    PlainStr(PlainStrInput),
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
            let values = match handle.open()? {
                DynamicColumn::Str(str_column) => {
                    let (term_dictionary, term_ords) = str_column.into_parts();
                    InputValues::Ordinals {
                        column: term_ords,
                        first_values: Vec::with_capacity(COLLECT_BLOCK_BUFFER_LEN),
                        term_resolver: TermResolver::for_dictionary(term_dictionary)?,
                    }
                }
                DynamicColumn::PlainStr(plain_str_column) => {
                    InputValues::PlainStr(PlainStrInput::new(&plain_str_column))
                }
                _ => {
                    return Err(TantivyError::InternalError(
                        "a text column could not be opened as a text column".to_string(),
                    ));
                }
            };
            return Ok(InputColumn { var_type, values });
        }
        let column = handle.open_u64_lenient()?.ok_or_else(|| {
            TantivyError::InternalError(format!(
                "a column of type {column_type} could not be opened as u64"
            ))
        })?;
        Ok(InputColumn {
            var_type,
            values: InputValues::Ordinals {
                column,
                first_values: Vec::with_capacity(COLLECT_BLOCK_BUFFER_LEN),
                term_resolver: TermResolver::None,
            },
        })
    }

    pub(super) fn var_type(&self) -> VarType {
        self.var_type
    }

    /// Loads the first value of each doc of `docs`.
    ///
    /// Panics if the dictionary of a text column is corrupted, or if a plain column cannot be
    /// read.
    pub(super) fn load_block(&mut self, docs: &[DocId]) {
        match &mut self.values {
            InputValues::Ordinals {
                column,
                first_values,
                term_resolver,
            } => {
                // `first_vals` does not write the entries of the docs without a value: we reset
                // them.
                first_values.clear();
                first_values.resize(docs.len(), None);
                column.first_vals(docs, first_values);
                term_resolver.load(first_values);
            }
            InputValues::PlainStr(plain_str_input) => plain_str_input.load_block(docs),
        }
    }

    /// Returns the value of `docs[doc_pos]`, for the `docs` of the last `load_block` call.
    #[inline]
    pub(super) fn value(&self, doc_pos: usize) -> VariableValue<'_> {
        let (first_values, term_resolver) = match &self.values {
            InputValues::Ordinals {
                first_values,
                term_resolver,
                ..
            } => (first_values, term_resolver),
            InputValues::PlainStr(plain_str_input) => {
                return plain_str_input
                    .value(doc_pos)
                    .map(VariableValue::from)
                    .unwrap_or(VariableValue::none());
            }
        };
        let Some(value) = first_values[doc_pos] else {
            return VariableValue::none();
        };
        match self.var_type {
            VarType::Bool => VariableValue::from(bool::from_u64(value)),
            VarType::I64 => VariableValue::from(i64::from_u64(value)),
            VarType::U64 => VariableValue::from(value),
            VarType::F64 => VariableValue::from(f64::from_u64(value)),
            VarType::Str => VariableValue::from(term_resolver.term(doc_pos, value)),
            VarType::None => VariableValue::none(),
        }
    }

    /// Returns the value of `doc`, without going through `load_block`.
    ///
    /// Invalidates the last `load_block` call: `value` must not be called before the next
    /// `load_block`.
    ///
    /// Panics if the dictionary of a text column is corrupted, or if a plain column cannot be
    /// read.
    #[inline]
    pub(super) fn value_for_doc(&mut self, doc: DocId) -> VariableValue<'_> {
        let (column, term_resolver) = match &mut self.values {
            InputValues::Ordinals {
                column,
                term_resolver,
                ..
            } => (column, term_resolver),
            InputValues::PlainStr(plain_str_input) => {
                return plain_str_input
                    .value_for_doc(doc)
                    .map(VariableValue::from)
                    .unwrap_or(VariableValue::none());
            }
        };
        let Some(value) = column.first(doc) else {
            return VariableValue::none();
        };
        match self.var_type {
            VarType::Bool => VariableValue::from(bool::from_u64(value)),
            VarType::I64 => VariableValue::from(i64::from_u64(value)),
            VarType::U64 => VariableValue::from(value),
            VarType::F64 => VariableValue::from(f64::from_u64(value)),
            VarType::Str => VariableValue::from(term_resolver.term_for_ord(value)),
            VarType::None => VariableValue::none(),
        }
    }

    /// Returns the index of the column: which docs have a value.
    pub(super) fn column_index(&self) -> &ColumnIndex {
        match &self.values {
            InputValues::Ordinals { column, .. } => &column.index,
            InputValues::PlainStr(plain_str_input) => {
                plain_str_input.reader.column().column_index()
            }
        }
    }
}

/// Decodes the first value of docs from a plain-encoded `Str` column.
struct PlainStrInput {
    reader: PlainValueReader,
    /// The first values of the docs of the last block, concatenated.
    block_values: String,
    /// `block_value_ranges[doc_pos]` is the range of the value of `docs[doc_pos]` in
    /// `block_values`, for the docs of the last block. `None` if the doc has no value.
    block_value_ranges: Vec<Option<Range<usize>>>,
    /// The value decoded by the last `value_for_doc` call.
    single_value: String,
}

impl PlainStrInput {
    fn new(plain_str_column: &PlainStrColumn) -> PlainStrInput {
        PlainStrInput {
            reader: plain_str_column.reader(),
            block_values: String::new(),
            block_value_ranges: Vec::with_capacity(COLLECT_BLOCK_BUFFER_LEN),
            single_value: String::new(),
        }
    }

    /// Appends the first value of `doc` to `output`. Returns false if `doc` has no value.
    ///
    /// Panics if the column cannot be read: `load_block` cannot return an error.
    fn append_first_value(reader: &mut PlainValueReader, doc: DocId, output: &mut String) -> bool {
        let value_range = reader.column().column_index().value_row_ids(doc);
        if value_range.is_empty() {
            return false;
        }
        reader
            .append_str_value(value_range.start, output)
            .expect("plain-encoded fast field is corrupted");
        true
    }

    fn load_block(&mut self, docs: &[DocId]) {
        self.block_values.clear();
        self.block_value_ranges.clear();
        // Docs are sorted: the blocks of the column are read in order.
        for &doc in docs {
            let start = self.block_values.len();
            if Self::append_first_value(&mut self.reader, doc, &mut self.block_values) {
                self.block_value_ranges
                    .push(Some(start..self.block_values.len()));
            } else {
                self.block_value_ranges.push(None);
            }
        }
    }

    #[inline]
    fn value(&self, doc_pos: usize) -> Option<&str> {
        let value_range = self.block_value_ranges[doc_pos].clone()?;
        Some(&self.block_values[value_range])
    }

    fn value_for_doc(&mut self, doc: DocId) -> Option<&str> {
        self.single_value.clear();
        if !Self::append_first_value(&mut self.reader, doc, &mut self.single_value) {
            return None;
        }
        Some(self.single_value.as_str())
    }
}
