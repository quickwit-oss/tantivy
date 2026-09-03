use std::cell::RefCell;
use std::collections::HashMap;

use columnar::{ColumnType, DynamicColumn};
use jitexpr::ast::{infer_types_with_target, InferredTypeSet, TypeError, UntypedExpr};
use jitexpr::compile::{compile, CompiledFnCtx};
use jitexpr::types::{VarType, VariablePrimitiveOpt, VariableValue};

use super::{DocPredicate, SegmentDocPredicate};
use crate::index::SegmentReader;
use crate::{DocId, TantivyError};

/// A [`DocPredicate`] that evaluates a boolean JIT expression against
/// fast-field columns.
///
/// Variable names in the expression are resolved as fast-field names for each
/// segment. Multivalued columns contribute their first value. A document with
/// a missing input value does not match.
#[derive(Clone, Debug)]
pub struct JitExprPredicate {
    expression: UntypedExpr,
    inferred_inputs: Vec<(String, InferredTypeSet)>,
}

impl JitExprPredicate {
    /// Creates a predicate after inferring its inputs and requiring a boolean result.
    pub fn new(expression: UntypedExpr) -> Result<Self, TypeError> {
        let inferred_types = infer_types_with_target(&expression, InferredTypeSet::BOOLEAN)?;
        let mut inferred_inputs: Vec<(String, InferredTypeSet)> = inferred_types
            .into_iter()
            .map(|(name, types)| (name.to_string(), types))
            .collect();
        inferred_inputs.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(Self {
            expression,
            inferred_inputs,
        })
    }

    /// Returns the expression evaluated by this predicate.
    pub fn expression(&self) -> &UntypedExpr {
        &self.expression
    }
}

impl DocPredicate for JitExprPredicate {
    type SegmentDocPredicate = JitExprSegmentPredicate;

    fn doc_predicate(&self, segment_reader: &SegmentReader) -> crate::Result<JitExprSegmentPredicate> {
        let mut variable_types = HashMap::new();
        let mut opened_columns = HashMap::new();

        for (name, accepted_types) in &self.inferred_inputs {
            if let Some(column) = open_input_column(segment_reader, name, *accepted_types)? {
                let var_type = var_type_for_column_type(column.column_type()).ok_or_else(|| {
                    TantivyError::InternalError(format!(
                        "unsupported calculated-predicate column type {}",
                        column.column_type()
                    ))
                })?;
                variable_types.insert(name.as_str(), var_type);
                opened_columns.insert(name.clone(), column);
            }
        }

        let compiled = compile(&self.expression, &variable_types).map_err(|error| {
            TantivyError::InvalidArgument(format!(
                "failed to compile calculated predicate `{}`: {error}",
                self.expression
            ))
        })?;
        if compiled.result_type() != VarType::Bool {
            return Ok(JitExprSegmentPredicate::Never);
        }

        // The compiler owns the definitive ABI order. Reorder the opened columns
        // to match it instead of relying on inference or HashMap iteration order.
        let mut columns = Vec::with_capacity(compiled.inputs.len());
        for input in &compiled.inputs {
            let column = opened_columns
                .remove(input.variable_name.as_ref())
                .ok_or_else(|| {
                    TantivyError::InternalError(format!(
                        "compiled input `{}` has no corresponding fast-field column",
                        input.variable_name
                    ))
                })?;
            let column_type = var_type_for_column_type(column.column_type()).ok_or_else(|| {
                TantivyError::InternalError(format!(
                    "unsupported calculated-predicate column type {}",
                    column.column_type()
                ))
            })?;
            if column_type != input.r#type {
                return Err(TantivyError::InternalError(format!(
                    "compiled input `{}` expects {:?}, but its column has type {:?}",
                    input.variable_name, input.r#type, column_type
                )));
            }
            columns.push(column);
        }

        Ok(JitExprSegmentPredicate::Eval(RefCell::new(
            JitExprEvalState::new(compiled.into(), columns),
        )))
    }
}

fn open_input_column(
    reader: &SegmentReader,
    name: &str,
    accepted_types: InferredTypeSet,
) -> crate::Result<Option<DynamicColumn>> {
    let handles = reader.fast_fields().dynamic_column_handles(name)?;
    for handle in handles {
        let Some(var_type) = var_type_for_column_type(handle.column_type()) else {
            continue;
        };
        if !accepted_types.contains(var_type) {
            continue;
        }
        return Ok(Some(handle.open()?));
    }
    Ok(None)
}

fn var_type_for_column_type(column_type: ColumnType) -> Option<VarType> {
    match column_type {
        ColumnType::Bool => Some(VarType::Bool),
        ColumnType::I64 => Some(VarType::I64),
        ColumnType::U64 => Some(VarType::U64),
        ColumnType::F64 => Some(VarType::F64),
        ColumnType::Str => Some(VarType::Str),
        ColumnType::Bytes | ColumnType::IpAddr | ColumnType::DateTime => None,
    }
}

struct StringInput {
    value: String,
}

impl StringInput {
    fn new() -> Self {
        Self {
            value: String::new(),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RawStringRef {
    data: *const u8,
    len: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
union InputValue {
    primitive: VariablePrimitiveOpt,
    string: RawStringRef,
}

impl Default for InputValue {
    fn default() -> Self {
        Self {
            primitive: VariablePrimitiveOpt::none(),
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<InputValue>() == std::mem::size_of::<VariableValue>());
    assert!(std::mem::align_of::<InputValue>() == std::mem::align_of::<VariableValue>());
};

/// The per-segment state needed to evaluate a compiled expression.
///
/// Held behind a [`RefCell`] because [`SegmentDocPredicate::eval`] takes
/// `&self`, while evaluation needs to mutate the string scratch buffers and
/// the input slot values.
pub struct JitExprEvalState {
    compiled: CompiledFnCtx,
    columns: Vec<DynamicColumn>,
    string_inputs: Vec<StringInput>,
    input_values: Vec<InputValue>,
}

impl JitExprEvalState {
    fn new(compiled: CompiledFnCtx, columns: Vec<DynamicColumn>) -> Self {
        let input_values = vec![InputValue::default(); columns.len()];
        let string_inputs = columns
            .iter()
            .filter(|column| matches!(column, DynamicColumn::Str(_)))
            .map(|_| StringInput::new())
            .collect();
        Self {
            compiled,
            columns,
            string_inputs,
            input_values,
        }
    }

    fn populate_inputs(&mut self, doc: DocId) -> bool {
        let mut string_inputs = self.string_inputs.iter_mut();
        for (column, input_value) in self.columns.iter().zip(&mut self.input_values) {
            match column {
                DynamicColumn::Bool(column) => {
                    let Some(value) = column.first(doc) else {
                        return false;
                    };
                    *input_value = InputValue {
                        primitive: VariablePrimitiveOpt::some(value),
                    };
                }
                DynamicColumn::I64(column) => {
                    let Some(value) = column.first(doc) else {
                        return false;
                    };
                    *input_value = InputValue {
                        primitive: VariablePrimitiveOpt::some(value),
                    };
                }
                DynamicColumn::U64(column) => {
                    let Some(value) = column.first(doc) else {
                        return false;
                    };
                    *input_value = InputValue {
                        primitive: VariablePrimitiveOpt::some(value),
                    };
                }
                DynamicColumn::F64(column) => {
                    let Some(value) = column.first(doc) else {
                        return false;
                    };
                    *input_value = InputValue {
                        primitive: VariablePrimitiveOpt::some(value),
                    };
                }
                DynamicColumn::Str(column) => {
                    let Some(string_input) = string_inputs.next() else {
                        unreachable!("every string column has a string input buffer");
                    };
                    let Some(term_ord) = column.ords().first(doc) else {
                        return false;
                    };
                    string_input.value.clear();
                    let found = column
                        .ord_to_str(term_ord, &mut string_input.value)
                        .expect("a fast-field string dictionary became unreadable after opening");
                    if !found {
                        return false;
                    }
                    *input_value = InputValue {
                        string: RawStringRef {
                            data: string_input.value.as_ptr(),
                            len: string_input.value.len(),
                        },
                    };
                }
                DynamicColumn::Bytes(_) | DynamicColumn::IpAddr(_) | DynamicColumn::DateTime(_) => {
                    unreachable!("unsupported columns are filtered before compilation")
                }
            }
        }
        true
    }

    fn eval(&mut self, doc: DocId) -> bool {
        if !self.populate_inputs(doc) {
            return false;
        }
        // SAFETY: `InputValue` has the same layout as `VariableValue`. The slots
        // follow `compiled.inputs`, primitive values use the expected union arm,
        // and string pointers remain valid because `string_inputs` is not
        // mutated between `populate_inputs` and this call.
        let input_values = unsafe {
            std::slice::from_raw_parts(
                self.input_values.as_ptr().cast::<VariableValue>(),
                self.input_values.len(),
            )
        };
        // SAFETY: The slice above satisfies `CompiledFn::call`'s ABI contract.
        let result = unsafe { self.compiled.call(input_values) };
        // SAFETY: Construction requires a boolean result type.
        (unsafe { result.as_bool() }) == Some(true)
    }
}

/// The [`SegmentDocPredicate`] produced by [`JitExprPredicate`] for one segment.
pub enum JitExprSegmentPredicate {
    /// The expression never resolves to a boolean for this segment; no
    /// document matches.
    Never,
    /// The expression resolves to a boolean; holds the per-segment state
    /// needed to evaluate it for each document.
    Eval(RefCell<JitExprEvalState>),
}

// SAFETY: A `JitExprSegmentPredicate` is only ever evaluated from the single
// thread driving the `DocSet` that owns it, so the `RefCell` is never
// borrowed concurrently even though it is not itself `Sync`. String pointers
// stored in `JitExprEvalState::input_values` point into buffers owned by
// `string_inputs` in the same struct, whose allocation stays stable across
// moves, so moving or sending the whole struct across threads is sound.
unsafe impl Send for JitExprSegmentPredicate {}
unsafe impl Sync for JitExprSegmentPredicate {}

impl SegmentDocPredicate for JitExprSegmentPredicate {
    fn eval(&self, doc_id: DocId) -> bool {
        match self {
            JitExprSegmentPredicate::Never => false,
            JitExprSegmentPredicate::Eval(state) => state.borrow_mut().eval(doc_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::Count;
    use crate::query::doc_predicate_query::DocPredicateQuery;
    use crate::query::{EnableScoring, Query};
    use crate::schema::{Schema, FAST, STRING};
    use crate::Index;

    fn create_index() -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let number = schema_builder.add_u64_field("number", FAST);
        let flag = schema_builder.add_bool_field("flag", FAST);
        let label = schema_builder.add_text_field("label", STRING | FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!(number => 1u64, flag => true, label => "one"))?;
        writer.add_document(doc!(number => 2u64, flag => false, label => "two"))?;
        writer.add_document(doc!(number => 3u64, flag => true, label => "three"))?;
        writer.add_document(doc!(number => 4u64))?;
        writer.commit()?;
        Ok(index)
    }

    fn query(expression: &str) -> DocPredicateQuery {
        JitExprPredicate::new(jitexpr::ast::deserialize(expression).unwrap())
            .unwrap()
            .into()
    }

    #[test]
    fn test_constructor_requires_boolean_expression() {
        let expression = jitexpr::ast::deserialize("(ADD number 1u64)").unwrap();

        assert!(JitExprPredicate::new(expression).is_err());
    }

    #[test]
    fn test_numeric_predicate_query() -> crate::Result<()> {
        let index = create_index()?;
        let searcher = index.reader()?.searcher();

        assert_eq!(searcher.search(&query("(EQ number 2i64)"), &Count)?, 1);
        Ok(())
    }

    #[test]
    fn test_boolean_and_string_inputs() -> crate::Result<()> {
        let index = create_index()?;
        let searcher = index.reader()?.searcher();

        assert_eq!(searcher.search(&query("flag"), &Count)?, 2);
        assert_eq!(searcher.search(&query(r#"(EQ label "three")"#), &Count)?, 1);
        // Inference stores names alphabetically, while the compiled ABI follows
        // expression order (`label`, then `flag`) here.
        assert_eq!(
            searcher.search(&query(r#"(EQ (EQ label "three") flag)"#), &Count)?,
            2
        );
        Ok(())
    }

    #[test]
    fn test_constant_true_matches_every_document() -> crate::Result<()> {
        let index = create_index()?;
        let searcher = index.reader()?.searcher();

        assert_eq!(searcher.search(&query("true"), &Count)?, 4);
        Ok(())
    }

    #[test]
    fn test_missing_boolean_column_returns_empty_scorer() -> crate::Result<()> {
        let index = create_index()?;
        let searcher = index.reader()?.searcher();

        assert_eq!(searcher.search(&query("missing"), &Count)?, 0);
        Ok(())
    }

    #[test]
    fn test_incompatible_column_type_is_treated_as_missing() -> crate::Result<()> {
        let index = create_index()?;
        let searcher = index.reader()?.searcher();
        let calculated = query("(EQ (ADD label 1i64) 2i64)");

        assert_eq!(searcher.search(&calculated, &Count)?, 0);
        Ok(())
    }
}
