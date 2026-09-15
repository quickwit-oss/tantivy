use std::collections::HashMap;
use std::io;

use columnar::{ColumnType, DynamicColumn, StrColumn};
use jitexpr::ast::{infer_types_with_target, InferredTypeSet, TypeError, UntypedExpr};
use jitexpr::compile::{compile, CompiledFnCtx};
use jitexpr::types::{VarType, VariableValue};

use super::{DocPredicate, SegmentDocPredicate};
use crate::index::SegmentReader;
use crate::{DocId, TantivyError};

/// A [`DocPredicate`] that evaluates a boolean JIT expression against fast fields.
///
/// Requires the `jitexpr` feature. Variable names are resolved as fast-field names
/// for each segment, supporting boolean, numeric, and string columns. Missing or
/// incompatible columns are left unbound, so the compiler treats them as `None`.
/// For bound columns, multivalued documents contribute their first value, and a
/// document missing any input is skipped before evaluating the expression.
/// Only a present `true` result matches.
///
/// ```
/// use tantivy::jitexpr::ast::deserialize;
/// use tantivy::query::doc_predicate_query::{DocPredicateQuery, JitExprPredicate};
///
/// let expression = deserialize("(EQ (ADD price 1u64) 10u64)").unwrap();
/// let query: DocPredicateQuery = JitExprPredicate::new(expression).unwrap().into();
/// ```
#[derive(Clone, Debug)]
pub struct JitExprPredicate {
    expression: UntypedExpr,
    inferred_inputs: Vec<(String, InferredTypeSet)>,
}

impl JitExprPredicate {
    /// Creates a predicate after inferring its inputs and requiring a boolean result.
    pub fn new(expression: UntypedExpr) -> Result<Self, TypeError> {
        let inferred_types: HashMap<&str, InferredTypeSet> =
            infer_types_with_target(&expression, InferredTypeSet::BOOLEAN)?;
        let inferred_inputs: Vec<(String, InferredTypeSet)> = inferred_types
            .into_iter()
            .map(|(name, types)| (name.to_string(), types))
            .collect();
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
    type SegmentDocPredicate = Option<JitExprEvalState>;

    fn doc_predicate(
        &self,
        segment_reader: &SegmentReader,
    ) -> crate::Result<Option<JitExprEvalState>> {
        let mut variable_types = HashMap::with_capacity(self.inferred_inputs.len());
        let mut opened_columns: HashMap<&str, DynamicColumn> =
            HashMap::with_capacity(self.inferred_inputs.len());

        // We pick a single column for each variable name. NOTE this CAN yield to unexpected results
        // for some expression (e.g. (IS_NULL "mycol")).
        // For instance, a document could be matching in one segment, and not matching if it
        // was in another segment, just because the presence of column with the same name
        // and different type could interfere.
        for (name, accepted_types) in &self.inferred_inputs {
            let Some(column) = open_input_column(segment_reader, name, *accepted_types)? else {
                // If we do not have a valid column for that expression, we do not
                // fill the HashMap at all.
                //
                // The compiler will replace the expression and make it behave like the null
                // literal.
                continue;
            };
            let Some(var_type) = var_type_for_column_type(column.column_type()) else {
                continue;
            };
            variable_types.insert(name.as_str(), var_type);
            opened_columns.insert(name.as_str(), column);
        }

        let compiled_fn =
            compile(&self.expression, &variable_types).map_err(|compilation_err| {
                TantivyError::InvalidArgument(format!(
                    "the expression compilation failed {:?}. error: {compilation_err}",
                    self.expression
                ))
            })?;

        // We ended up with an expression that could not resolve to anything apparently.
        if compiled_fn.result_type() == VarType::None {
            return Ok(None);
        }

        if compiled_fn.result_type() != VarType::Bool {
            // This should never happen: we passed a target inferred type of Bool,
            // so we should have either Bool or None.
            return Err(TantivyError::InvalidArgument(format!(
                "the expression is not a predicate {}",
                self.expression
            )));
        }

        // The compiler owns the definitive ABI order. Do not rely on inference
        // or HashMap iteration order when building the argument slots.
        let mut columns = Vec::with_capacity(compiled_fn.inputs().len());
        for input in compiled_fn.inputs() {
            let column = opened_columns
                .remove(input.variable_name.as_ref())
                .ok_or_else(|| {
                    TantivyError::InternalError(format!(
                        "compiled input `{}` has no corresponding fast-field column",
                        input.variable_name
                    ))
                })?;
            if var_type_for_column_type(column.column_type()) != Some(input.r#type) {
                return Err(TantivyError::InternalError(format!(
                    "compiled input `{}` expects {:?}, but its column has type {}",
                    input.variable_name,
                    input.r#type,
                    column.column_type()
                )));
            }
            columns.push(column);
        }
        // There is one reusable buffer per string column, in ABI order.
        let num_string_inputs = columns
            .iter()
            .filter(|column| matches!(column, DynamicColumn::Str(_)))
            .count();
        let num_inputs = columns.len();
        Ok(Some(JitExprEvalState {
            compiled: compiled_fn.context(),
            columns,
            string_inputs: vec![String::new(); num_string_inputs],
            input_values: Vec::with_capacity(num_inputs),
        }))
    }
}

fn open_input_column(
    reader: &SegmentReader,
    name: &str,
    accepted_types: InferredTypeSet,
) -> io::Result<Option<DynamicColumn>> {
    let Ok(column_handles) = reader.fast_fields().dynamic_column_handles(name) else {
        // If the call to dynamic_column_handles fails (for instance because the column is not a
        // fast field) we choose to act as if the column was absent.
        return Ok(None);
    };
    for handle in column_handles {
        // We return the first column that could be accepted
        let Some(var_type) = var_type_for_column_type(handle.column_type()) else {
            continue;
        };
        if accepted_types.contains(var_type) {
            return Ok(Some(handle.open()?));
        }
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

/// The [`SegmentDocPredicate`] produced by [`JitExprPredicate`] for one segment.
pub struct JitExprEvalState {
    compiled: CompiledFnCtx,
    columns: Vec<DynamicColumn>,
    // One reusable buffer per string column.
    string_inputs: Vec<String>,
    // Reusable argument slots.
    //
    // Hidden contract: this vector is always empty between evaluations, so the
    // `'static` lifetime is a placeholder for an unused element type rather
    // than a claim about any stored string. Only its capacity carries over,
    // which is what makes restoring the `'static` type after an evaluation
    // sound. `eval` is responsible for upholding this on every return path.
    input_values: Vec<VariableValue<'static>>,
}

/// A wrapper to make sure the variable value buffer is cleared even if the evaluation
/// panicked.
struct ClearOnDrop<'a>(&'a mut Vec<VariableValue<'a>>);

impl<'a> ClearOnDrop<'a> {
    fn wrap(input_values: &'a mut Vec<VariableValue<'static>>) -> Self {
        debug_assert!(input_values.is_empty());
        // Input_values is just a buffer we share to avoid allocations
        let lower_lifetime_input_values: &mut Vec<VariableValue<'_>> =
            unsafe { std::mem::transmute(input_values) };
        ClearOnDrop(lower_lifetime_input_values)
    }
}

impl<'a> Drop for ClearOnDrop<'a> {
    fn drop(&mut self) {
        self.0.clear();
    }
}

impl SegmentDocPredicate for JitExprEvalState {
    fn eval(&mut self, doc_id: DocId) -> bool {
        // Input_values is just a buffer we share to avoid allocations
        let mut inputs_vec = ClearOnDrop::wrap(&mut self.input_values);

        fill_input_values(
            &self.columns,
            &mut self.string_inputs,
            &mut inputs_vec.0,
            doc_id,
        );

        // SAFETY: Columns follow compiled.inputs() and their types were checked
        // during setup. Each slot uses the matching union arm. String buffers
        // remain borrowed, and cannot be mutated, until this call finishes.
        let eval_result: Option<bool> = unsafe { self.compiled.call(&inputs_vec.0).as_bool() };

        eval_result == Some(true)
    }
}

fn fill_input_values<'buffer>(
    columns: &[DynamicColumn],
    string_inputs: &'buffer mut [String],
    input_values: &mut Vec<VariableValue<'buffer>>,
    doc_id: DocId,
) {
    debug_assert!(input_values.is_empty());
    let mut string_inputs = string_inputs.iter_mut();
    for column in columns {
        let input: Option<VariableValue> = match column {
            DynamicColumn::Bool(column) => column.first(doc_id).map(VariableValue::from),
            DynamicColumn::I64(column) => column.first(doc_id).map(VariableValue::from),
            DynamicColumn::U64(column) => column.first(doc_id).map(VariableValue::from),
            DynamicColumn::F64(column) => column.first(doc_id).map(VariableValue::from),
            DynamicColumn::Str(column) => {
                let string_input = string_inputs
                    .next()
                    .expect("every string column has a string input buffer");
                load_str_input(column, doc_id, string_input).map(VariableValue::from)
            }
            DynamicColumn::Bytes(_) | DynamicColumn::IpAddr(_) | DynamicColumn::DateTime(_) => {
                unreachable!("unsupported columns are filtered before compilation")
            }
        };
        // If the value is someone absent, we set the input to none/null.
        input_values.push(input.unwrap_or(VariableValue::none()));
    }
}

/// Loads the first value of a string column for `doc_id` into `buffer`.
///
/// Returns `None` when the document has no value, which skips the document
/// before evaluation. `buffer` is cleared first, so its previous contents are
/// discarded whether or not a value is found.
///
/// This function may panic if the dictionary is corrupted or if the column
/// contains term ords that do not exist in the dictionary.
fn load_str_input<'buffer>(
    column: &StrColumn,
    doc_id: DocId,
    buffer: &'buffer mut String,
) -> Option<&'buffer str> {
    buffer.clear();
    let term_ord = column.ords().first(doc_id)?;
    // SegmentDocPredicate::eval cannot return I/O errors; an unreadable
    // dictionary therefore panics.
    // TODO this is terribly inefficient: we need at least some caching.
    let found = column
        .ord_to_str(term_ord, buffer)
        .expect("fast-field string dictionary is corrupted");
    assert!(found);
    Some(buffer.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::Count;
    use crate::query::doc_predicate_query::DocPredicateQuery;
    use crate::schema::{Schema, FAST, STORED, STRING};
    use crate::Index;

    fn create_index() -> Index {
        let mut schema_builder = Schema::builder();
        let number = schema_builder.add_u64_field("number", FAST);
        let flag = schema_builder.add_bool_field("flag", FAST);
        let _notfast = schema_builder.add_bool_field("notfast", STORED);
        let label = schema_builder.add_text_field("label", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests().unwrap();
        writer
            .add_document(doc!(number => 1u64, flag => true, label => "one"))
            .unwrap();
        writer
            .add_document(doc!(number => 2u64, flag => false, label => "two"))
            .unwrap();
        writer
            .add_document(doc!(number => 3u64, flag => true, label => "three"))
            .unwrap();
        writer.add_document(doc!(number => 4u64)).unwrap();
        writer.commit().unwrap();
        index
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
    fn test_simple() {
        let index = create_index();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(
            searcher.search(&query("(EQ number 2i64)"), &Count).unwrap(),
            1
        );
        assert_eq!(
            searcher
                .search(&query("(EQ (ADD number 1u64) 3u64)"), &Count)
                .unwrap(),
            1
        );
    }

    #[test]
    fn test_simple_string_ref_predicate() {
        let index = create_index();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(
            searcher
                .search(
                    &query(r#"(EQ (REGEXP_EXTRACT label "(.).*" 1u64) "o")"#),
                    &Count
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn test_simple_built_string_predicate() {
        let index = create_index();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(
            searcher
                .search(&query(r#"(EQ (UPPER label) "TWO")"#), &Count)
                .unwrap(),
            1
        );
    }

    #[test]
    fn test_simple_missing_field() {
        let index = create_index();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(
            searcher
                .search(&query(r#"(EQ missing_field true)"#), &Count)
                .unwrap(),
            0
        );
    }
    #[test]
    fn test_simple_notfast() {
        let index = create_index();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(
            searcher
                .search(&query(r#"(EQ notfast true)"#), &Count)
                .unwrap(),
            0
        );
    }

    #[test]
    fn test_boolean_and_string_inputs_follow_compiled_order() {
        let index = create_index();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(searcher.search(&query("flag"), &Count).unwrap(), 2);
        assert_eq!(
            searcher
                .search(&query(r#"(EQ label "three")"#), &Count)
                .unwrap(),
            1
        );
        // Inference sorts names, but the ABI follows expression order: label, flag.
        assert_eq!(
            searcher
                .search(&query(r#"(EQ (EQ label "three") flag)"#), &Count)
                .unwrap(),
            2
        );
    }

    #[test]
    fn test_constant_predicates() {
        let index = create_index();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(searcher.search(&query("true"), &Count).unwrap(), 4);
        assert_eq!(searcher.search(&query("false"), &Count).unwrap(), 0);
    }

    #[test]
    fn test_missing_and_incompatible_columns() {
        let index = create_index();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(searcher.search(&query("missing"), &Count).unwrap(), 0);

        // A column missing from the segment is compiled as None.
        assert_eq!(
            searcher
                .search(&query("(IS_NULL missing)"), &Count)
                .unwrap(),
            4
        );
        // A column missing from the segment is compiled as None.
        assert_eq!(
            searcher
                .search(&query("(IS_NOT_NULL missing)"), &Count)
                .unwrap(),
            0
        );
        assert_eq!(
            searcher.search(&query("(IS_NULL flag)"), &Count).unwrap(),
            1
        );
        assert_eq!(
            searcher
                .search(&query("(IS_NOT_NULL flag)"), &Count)
                .unwrap(),
            3
        );
        assert_eq!(
            searcher
                .search(&query("(EQ (ADD label 1i64) 2i64)"), &Count)
                .unwrap(),
            0
        );
        assert_eq!(
            searcher
                .search(&query("(IS_NULL (ADD label 1i64))"), &Count)
                .unwrap(),
            4
        );
    }

    #[test]
    fn test_signed_and_float_columns() {
        let mut schema_builder = Schema::builder();
        let signed = schema_builder.add_i64_field("signed", FAST);
        let float = schema_builder.add_f64_field("float", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests().unwrap();
        writer
            .add_document(doc!(signed => -2i64, float => 1.5f64))
            .unwrap();
        writer
            .add_document(doc!(signed => 3i64, float => 2.5f64))
            .unwrap();
        writer.add_document(doc!()).unwrap();
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(
            searcher
                .search(&query("(EQ signed -2i64)"), &Count)
                .unwrap(),
            1
        );
        assert_eq!(
            searcher
                .search(&query("(EQ signed -2f64)"), &Count)
                .unwrap(),
            1
        );
        assert_eq!(
            searcher
                .search(&query("(EQ float 1.5f64)"), &Count)
                .unwrap(),
            1
        );
        assert_eq!(
            searcher.search(&query("(EQ float 1i64)"), &Count).unwrap(),
            0
        );
    }

    #[test]
    fn test_multivalued_columns_use_first_value() {
        let mut schema_builder = Schema::builder();
        let number = schema_builder.add_u64_field("number", FAST);
        let label = schema_builder.add_text_field("label", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests().unwrap();
        writer
            .add_document(doc!(number => 1u64, number => 2u64,
                                 label => "first", label => "second"))
            .unwrap();
        writer
            .add_document(doc!(number => 2u64, number => 1u64,
                                 label => "second", label => "first"))
            .unwrap();
        writer.add_document(doc!()).unwrap();
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(
            searcher.search(&query("(EQ number 1u64)"), &Count).unwrap(),
            1
        );
        assert_eq!(
            searcher
                .search(&query(r#"(EQ label "first")"#), &Count)
                .unwrap(),
            1
        );
    }

    // THIS FAILS! due to our pick best possible column approach policy.
    // #[test]
    // fn test_multi_typed_field_picks_one() {
    //     let mut schema_builder = Schema::builder();
    //     let json = schema_builder.add_json_field("json", FAST);
    //     let index = Index::create_in_ram(schema_builder.build());
    //     let mut writer = index.writer_for_tests().unwrap();
    //     writer
    //         .add_document(doc!(json => serde_json::json!({"myfield": 2u64})))
    //         .unwrap();
    //     writer
    //         .add_document(doc!(json => serde_json::json!({"myfield": "b"})))
    //         .unwrap();
    //     writer.commit().unwrap();
    //     let searcher = index.reader().unwrap().searcher();
    //     assert_eq!(
    //         searcher
    //             .search(&query(r#"(IS_NULL json.myfield)"#), &Count)
    //             .unwrap(),
    //         2 // assertion fails, expected 2 got 1
    //     );
    // }

    // THIS FAILS DUE TO EQ infer_types being too lenient.
    // #[test]
    // fn test_multi_typed_field_eq_too_lenient_failing() {
    //     let mut schema_builder = Schema::builder();
    //     let json = schema_builder.add_json_field("json", FAST);
    //     let index = Index::create_in_ram(schema_builder.build());
    //     let mut writer = index.writer_for_tests().unwrap();
    //     writer
    //         .add_document(doc!(json => serde_json::json!({"myfield": 2u64})))
    //         .unwrap();
    //     writer
    //         .add_document(doc!(json => serde_json::json!({"myfield": "b"})))
    //         .unwrap();
    //     writer.commit().unwrap();
    //     let searcher = index.reader().unwrap().searcher();
    //     assert_eq!(
    //         searcher
    //             .search(&query(r#"(EQ json.myfield "b")"#), &Count)
    //             .unwrap(),
    //         1 // assertion fails, expected 2 got 1
    //     );
    // }
}
