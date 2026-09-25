use std::collections::HashMap;
use std::io;

use columnar::{ColumnIndex, ColumnType, DynamicColumn, StrColumn};
use jitexpr::ast::{
    infer_types_with_target, required_presence_for_true, InferredTypeSet, TypeError, UntypedExpr,
    VariablePresenceCondition,
};
use jitexpr::compile::{CompiledFnCtx, StringArena};
use jitexpr::types::{VarType, VariableValue};

use super::{DocPredicate, SegmentDocPredicate};
use crate::index::SegmentReader;
use crate::query::doc_predicate_query::ConstOrVariableSegmentPredicate;
use crate::query::exist_query::{ExistsColumnIndex, ExistsDocSet};
use crate::query::union::SimpleUnion;
use crate::query::Intersection;
use crate::{DocId, DocSet, TantivyError};

/// A [`DocPredicate`] that evaluates a boolean JIT expression against fast fields.
///
/// Requires the `jitexpr` feature. Variable names are resolved as fast-field names
/// for each segment, supporting boolean, numeric, and string columns. Missing or
/// incompatible columns are left unbound, so the compiler treats them as `None`.
///
/// For bound columns, multivalued documents contribute their first value, and a
/// document missing any input will still be evaluated with null in place of the input.
///
/// We "fast path" cases where we detect the expression will always evaluate to true or false
/// on a segment. (e.g. if all variable columns are missing).
///
/// Only a present `true` result matches.
///
/// Documents missing the fields required for the expression to be `true` are skipped without
/// being evaluated. For instance, `(EQ (ADD price 1u64) 10u64)` is only evaluated on the
/// documents having a `price` value.
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
    // A necessary condition, on the presence of the variables, for the expression to be `true`.
    required_presence: VariablePresenceCondition,
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
        let required_presence = required_presence_for_true(&expression);
        Ok(Self {
            expression,
            inferred_inputs,
            required_presence,
        })
    }

    /// Returns the expression evaluated by this predicate.
    pub fn expression(&self) -> &UntypedExpr {
        &self.expression
    }
}

impl DocPredicate for JitExprPredicate {
    type SegmentDocPredicate = JitExprEvalState;

    fn doc_predicate(
        &self,
        segment_reader: &SegmentReader,
    ) -> crate::Result<ConstOrVariableSegmentPredicate<JitExprEvalState>> {
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

        // The variables are bound to the columns opened above, and only to them: the presence
        // of a variable is the presence of a value in its column.
        let necessary_condition: Option<Box<dyn DocSet>> = match build_necessary_condition_docset(
            &self.required_presence,
            &opened_columns,
            segment_reader.max_doc(),
        ) {
            ResolvedCondition::NoDocs => return Ok(ConstOrVariableSegmentPredicate::Const(false)),
            ResolvedCondition::AllDocs => None,
            ResolvedCondition::DocSet(necessary_condition) => Some(necessary_condition),
        };

        let compiled_fn = segment_reader
            .index()
            .expr_compilation_cache()
            .compile(&self.expression, &variable_types)
            .map_err(|compilation_err| {
                TantivyError::InvalidArgument(format!(
                    "the expression compilation failed {:?}. error: {compilation_err}",
                    self.expression
                ))
            })?;

        // If the function is by nature const, or if all of its variable are known to be null
        // (because we don't have such columns), then we eval the value only once optimize
        //
        // TODO optimize further when the columns have a single value (full + min/max value)
        if variable_types.is_empty() || compiled_fn.inputs().is_empty() {
            // We have no variables!
            // This means eventual inputs are not. Let's return a const predicate.
            let inputs: Vec<VariableValue> =
                std::iter::repeat_n(VariableValue::none(), compiled_fn.inputs().len()).collect();
            let mut string_arena = StringArena::default();
            let result = unsafe { compiled_fn.call(&inputs[..], &mut string_arena) };
            let const_bool = unsafe { result.as_bool() }.unwrap_or(false);
            return Ok(ConstOrVariableSegmentPredicate::Const(const_bool));
        }

        // We ended up with an expression that could not resolve to anything apparently.
        if compiled_fn.result_type() == VarType::None {
            return Ok(ConstOrVariableSegmentPredicate::Const(false));
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
        let mut columns_opt = Vec::with_capacity(compiled_fn.inputs().len());
        for input in compiled_fn.inputs() {
            let column_opt: Option<DynamicColumn> =
                opened_columns.remove(input.variable_name.as_ref());
            if let Some(column) = column_opt {
                if var_type_for_column_type(column.column_type()) != Some(input.r#type) {
                    return Err(TantivyError::InternalError(format!(
                        "compiled input `{}` expects {:?}, but its column has type {}",
                        input.variable_name,
                        input.r#type,
                        column.column_type()
                    )));
                }
                columns_opt.push(Some(column));
            } else {
                columns_opt.push(None);
            }
        }
        // There is one reusable buffer per string column, in ABI order.
        let num_string_inputs = columns_opt
            .iter()
            .filter(|column_opt| matches!(column_opt, Some(DynamicColumn::Str(_))))
            .count();
        let num_inputs = columns_opt.len();
        let predicate = JitExprEvalState {
            compiled: compiled_fn.context(),
            columns_opt,
            string_inputs: vec![String::new(); num_string_inputs],
            input_values: Vec::with_capacity(num_inputs),
        };
        Ok(ConstOrVariableSegmentPredicate::Variable {
            predicate,
            necessary_condition,
        })
    }
}

/// A [`PresenceCondition`] resolved against the columns of a segment.
enum ResolvedCondition {
    /// Satisfied by all of the documents of the segment.
    AllDocs,
    /// Satisfied by none of the documents of the segment.
    NoDocs,
    /// Satisfied by the documents of the `DocSet`, positioned on its first document.
    DocSet(Box<dyn DocSet>),
}

/// Builds a docset off the variable presence condition.
///
/// Variables are bound to the columns of `columns`. A variable missing from `columns` is null
/// for all documents.
fn build_necessary_condition_docset(
    condition: &VariablePresenceCondition,
    columns: &HashMap<&str, DynamicColumn>,
    max_doc: DocId,
) -> ResolvedCondition {
    match condition {
        VariablePresenceCondition::Always => ResolvedCondition::AllDocs,
        VariablePresenceCondition::Never => ResolvedCondition::NoDocs,
        VariablePresenceCondition::Present(variable_name) => {
            let Some(column) = columns.get(variable_name.as_ref()) else {
                return ResolvedCondition::NoDocs;
            };
            let exists_column_index = match column.column_index() {
                ColumnIndex::Empty { .. } => return ResolvedCondition::NoDocs,
                ColumnIndex::Full => return ResolvedCondition::AllDocs,
                ColumnIndex::Optional(optional_index) => {
                    ExistsColumnIndex::Optional(optional_index.clone())
                }
                ColumnIndex::Multivalued(multivalued_index) => {
                    ExistsColumnIndex::Multivalued(multivalued_index.clone())
                }
            };
            ResolvedCondition::DocSet(Box::new(ExistsDocSet::new(exists_column_index)))
        }
        VariablePresenceCondition::All(conditions) => {
            let mut doc_sets: Vec<Box<dyn DocSet>> = Vec::new();
            for condition in conditions.iter() {
                match build_necessary_condition_docset(condition, columns, max_doc) {
                    ResolvedCondition::AllDocs => {}
                    ResolvedCondition::NoDocs => return ResolvedCondition::NoDocs,
                    ResolvedCondition::DocSet(doc_set) => doc_sets.push(doc_set),
                }
            }
            match doc_sets.len() {
                0 => ResolvedCondition::AllDocs,
                1 => ResolvedCondition::DocSet(doc_sets.pop().unwrap()),
                _ => ResolvedCondition::DocSet(Box::new(Intersection::new(doc_sets, max_doc))),
            }
        }
        VariablePresenceCondition::Any(conditions) => {
            let mut doc_sets: Vec<Box<dyn DocSet>> = Vec::new();
            for condition in conditions.iter() {
                match build_necessary_condition_docset(condition, columns, max_doc) {
                    ResolvedCondition::AllDocs => return ResolvedCondition::AllDocs,
                    ResolvedCondition::NoDocs => {}
                    ResolvedCondition::DocSet(doc_set) => doc_sets.push(doc_set),
                }
            }
            match doc_sets.len() {
                0 => ResolvedCondition::NoDocs,
                1 => ResolvedCondition::DocSet(doc_sets.pop().unwrap()),
                _ => ResolvedCondition::DocSet(Box::new(SimpleUnion::build(doc_sets))),
            }
        }
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
    columns_opt: Vec<Option<DynamicColumn>>,
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
            &self.columns_opt,
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
    columns: &[Option<DynamicColumn>],
    string_inputs: &'buffer mut [String],
    input_values: &mut Vec<VariableValue<'buffer>>,
    doc_id: DocId,
) {
    debug_assert!(input_values.is_empty());
    let mut string_inputs = string_inputs.iter_mut();
    for column_opt in columns {
        let Some(column) = column_opt else {
            // The full column is absent. We treat it as None.
            input_values.push(VariableValue::none());
            continue;
        };
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
/// Missing values return None.
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
    use crate::collector::{Count, DocSetCollector};
    use crate::query::doc_predicate_query::DocPredicateQuery;
    use crate::query::{EnableScoring, Query};
    use crate::schema::{Schema, FAST, INDEXED, STORED, STRING};
    use crate::{Index, TantivyDocument, Term};

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
    fn test_simple_missing_field_is_not_null() {
        let index = create_index();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(
            searcher
                .search(&query(r#"(IS_NOT_NULL missing_field)"#), &Count)
                .unwrap(),
            0
        );
    }

    #[test]
    fn test_simple_missing_field_is_null() {
        let index = create_index();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(
            searcher
                .search(&query(r#"(IS_NULL missing_field)"#), &Count)
                .unwrap(),
            4
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

    /// Two segments with sparse, multivalued, and segment-dependent columns, and deleted docs.
    ///
    /// `label` only has values in the second segment.
    fn create_sparse_index() -> Index {
        let mut schema_builder = Schema::builder();
        let id = schema_builder.add_u64_field("id", FAST | INDEXED);
        let number = schema_builder.add_u64_field("number", FAST);
        let score = schema_builder.add_i64_field("score", FAST);
        let flag = schema_builder.add_bool_field("flag", FAST);
        let label = schema_builder.add_text_field("label", STRING | FAST);
        let tags = schema_builder.add_text_field("tags", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests().unwrap();
        for segment_ord in 0..2u64 {
            for i in 0..300u64 {
                let mut doc = TantivyDocument::default();
                doc.add_u64(id, segment_ord * 1000 + i);
                if i % 3 == 0 {
                    doc.add_u64(number, i);
                }
                if i % 5 == 0 {
                    doc.add_i64(score, (i % 7) as i64 - 3);
                }
                if i % 2 == 0 {
                    doc.add_bool(flag, i % 4 == 0);
                }
                if segment_ord == 1 && i % 4 == 0 {
                    doc.add_text(label, ["a", "b", "ab"][(i % 3) as usize]);
                }
                if i % 6 == 0 {
                    doc.add_text(tags, "x");
                    doc.add_text(tags, "y");
                } else if i % 6 == 1 {
                    doc.add_text(tags, "y");
                }
                writer.add_document(doc).unwrap();
            }
            writer.commit().unwrap();
        }
        writer.delete_term(Term::from_field_u64(id, 30));
        writer.delete_term(Term::from_field_u64(id, 1060));
        writer.commit().unwrap();
        index
    }

    /// Returns a query matching the same documents as `query(expression)`, but requiring the
    /// presence of no field, so that all documents are evaluated.
    ///
    /// `(NOT true)` is a present `false`: the disjunction is `true` if and only if `expression` is.
    /// As `NOT` requires the presence of no field, neither does the disjunction.
    fn query_without_required_presence(expression: &str) -> DocPredicateQuery {
        query(&format!("(OR {expression} (NOT true))"))
    }

    #[test]
    fn test_required_presence_does_not_change_results() {
        let index = create_sparse_index();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(searcher.segment_readers().len(), 2);
        let expressions = [
            "true",
            "false",
            "flag",
            "(EQ number 33u64)",
            "(EQ number 30u64)",
            "(IS_NOT_NULL number)",
            "(IS_NULL score)",
            "(NOT (EQ number 3u64))",
            "(NEQ score 1i64)",
            "(GT (ADD number score) 10i64)",
            "(OR (EQ number 3u64) (EQ score 1i64))",
            "(AND flag (IS_NOT_NULL label))",
            r#"(EQ label "a")"#,
            r#"(OR (EQ label "b") flag)"#,
            r#"(REGEXP_LIKE label "b")"#,
            r#"(EQ (UPPER tags) "X")"#,
            "(IF flag (GT number 100u64) (LT score 0i64))",
            "(IS_NOT_NULL (IF flag number score))",
            "(AND (EQ missing 1i64) flag)",
            "(OR (IS_NOT_NULL missing) (EQ score 2i64))",
            "(OR (IS_NULL missing) (EQ score 2i64))",
        ];
        for expression in expressions {
            let expected = searcher
                .search(
                    &query_without_required_presence(expression),
                    &DocSetCollector,
                )
                .unwrap();
            let accelerated = searcher
                .search(&query(expression), &DocSetCollector)
                .unwrap();
            assert_eq!(accelerated, expected, "{expression}");
        }
        // Sanity checks: the index does exercise the predicates.
        let count = |expression: &str| searcher.search(&query(expression), &Count).unwrap();
        assert_eq!(count("(EQ number 33u64)"), 2);
        // Doc 30 of the first segment is deleted.
        assert_eq!(count("(EQ number 30u64)"), 1);
        // `label` is "a" on 25 docs of the second segment, one of which (1060) is deleted.
        assert_eq!(count(r#"(EQ label "a")"#), 24);
    }

    #[test]
    fn test_required_presence_restricts_evaluated_docs() {
        let index = create_sparse_index();
        let searcher = index.reader().unwrap().searcher();
        let segment_reader = searcher.segment_reader(0);
        let size_hint = |query: DocPredicateQuery| {
            query
                .weight(EnableScoring::disabled_from_searcher(&searcher))
                .unwrap()
                .scorer(segment_reader, 1.0)
                .unwrap()
                .size_hint()
        };
        // `number` has a value in one doc out of three.
        assert_eq!(size_hint(query("(GT number 10u64)")), 100);
        assert_eq!(
            size_hint(query_without_required_presence("(GT number 10u64)")),
            300
        );
        // Nothing to require: all docs are evaluated.
        assert_eq!(size_hint(query("(NOT (GT number 10u64))")), 300);
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
