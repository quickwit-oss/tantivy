use std::collections::HashMap;

use columnar::{ColumnType, DynamicColumn};
use jitexpr::ast::{infer_types_with_target, InferredTypeSet, TypeError, UntypedExpr};
use jitexpr::compile::{compile, CompiledFn};
use jitexpr::types::{StringRef, VarType, VariableValue};

use crate::docset::{SeekDangerResult, TERMINATED};
use crate::index::SegmentReader;
use crate::query::explanation::does_not_match;
use crate::query::{EmptyScorer, EnableScoring, Explanation, Query, Scorer, Weight};
use crate::{DocId, DocSet, Score, TantivyError};

/// A query that evaluates a boolean JIT expression against fast-field columns.
///
/// Variable names in the expression are resolved as fast-field names for each
/// segment. Multivalued columns contribute their first value. A document with a
/// missing input value does not match.
#[derive(Clone, Debug)]
pub struct CalculatedPredicateQuery {
    expression: UntypedExpr,
    inferred_inputs: Vec<(String, InferredTypeSet)>,
}

impl CalculatedPredicateQuery {
    /// Creates a query after inferring its inputs and requiring a boolean result.
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

    /// Returns the expression evaluated by this query.
    pub fn expression(&self) -> &UntypedExpr {
        &self.expression
    }
}

impl Query for CalculatedPredicateQuery {
    fn weight(&self, _enable_scoring: EnableScoring) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(CalculatedPredicateWeight {
            expression: self.expression.clone(),
            inferred_inputs: self.inferred_inputs.clone(),
        }))
    }
}

struct CalculatedPredicateWeight {
    expression: UntypedExpr,
    inferred_inputs: Vec<(String, InferredTypeSet)>,
}

impl Weight for CalculatedPredicateWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let mut variable_types = HashMap::new();
        let mut opened_columns = HashMap::new();

        for (name, accepted_types) in &self.inferred_inputs {
            if let Some(column) = open_input_column(reader, name, *accepted_types)? {
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
            return Ok(Box::new(EmptyScorer));
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

        Ok(Box::new(CalculatedPredicateScorer::new(
            compiled,
            columns,
            reader.max_doc(),
            boost,
        )))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        Ok(Explanation::new("CalculatedPredicateQuery", 1.0))
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
    value_ref: Box<StringRef>,
}

impl StringInput {
    fn new() -> Self {
        Self {
            value: String::new(),
            value_ref: Box::new(StringRef::new("")),
        }
    }
}

struct CalculatedPredicateScorer {
    compiled: CompiledFn,
    columns: Vec<DynamicColumn>,
    string_inputs: Vec<StringInput>,
    input_values: Vec<VariableValue>,
    doc: DocId,
    max_doc: DocId,
    score: Score,
}

// SAFETY: Every string pointer stored in `input_values` points to a boxed
// `StringRef` owned by `string_inputs`. The boxes and all resources
// referenced by `compiled` remain alive when the scorer is moved. `DocSet`
// access requires `&mut self`, so evaluation cannot happen concurrently.
unsafe impl Send for CalculatedPredicateScorer {}

impl CalculatedPredicateScorer {
    fn new(
        compiled: CompiledFn,
        columns: Vec<DynamicColumn>,
        max_doc: DocId,
        score: Score,
    ) -> Self {
        let input_values = vec![VariableValue::default(); columns.len()];
        let string_inputs = columns
            .iter()
            .filter(|column| matches!(column, DynamicColumn::Str(_)))
            .map(|_| StringInput::new())
            .collect();
        let mut scorer = Self {
            compiled,
            columns,
            string_inputs,
            input_values,
            doc: 0,
            max_doc,
            score,
        };
        scorer.doc = scorer.find_match(0);
        scorer
    }

    fn populate_inputs(&mut self, doc: DocId) -> bool {
        let mut string_inputs = self.string_inputs.iter_mut();
        for (column, input_value) in self.columns.iter().zip(&mut self.input_values) {
            match column {
                DynamicColumn::Bool(column) => {
                    let Some(value) = column.first(doc) else {
                        return false;
                    };
                    *input_value = VariableValue { boolean: value };
                }
                DynamicColumn::I64(column) => {
                    let Some(value) = column.first(doc) else {
                        return false;
                    };
                    *input_value = VariableValue { int_i64: value };
                }
                DynamicColumn::U64(column) => {
                    let Some(value) = column.first(doc) else {
                        return false;
                    };
                    *input_value = VariableValue { int_u64: value };
                }
                DynamicColumn::F64(column) => {
                    let Some(value) = column.first(doc) else {
                        return false;
                    };
                    *input_value = VariableValue { float: value };
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
                    *string_input.value_ref = StringRef::new(&string_input.value);
                    *input_value = VariableValue {
                        string: string_input.value_ref.as_mut(),
                    };
                }
                DynamicColumn::Bytes(_) | DynamicColumn::IpAddr(_) | DynamicColumn::DateTime(_) => {
                    unreachable!("unsupported columns are filtered before compilation")
                }
            }
        }
        true
    }

    fn find_match(&mut self, mut target: DocId) -> DocId {
        loop {
            match self.seek_danger(target) {
                SeekDangerResult::Found => return target,
                SeekDangerResult::SeekLowerBound(next_target) => {
                    if next_target >= self.max_doc {
                        self.doc = TERMINATED;
                        return TERMINATED;
                    }
                    target = next_target;
                }
            }
        }
    }
}

impl DocSet for CalculatedPredicateScorer {
    fn advance(&mut self) -> DocId {
        if self.doc == TERMINATED {
            return TERMINATED;
        }
        self.find_match(self.doc + 1)
    }

    fn seek(&mut self, target: DocId) -> DocId {
        debug_assert!(target >= self.doc);
        if self.doc == TERMINATED {
            return TERMINATED;
        }
        self.find_match(target)
    }

    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        if target >= self.max_doc {
            self.doc = TERMINATED;
            return SeekDangerResult::SeekLowerBound(TERMINATED);
        }

        // TODO: fix me. With predicate,it is actually often acceptable to have Null populated here.
        //
        if !self.populate_inputs(target) {
            return SeekDangerResult::SeekLowerBound(target + 1);
        }
        let mut result = VariableValue { boolean: false };
        // SAFETY: `columns` and `input_values` were built in `compiled.inputs`
        // order, and every slot is populated using that input's concrete type.
        unsafe { self.compiled.call(&self.input_values, &mut result) };
        if unsafe { result.boolean } {
            self.doc = target;
            SeekDangerResult::Found
        } else {
            SeekDangerResult::SeekLowerBound(target + 1)
        }
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    fn size_hint(&self) -> u32 {
        self.max_doc
    }
}

impl Scorer for CalculatedPredicateScorer {
    fn score(&mut self) -> Score {
        self.score
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::Count;
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

    fn query(expression: &str) -> CalculatedPredicateQuery {
        CalculatedPredicateQuery::new(jitexpr::ast::deserialize(expression).unwrap()).unwrap()
    }

    #[test]
    fn test_constructor_requires_boolean_expression() {
        let expression = jitexpr::ast::deserialize("(ADD number 1u64)").unwrap();

        assert!(CalculatedPredicateQuery::new(expression).is_err());
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
    fn test_seek_danger_evaluates_target_document() -> crate::Result<()> {
        let index = create_index()?;
        let searcher = index.reader()?.searcher();
        let weight = query("flag").weight(EnableScoring::disabled_from_searcher(&searcher))?;
        let mut scorer = weight.scorer(searcher.segment_reader(0), 1.0)?;

        assert_eq!(scorer.doc(), 0);
        assert_eq!(scorer.seek_danger(1), SeekDangerResult::SeekLowerBound(2));
        assert_eq!(scorer.seek_danger(2), SeekDangerResult::Found);
        assert_eq!(scorer.doc(), 2);
        Ok(())
    }

    #[test]
    fn test_missing_boolean_column_returns_empty_scorer() -> crate::Result<()> {
        let index = create_index()?;
        let searcher = index.reader()?.searcher();
        let weight = query("missing").weight(EnableScoring::disabled_from_searcher(&searcher))?;
        let scorer = weight.scorer(searcher.segment_reader(0), 1.0)?;

        assert!(scorer.is::<EmptyScorer>());
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
