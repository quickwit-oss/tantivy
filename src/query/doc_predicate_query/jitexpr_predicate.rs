use std::collections::HashMap;

use columnar::{ColumnIndex, ColumnType};
use jitexpr::ast::{
    infer_types_with_target, required_presence_for_true, InferredTypeSet, TypeError, UntypedExpr,
    VariablePresenceCondition,
};

use super::{DocPredicate, SegmentDocPredicate};
use crate::aggregation::jitexpr_value_source::{
    JitExprSegmentSource, JitExprValueSource, JitExprValueSourceProvider,
};
use crate::index::SegmentReader;
use crate::query::doc_predicate_query::ConstOrVariableSegmentPredicate;
use crate::query::exist_query::{ExistsColumnIndex, ExistsDocSet};
use crate::query::union::SimpleUnion;
use crate::query::{AllScorer, EmptyScorer, Intersection};
use crate::{DocId, DocSet, TERMINATED};

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
#[derive(Clone, Debug)]
pub struct JitExprPredicate {
    /// Binds the expression to the columns of each segment, and compiles it.
    provider: JitExprValueSourceProvider,
    // A necessary condition, on the presence of the variables, for the expression to be `true`.
    required_presence: VariablePresenceCondition,
}

impl JitExprPredicate {
    /// Creates a predicate after inferring its inputs and requiring a boolean result.
    pub fn new(expression: UntypedExpr) -> Result<Self, TypeError> {
        infer_types_with_target(&expression, InferredTypeSet::BOOLEAN)?;
        let required_presence = required_presence_for_true(&expression);
        let provider = JitExprValueSourceProvider::new(expression)?;
        Ok(Self {
            provider,
            required_presence,
        })
    }

    /// Returns the expression evaluated by this predicate.
    pub fn expression(&self) -> &UntypedExpr {
        self.provider.expression()
    }
}

impl DocPredicate for JitExprPredicate {
    type SegmentDocPredicate = JitExprEvalState;

    fn doc_predicate(
        &self,
        segment_reader: &SegmentReader,
    ) -> crate::Result<ConstOrVariableSegmentPredicate<JitExprEvalState>> {
        let segment_source = self
            .provider
            .open_segment(segment_reader, Some(&[ColumnType::Bool]))?;
        let value_source: JitExprValueSource = match segment_source {
            JitExprSegmentSource::AlwaysNull(_) => {
                return Ok(ConstOrVariableSegmentPredicate::Const(false));
            }
            JitExprSegmentSource::Constant(constant_source) => {
                // Only a present `true` matches. `true` is 1 in the `u64` mapping.
                let const_bool = constant_source.value() == 1u64;
                return Ok(ConstOrVariableSegmentPredicate::Const(const_bool));
            }
            JitExprSegmentSource::Variable(value_source) => value_source,
        };

        // The presence of a variable is the presence of a value in the column of its input. A
        // variable that is not an input of the compiled function is absent. This is sound even if
        // the compiler dropped a bound variable: the result does not depend on it, so if `true`
        // requires its presence, the result is never `true`.
        let input_column_indexes: HashMap<&str, &ColumnIndex> =
            value_source.input_column_indexes().collect();
        let necessary_condition: Box<dyn DocSet> = build_necessary_condition_docset(
            &self.required_presence,
            &input_column_indexes,
            segment_reader.max_doc(),
        );
        if necessary_condition.doc() == TERMINATED {
            return Ok(ConstOrVariableSegmentPredicate::Const(false));
        }
        Ok(ConstOrVariableSegmentPredicate::Variable {
            predicate: JitExprEvalState { value_source },
            necessary_condition,
        })
    }
}

/// Builds a docset off the variable presence condition.
///
/// Variables are bound to the column indexes of `column_indexes`. A variable missing from
/// `column_indexes` is null for all documents.
///
/// The returned `DocSet` is positioned on its first document. It is `TERMINATED` if and only if
/// no document of the segment satisfies the condition.
fn build_necessary_condition_docset(
    condition: &VariablePresenceCondition,
    column_indexes: &HashMap<&str, &ColumnIndex>,
    max_doc: DocId,
) -> Box<dyn DocSet> {
    match condition {
        VariablePresenceCondition::Always => Box::new(AllScorer::new(max_doc)),
        VariablePresenceCondition::Never => Box::new(EmptyScorer),
        VariablePresenceCondition::Present(variable_name) => {
            let Some(column_index) = column_indexes.get(variable_name.as_ref()) else {
                return Box::new(EmptyScorer);
            };
            let exists_column_index = match column_index {
                ColumnIndex::Empty { .. } => return Box::new(EmptyScorer),
                ColumnIndex::Full => return Box::new(AllScorer::new(max_doc)),
                ColumnIndex::Optional(optional_index) => {
                    ExistsColumnIndex::Optional(optional_index.clone())
                }
                ColumnIndex::Multivalued(multivalued_index) => {
                    ExistsColumnIndex::Multivalued(multivalued_index.clone())
                }
            };
            Box::new(ExistsDocSet::new(exists_column_index))
        }
        VariablePresenceCondition::All(conditions) => {
            let mut doc_sets: Vec<Box<dyn DocSet>> = conditions
                .iter()
                .map(|condition| {
                    build_necessary_condition_docset(condition, column_indexes, max_doc)
                })
                .collect();
            match doc_sets.len() {
                0 => Box::new(AllScorer::new(max_doc)),
                1 => doc_sets.pop().unwrap(),
                _ => Box::new(Intersection::new(doc_sets, max_doc)),
            }
        }
        VariablePresenceCondition::Any(conditions) => {
            let doc_sets: Vec<Box<dyn DocSet>> = conditions
                .iter()
                .map(|condition| {
                    build_necessary_condition_docset(condition, column_indexes, max_doc)
                })
                .collect();
            Box::new(SimpleUnion::build(doc_sets))
        }
    }
}

/// The [`SegmentDocPredicate`] produced by [`JitExprPredicate`] for one segment.
pub struct JitExprEvalState {
    /// Invariant: the result type is `Bool` (`open_segment` is called with `Bool` only).
    value_source: JitExprValueSource,
}

impl SegmentDocPredicate for JitExprEvalState {
    fn eval(&mut self, doc_id: DocId) -> bool {
        // Only a present `true` matches. `true` is 1 in the `u64` mapping.
        self.value_source.eval_single_doc(doc_id) == Some(1u64)
    }
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

    #[test]
    fn test_string_predicate_on_large_dictionary() {
        // More terms than `FULL_DECODE_MAX_NUM_TERMS`: the terms are decoded on demand.
        let mut schema_builder = Schema::builder();
        let label = schema_builder.add_text_field("label", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests().unwrap();
        for doc_pos in 0..3000u32 {
            writer
                .add_document(doc!(label => format!("label{:05}", doc_pos % 2000)))
                .unwrap();
        }
        writer.add_document(doc!()).unwrap();
        writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        assert_eq!(
            searcher
                .search(&query(r#"(EQ label "label00042")"#), &Count)
                .unwrap(),
            2
        );
        assert_eq!(
            searcher
                .search(&query(r#"(EQ (UPPER label) "LABEL01999")"#), &Count)
                .unwrap(),
            1
        );
        assert_eq!(
            searcher
                .search(&query(r#"(IS_NULL label)"#), &Count)
                .unwrap(),
            1
        );
    }
}
