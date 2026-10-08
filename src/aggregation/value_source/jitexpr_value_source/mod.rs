//! A [`ValueSource`] computing its values with a jitexpr expression.

mod dynamic_term_dictionary;
mod input_column;
mod jitexpr_value_source;

use std::collections::HashMap;
use std::sync::Arc;

use columnar::{Column, ColumnType, DynamicColumnHandle};
use jitexpr::ast::{infer_types, infer_types_with_target, InferredTypeSet, TypeError, UntypedExpr};
use jitexpr::compile::CompiledFn;
use jitexpr::types::VarType;

use self::input_column::InputColumn;
use self::jitexpr_value_source::{ConstantValueSource, JitExprValueSource};
use super::{ValueSource, ValueSourceProvider};
use crate::query::doc_predicate_query::{find_input_column_handle, var_type_for_column_type};
use crate::{SegmentReader, TantivyError};

/// A [`ValueSourceProvider`] computing values with a jitexpr expression over the fast fields of a
/// segment.
///
/// # Types
///
/// The expr types are resolved for each segment, from the column types of
/// the segment and the column types accepted by the aggregation. The result type can therefore
/// differ from one segment to another: e.g. `(ADD a 1i64)` is `i64` on a segment where `a` is an
/// `i64` column, and `f64` on a segment where `a` is an `f64` column.
///
/// - If the expression can never produce a type accepted by the aggregation (regardless of what
///   columns are present).
///  e.g. jitexpr has no date type: date aggregations always fail.
/// - If the expression does not produce an accepted type on a given segment, the source has no
///   value on that segment.
///
/// # Values
///
/// Each input takes the first value of the doc. A null result means that the doc has no value. See
/// `JitExprValueSource`.
///
/// Values use the monotonic `u64` mapping of fast fields. Text results are term ords of a
/// dictionary built during the evaluation. These ords are not sorted with the terms, so the terms
/// aggregation rejects the options relying on that (e.g. ordering by `_key`).
///
/// # Compilation
///
/// The expression is compiled for each segment, through the expression compilation cache of the
/// index: segments with the same column types share the same compiled function.
#[derive(Clone, Debug)]
pub struct JitExprValueSourceProvider {
    expression: UntypedExpr,
}

impl JitExprValueSourceProvider {
    /// Creates a provider. Returns an error if the expression is not well typed.
    pub fn new(expression: UntypedExpr) -> Result<JitExprValueSourceProvider, TypeError> {
        infer_types(&expression)?;
        Ok(JitExprValueSourceProvider { expression })
    }

    /// Returns the expression evaluated by this provider.
    pub fn expression(&self) -> &UntypedExpr {
        &self.expression
    }
}

impl ValueSourceProvider for JitExprValueSourceProvider {
    fn for_segment(
        &self,
        reader: &SegmentReader,
        allowed_column_types: Option<&[ColumnType]>,
    ) -> crate::Result<Option<Box<dyn ValueSource>>> {
        // The checks below only depend on the expression and on the aggregation, so they fail on
        // all of the segments alike.
        let target_types: InferredTypeSet = target_type_set(allowed_column_types);
        if target_types == InferredTypeSet::NONE {
            return Err(TantivyError::InvalidArgument(format!(
                "the calculated field `{}` cannot produce any of the column types accepted by the \
                 aggregation: {allowed_column_types:?}",
                self.expression
            )));
        }
        let inferred_types: HashMap<&str, InferredTypeSet> =
            infer_types_with_target(&self.expression, target_types).map_err(|type_error| {
                TantivyError::InvalidArgument(format!(
                    "the calculated field `{}` cannot produce any of the types accepted by the \
                     aggregation {target_types}: {type_error}",
                    self.expression
                ))
            })?;

        let mut variable_types: HashMap<&str, VarType> =
            HashMap::with_capacity(inferred_types.len());
        let mut column_handles: HashMap<&str, DynamicColumnHandle> =
            HashMap::with_capacity(inferred_types.len());
        for (variable_name, accepted_types) in inferred_types {
            let Some(handle) = find_input_column_handle(reader, variable_name, accepted_types)
            else {
                // The compiler treats unbound variables as null.
                continue;
            };
            let Some(var_type) = var_type_for_column_type(handle.column_type()) else {
                continue;
            };
            variable_types.insert(variable_name, var_type);
            column_handles.insert(variable_name, handle);
        }

        let compiled_fn: Arc<CompiledFn> = reader
            .index()
            .expr_compilation_cache()
            .compile(&self.expression, &variable_types)
            .map_err(|compile_error| {
                TantivyError::InvalidArgument(format!(
                    "the compilation of the calculated field `{}` failed: {compile_error}",
                    self.expression
                ))
            })?;

        let Some(column_type) = column_type_for_result_type(compiled_fn.result_type()) else {
            // The expression is null for all of the docs of the segment.
            return Ok(Some(empty_source(
                reader,
                ColumnType::U64,
                allowed_column_types,
            )));
        };
        if let Some(allowed_column_types) = allowed_column_types {
            if !allowed_column_types.contains(&column_type) {
                return Ok(Some(empty_source(
                    reader,
                    column_type,
                    Some(allowed_column_types),
                )));
            }
        }

        if compiled_fn.inputs().is_empty() {
            // The expression does not depend on the doc: we evaluate it once.
            let Some(constant_source) =
                ConstantValueSource::evaluate(compiled_fn.context(), column_type)
            else {
                return Ok(Some(empty_source(
                    reader,
                    column_type,
                    allowed_column_types,
                )));
            };
            return Ok(Some(Box::new(constant_source)));
        }

        // The compiler defines the order of the inputs. Do not rely on the inference or on the
        // HashMap iteration order.
        let mut inputs: Vec<InputColumn> = Vec::with_capacity(compiled_fn.inputs().len());
        for compiled_input in compiled_fn.inputs() {
            let Some(handle) = column_handles.remove(compiled_input.variable_name.as_ref()) else {
                return Err(TantivyError::InternalError(format!(
                    "the compiled input `{}` is not bound to a column",
                    compiled_input.variable_name
                )));
            };
            let input = InputColumn::open(&handle)?;
            if input.var_type() != compiled_input.r#type {
                return Err(TantivyError::InternalError(format!(
                    "the compiled input `{}` expects {:?}, but its column has type {}",
                    compiled_input.variable_name,
                    compiled_input.r#type,
                    handle.column_type()
                )));
            }
            inputs.push(input);
        }
        Ok(Some(Box::new(JitExprValueSource::new(
            compiled_fn.context(),
            column_type,
            inputs,
        ))))
    }
}

/// Returns the result types matching the column types accepted by the aggregation.
fn target_type_set(allowed_column_types: Option<&[ColumnType]>) -> InferredTypeSet {
    let Some(allowed_column_types) = allowed_column_types else {
        return InferredTypeSet::ALL;
    };
    let mut target_types = InferredTypeSet::NONE;
    for column_type in allowed_column_types {
        match column_type {
            ColumnType::Bool => target_types.boolean = true,
            ColumnType::I64 => target_types.i64 = true,
            ColumnType::U64 => target_types.u64 = true,
            ColumnType::F64 => target_types.f64 = true,
            ColumnType::Str => target_types.string = true,
            // jitexpr has no such types.
            ColumnType::DateTime | ColumnType::IpAddr | ColumnType::Bytes => {}
        }
    }
    target_types
}

/// Returns `None` for an expression that is always null.
fn column_type_for_result_type(result_type: VarType) -> Option<ColumnType> {
    match result_type {
        VarType::Bool => Some(ColumnType::Bool),
        VarType::I64 => Some(ColumnType::I64),
        VarType::U64 => Some(ColumnType::U64),
        VarType::F64 => Some(ColumnType::F64),
        VarType::Str => Some(ColumnType::Str),
        VarType::None => None,
    }
}

/// Returns a source without any value.
///
/// The source gets `column_type` if the aggregation accepts it, and an accepted type otherwise:
/// with a type the aggregation does not accept, the aggregation would use the fast field with the
/// same name instead.
fn empty_source(
    reader: &SegmentReader,
    column_type: ColumnType,
    allowed_column_types: Option<&[ColumnType]>,
) -> Box<dyn ValueSource> {
    let column_type = match allowed_column_types {
        Some(allowed_column_types) if !allowed_column_types.contains(&column_type) => {
            allowed_column_types.first().copied().unwrap_or(column_type)
        }
        _ => column_type,
    };
    Box::new((
        Column::<u64>::build_empty_column(reader.max_doc()),
        column_type,
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::*;
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::{AggContextParams, AggregationCollector, ValueSourceRegistry};
    use crate::query::AllQuery;
    use crate::schema::{Schema, FAST, STRING};
    use crate::{Index, TantivyDocument};

    fn registry(calculated_fields: &[(&str, &str)]) -> ValueSourceRegistry {
        let mut registry = ValueSourceRegistry::default();
        for (name, expression) in calculated_fields {
            let expression = jitexpr::ast::deserialize(expression).unwrap();
            let provider = JitExprValueSourceProvider::new(expression).unwrap();
            registry.register(name, Arc::new(provider));
        }
        registry
    }

    fn try_run_agg(
        index: &Index,
        calculated_fields: &[(&str, &str)],
        aggs: Value,
    ) -> crate::Result<Value> {
        let context =
            AggContextParams::default().with_value_sources(Arc::new(registry(calculated_fields)));
        let aggs: Aggregations = serde_json::from_value(aggs).unwrap();
        let collector = AggregationCollector::from_aggs(aggs, context);
        let searcher = index.reader().unwrap().searcher();
        let result = searcher.search(&AllQuery, &collector)?;
        Ok(serde_json::to_value(result).unwrap())
    }

    fn run_agg(index: &Index, calculated_fields: &[(&str, &str)], aggs: Value) -> Value {
        try_run_agg(index, calculated_fields, aggs).unwrap()
    }

    /// Returns the `(key, doc_count)` of the buckets, sorted by key.
    fn sorted_bucket_counts(result: &Value, agg_name: &str) -> Vec<(String, u64)> {
        let mut bucket_counts: Vec<(String, u64)> = result[agg_name]["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bucket| {
                (
                    bucket["key"].to_string(),
                    bucket["doc_count"].as_u64().unwrap(),
                )
            })
            .collect();
        bucket_counts.sort();
        bucket_counts
    }

    /// Two segments. The last doc of each segment has no value.
    fn create_index() -> Index {
        let mut schema_builder = Schema::builder();
        let number = schema_builder.add_u64_field("number", FAST);
        let price = schema_builder.add_f64_field("price", FAST);
        let delta = schema_builder.add_i64_field("delta", FAST);
        let flag = schema_builder.add_bool_field("flag", FAST);
        let label = schema_builder.add_text_field("label", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests().unwrap();
        writer
            .add_document(
                doc!(number => 1u64, price => 1.5f64, delta => -3i64, flag => true, label => "a"),
            )
            .unwrap();
        writer
            .add_document(
                doc!(number => 2u64, price => -2.5f64, delta => 4i64, flag => false, label => "b"),
            )
            .unwrap();
        writer.add_document(doc!()).unwrap();
        writer.commit().unwrap();
        writer
            .add_document(
                doc!(number => 3u64, price => 10.0f64, delta => -1i64, flag => true, label => "a"),
            )
            .unwrap();
        writer.add_document(doc!()).unwrap();
        writer.commit().unwrap();
        index
    }

    #[test]
    fn test_metrics_over_calculated_field() {
        let index = create_index();
        let result = run_agg(
            &index,
            &[("doubled", "(MULTIPLY number 2u64)")],
            json!({
                "s": { "stats": { "field": "doubled" } },
                "with_missing": { "stats": { "field": "doubled", "missing": 100.0 } }
            }),
        );
        // The docs without `number` evaluate to null, and have no value.
        assert_eq!(result["s"]["count"], 3);
        assert_eq!(result["s"]["sum"], 12.0);
        assert_eq!(result["s"]["min"], 2.0);
        assert_eq!(result["s"]["max"], 6.0);
        assert_eq!(result["with_missing"]["count"], 5);
        assert_eq!(result["with_missing"]["sum"], 212.0);
    }

    #[test]
    fn test_histogram_and_range_over_calculated_field() {
        let index = create_index();
        let result = run_agg(
            &index,
            &[("shifted", "(ADD price 1.0f64)")],
            json!({
                "h": { "histogram": { "field": "shifted", "interval": 5.0 } },
                "r": { "range": { "field": "shifted", "ranges": [{ "to": 0.0 }, { "from": 0.0 }] } }
            }),
        );
        let histogram_counts: Vec<(f64, u64)> = result["h"]["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bucket| {
                (
                    bucket["key"].as_f64().unwrap(),
                    bucket["doc_count"].as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            histogram_counts,
            vec![(-5.0, 1), (0.0, 1), (5.0, 0), (10.0, 1)]
        );
        let range_buckets = result["r"]["buckets"].as_array().unwrap();
        assert_eq!(range_buckets[0]["doc_count"], 1);
        assert_eq!(range_buckets[1]["doc_count"], 2);
    }

    /// An aggregation over the identity calculated field must match the aggregation over the
    /// field itself. This checks the monotonic mapping of each type.
    #[test]
    fn test_identity_matches_fast_field_for_each_type() {
        let index = create_index();
        for (field, agg_kinds) in [
            ("number", &["terms", "stats"][..]),
            ("price", &["terms", "stats"][..]),
            ("delta", &["terms", "stats"][..]),
            ("flag", &["terms"][..]),
            ("label", &["terms", "cardinality"][..]),
        ] {
            for agg_kind in agg_kinds {
                let agg_on =
                    |field_name: &str| json!({ "agg": { *agg_kind: { "field": field_name } } });
                let expected = run_agg(&index, &[], agg_on(field));
                let actual = run_agg(&index, &[("calculated", field)], agg_on("calculated"));
                assert_eq!(actual, expected, "{agg_kind} over {field}");
            }
        }
    }

    #[test]
    fn test_registered_name_shadows_fast_field() {
        let index = create_index();
        // The variable `number` is the fast field, the aggregated `number` is the calculated
        // field.
        let result = run_agg(
            &index,
            &[("number", "(MULTIPLY number 10u64)")],
            json!({ "s": { "sum": { "field": "number" } } }),
        );
        assert_eq!(result["s"]["value"], 60.0);
    }

    #[test]
    fn test_constant_calculated_field() {
        let index = create_index();
        let result = run_agg(
            &index,
            &[("three", "(ADD 1u64 2u64)"), ("constant_text", r#""x""#)],
            json!({
                "s": { "stats": { "field": "three" } },
                "t": { "terms": { "field": "constant_text" } }
            }),
        );
        assert_eq!(result["s"]["count"], 5);
        assert_eq!(result["s"]["sum"], 15.0);
        assert_eq!(
            sorted_bucket_counts(&result, "t"),
            vec![(r#""x""#.to_string(), 5)]
        );
    }

    #[test]
    fn test_unknown_variable_has_no_value() {
        let index = create_index();
        let result = run_agg(
            &index,
            &[("unknown", "(ADD absent_field 1u64)")],
            json!({ "s": { "stats": { "field": "unknown" } } }),
        );
        assert_eq!(result["s"]["count"], 0);
    }

    #[test]
    fn test_incompatible_calculated_field_is_an_error() {
        let index = create_index();
        let err = try_run_agg(
            &index,
            &[("upper", "(UPPER label)")],
            json!({ "s": { "stats": { "field": "upper" } } }),
        )
        .unwrap_err();
        assert!(err.to_string().contains("cannot produce"), "{err}");
        // jitexpr has no date type.
        let err = try_run_agg(
            &index,
            &[("doubled", "(MULTIPLY number 2u64)")],
            json!({ "h": { "date_histogram": { "field": "doubled", "fixed_interval": "1d" } } }),
        )
        .unwrap_err();
        assert!(err.to_string().contains("cannot produce"), "{err}");
    }

    #[test]
    fn test_multivalued_inputs_use_first_value() {
        let mut schema_builder = Schema::builder();
        let number = schema_builder.add_u64_field("number", FAST);
        let other = schema_builder.add_u64_field("other", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests().unwrap();
        writer
            .add_document(doc!(number => 1u64, number => 2u64, other => 10u64, other => 20u64))
            .unwrap();
        writer
            .add_document(doc!(number => 3u64, other => 30u64))
            .unwrap();
        writer.add_document(doc!(other => 40u64)).unwrap();
        writer.commit().unwrap();

        // Each input takes the first value of the doc. The doc without `number` is null.
        let result = run_agg(
            &index,
            &[("sum", "(ADD number other)")],
            json!({
                "s": { "stats": { "field": "sum" } },
                "t": { "terms": { "field": "sum" } }
            }),
        );
        assert_eq!(result["s"]["count"], 2);
        assert_eq!(result["s"]["sum"], 11.0 + 33.0);
        assert_eq!(
            sorted_bucket_counts(&result, "t"),
            vec![("11".to_string(), 1), ("33".to_string(), 1)]
        );

        // A doc without a value for an input is evaluated with null.
        let result = run_agg(
            &index,
            &[("is_null", "(IS_NULL number)")],
            json!({ "t": { "terms": { "field": "is_null" } } }),
        );
        // Bool keys are serialized as 0 / 1, like for a bool fast field.
        assert_eq!(
            sorted_bucket_counts(&result, "t"),
            vec![("0".to_string(), 2), ("1".to_string(), 1)]
        );
    }

    /// Two segments, in which the labels are first seen in a different order.
    fn create_label_index() -> Index {
        let mut schema_builder = Schema::builder();
        let label = schema_builder.add_text_field("label", STRING | FAST);
        let score = schema_builder.add_u64_field("score", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests().unwrap();
        for (label_value, score_value) in [("b", 1u64), ("a", 2), ("b", 3), ("c", 4)] {
            writer
                .add_document(doc!(label => label_value, score => score_value))
                .unwrap();
        }
        writer.add_document(doc!(score => 100u64)).unwrap();
        writer.commit().unwrap();
        for (label_value, score_value) in [("c", 5u64), ("a", 6), ("b", 7)] {
            writer
                .add_document(doc!(label => label_value, score => score_value))
                .unwrap();
        }
        // A multivalued doc: only its first value, `a`, is used.
        writer
            .add_document(doc!(label => "a", label => "d", score => 8u64))
            .unwrap();
        writer.commit().unwrap();
        index
    }

    #[test]
    fn test_terms_over_calculated_text_field() {
        let index = create_label_index();
        let result = run_agg(
            &index,
            &[("upper", "(UPPER label)")],
            json!({
                "t": {
                    "terms": { "field": "upper" },
                    "aggs": { "score_sum": { "sum": { "field": "score" } } }
                }
            }),
        );
        let keys_counts_sums: Vec<(&str, u64, f64)> = result["t"]["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bucket| {
                (
                    bucket["key"].as_str().unwrap(),
                    bucket["doc_count"].as_u64().unwrap(),
                    bucket["score_sum"]["value"].as_f64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            keys_counts_sums,
            vec![("A", 3, 16.0), ("B", 3, 11.0), ("C", 2, 9.0)]
        );
    }

    #[test]
    fn test_terms_ordered_by_sub_aggregation_over_calculated_text_field() {
        let index = create_label_index();
        let result = run_agg(
            &index,
            &[("upper", "(UPPER label)")],
            json!({
                "t": {
                    "terms": { "field": "upper", "order": { "score_min": "asc" } },
                    "aggs": { "score_min": { "min": { "field": "score" } } }
                }
            }),
        );
        let keys: Vec<&str> = result["t"]["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bucket| bucket["key"].as_str().unwrap())
            .collect();
        assert_eq!(keys, vec!["B", "A", "C"]);
    }

    #[test]
    fn test_cardinality_over_calculated_text_field() {
        let index = create_label_index();
        let result = run_agg(
            &index,
            &[("first_letter", "(LEFT (UPPER label) 1u64)")],
            json!({ "c": { "cardinality": { "field": "first_letter" } } }),
        );
        assert_eq!(result["c"]["value"], 3.0);
    }

    #[test]
    fn test_unsupported_terms_options_on_calculated_text_field() {
        let index = create_label_index();
        let calculated_fields = [("upper", "(UPPER label)")];
        for (terms_req, expected_message) in [
            (
                json!({ "field": "upper", "order": { "_key": "asc" } }),
                "ordered by `_key`",
            ),
            (
                json!({ "field": "upper", "min_doc_count": 0 }),
                "`min_doc_count: 0`",
            ),
            (
                json!({ "field": "upper", "include": "A.*" }),
                "`include` / `exclude`",
            ),
            (
                json!({ "field": "upper", "exclude": ["A"] }),
                "`include` / `exclude`",
            ),
            (json!({ "field": "upper", "missing": "NONE" }), "`missing`"),
        ] {
            let err = try_run_agg(
                &index,
                &calculated_fields,
                json!({ "t": { "terms": terms_req } }),
            )
            .unwrap_err();
            assert!(
                err.to_string().contains(expected_message),
                "{terms_req}: {err}"
            );
        }
    }

    #[test]
    fn test_result_type_is_resolved_per_segment() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("j", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests().unwrap();
        writer
            .add_document(doc!(json_field => json!({ "x": 2 })))
            .unwrap();
        writer.commit().unwrap();
        writer
            .add_document(doc!(json_field => json!({ "x": 1.5 })))
            .unwrap();
        writer.commit().unwrap();

        let expression = jitexpr::ast::deserialize("(ADD j.x 1i64)").unwrap();
        let provider = JitExprValueSourceProvider::new(expression).unwrap();
        let searcher = index.reader().unwrap().searcher();
        let mut column_types: Vec<ColumnType> = Vec::new();
        for segment_reader in searcher.segment_readers() {
            let source = provider
                .for_segment(segment_reader, None)
                .unwrap()
                .expect("a calculated field always returns a source");
            column_types.push(source.column_type());
        }
        column_types.sort_by_key(|column_type| column_type.to_code());
        assert_eq!(column_types, vec![ColumnType::I64, ColumnType::F64]);

        let result = run_agg(
            &index,
            &[("x_plus_one", "(ADD j.x 1i64)")],
            json!({ "s": { "stats": { "field": "x_plus_one" } } }),
        );
        assert_eq!(result["s"]["count"], 2);
        assert_eq!(result["s"]["sum"], 5.5);
    }

    #[test]
    fn test_compilation_uses_the_index_cache() {
        let index = create_index();
        let cache = index.expr_compilation_cache();
        assert!(cache.is_empty());
        run_agg(
            &index,
            &[("doubled", "(MULTIPLY number 2u64)")],
            json!({ "s": { "stats": { "field": "doubled" } } }),
        );
        // Both segments have the same column types, and share the same compiled function.
        assert_eq!(cache.len(), 1);
        run_agg(
            &index,
            &[("doubled", "(MULTIPLY number 2u64)")],
            json!({ "s": { "sum": { "field": "doubled" } } }),
        );
        assert_eq!(cache.len(), 1);
    }

    /// Calculated fields over many docs (several blocks), with sparse inputs and deletes, must
    /// match the same values materialized in fast fields.
    #[test]
    fn test_matches_materialized_fields() {
        let mut schema_builder = Schema::builder();
        let id = schema_builder.add_u64_field("id", FAST | crate::schema::INDEXED);
        let number = schema_builder.add_u64_field("number", FAST);
        let label = schema_builder.add_text_field("label", STRING | FAST);
        let tripled = schema_builder.add_u64_field("tripled", FAST);
        let upper = schema_builder.add_text_field("upper", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests().unwrap();
        for segment_ord in 0..2u64 {
            for i in 0..300u64 {
                let mut doc = TantivyDocument::default();
                doc.add_u64(id, segment_ord * 1000 + i);
                if i % 3 != 0 {
                    doc.add_u64(number, i % 17);
                    doc.add_u64(tripled, (i % 17) * 3);
                }
                if i % 2 == 0 {
                    let label_value = ["a", "bb", "ccc", "dd"][((i + segment_ord) % 4) as usize];
                    doc.add_text(label, label_value);
                    doc.add_text(upper, label_value.to_uppercase());
                }
                writer.add_document(doc).unwrap();
            }
            writer.commit().unwrap();
        }
        writer.delete_term(crate::Term::from_field_u64(id, 4));
        writer.delete_term(crate::Term::from_field_u64(id, 1002));
        writer.commit().unwrap();

        let calculated_fields = [
            ("tripled_calc", "(MULTIPLY number 3u64)"),
            ("upper_calc", "(UPPER label)"),
        ];
        let aggs_on = |tripled_field: &str, upper_field: &str| {
            json!({
                "tripled_stats": { "stats": { "field": tripled_field } },
                "tripled_terms": { "terms": { "field": tripled_field, "size": 100 } },
                "upper_terms": { "terms": { "field": upper_field, "size": 100 } },
                "upper_cardinality": { "cardinality": { "field": upper_field } }
            })
        };
        let expected = run_agg(&index, &[], aggs_on("tripled", "upper"));
        let actual = run_agg(
            &index,
            &calculated_fields,
            aggs_on("tripled_calc", "upper_calc"),
        );
        assert_eq!(actual["tripled_stats"], expected["tripled_stats"]);
        assert_eq!(actual["upper_cardinality"], expected["upper_cardinality"]);
        for agg_name in ["tripled_terms", "upper_terms"] {
            assert_eq!(
                sorted_bucket_counts(&actual, agg_name),
                sorted_bucket_counts(&expected, agg_name),
                "{agg_name}"
            );
        }
        assert_eq!(expected["tripled_stats"]["count"], 398);
    }
}
