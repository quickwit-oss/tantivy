//! Aggregations over calculated fields: a [`ValueSource`] evaluating a jitexpr expression.

mod dynamic_term_dict;

use std::collections::HashMap;

use columnar::{
    Cardinality, Column, ColumnType, DynamicColumn, DynamicColumnHandle,
    MonotonicallyMappableToU64, RowId, StrColumn,
};
use jitexpr::ast::{infer_types, infer_types_with_target, InferredTypeSet, TypeError, UntypedExpr};
use jitexpr::compile::CompiledFnCtx;
use jitexpr::types::{VarType, VariableValue};

use self::dynamic_term_dict::DynamicTermDict;
use super::{ValueSource, ValueSourceDictionary, ValueSourceProvider};
use crate::jitexpr_binding::{find_input_column_handle, var_type_for_column_type};
use crate::{DocId, SegmentReader, TantivyError};

/// A [`ValueSourceProvider`] evaluating a jitexpr expression over the fast fields of a segment.
///
/// # Values
///
/// Values are produced in the monotonic `u64` mapping tantivy uses for fast fields
/// ([`MonotonicallyMappableToU64`]). Text results are term ords, attributed in
/// first-seen order by a dictionary built while evaluating the segment: they are NOT sorted with
/// the terms, so the aggregations relying on that (e.g. terms ordered by `_key`) reject them.
///
/// # Multivalued inputs
///
/// Each input takes the first value of the document: the expression is evaluated at most once per
/// document, and produces at most one value. See `JitExprValueSource`.
///
/// # Types
///
/// The result type is resolved per segment, so it can differ from one segment to another (e.g.
/// `(ADD a b)` is `i64` where `a` has an `i64` column, `f64` where it has an `f64` column).
///
/// - If the expression can never produce a type the aggregation accepts, `for_segment` returns an
///   error.
/// - If it only does not for a specific segment, the aggregation falls back to the physical fast
///   field with the same name in that segment, if any.
pub struct JitExprValueSourceProvider {
    expression: UntypedExpr,
}

impl JitExprValueSourceProvider {
    /// Creates a provider, checking that the expression is well typed.
    pub fn new(expression: UntypedExpr) -> Result<Self, TypeError> {
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
    ) -> crate::Result<Box<dyn ValueSource>> {
        let target_types: InferredTypeSet = target_type_set(allowed_column_types);
        if target_types == InferredTypeSet::NONE {
            return Err(TantivyError::InvalidArgument(format!(
                "the calculated field `{}` cannot produce any of the types accepted by the \
                 aggregation: {allowed_column_types:?}",
                self.expression
            )));
        }
        // This only depends on the expression, so it fails on all segments alike.
        let inferred_types: HashMap<&str, InferredTypeSet> =
            infer_types_with_target(&self.expression, target_types).map_err(|type_err| {
                TantivyError::InvalidArgument(format!(
                    "the calculated field `{}` cannot produce any of the types accepted by the \
                     aggregation {target_types}: {type_err}",
                    self.expression
                ))
            })?;

        let mut variable_types: HashMap<&str, VarType> =
            HashMap::with_capacity(inferred_types.len());
        let mut column_handles: HashMap<&str, DynamicColumnHandle> =
            HashMap::with_capacity(inferred_types.len());
        for (name, accepted_types) in inferred_types {
            let Some(handle) = find_input_column_handle(reader, name, accepted_types)? else {
                // Unbound variables are compiled as null.
                continue;
            };
            let Some(var_type) = var_type_for_column_type(handle.column_type()) else {
                continue;
            };
            variable_types.insert(name, var_type);
            column_handles.insert(name, handle);
        }

        let compiled_fn = reader
            .index()
            .expr_compilation_cache()
            .compile(&self.expression, &variable_types)
            .map_err(|compilation_err| {
                TantivyError::InvalidArgument(format!(
                    "the compilation of the calculated field `{}` failed: {compilation_err}",
                    self.expression
                ))
            })?;

        let result_type = compiled_fn.result_type();
        let Some(column_type) = column_type_for_result_type(result_type) else {
            // The expression is always null on this segment.
            return Ok(empty_source(reader, ColumnType::U64));
        };

        // The compiler owns the definitive ABI order. Do not rely on inference or HashMap
        // iteration order when building the argument slots.
        let mut inputs: Vec<Option<InputColumn>> = Vec::with_capacity(compiled_fn.inputs().len());
        for input in compiled_fn.inputs() {
            let Some(handle) = column_handles.remove(input.variable_name.as_ref()) else {
                inputs.push(None);
                continue;
            };
            if var_type_for_column_type(handle.column_type()) != Some(input.r#type) {
                return Err(TantivyError::InternalError(format!(
                    "compiled input `{}` expects {:?}, but its column has type {}",
                    input.variable_name,
                    input.r#type,
                    handle.column_type()
                )));
            }
            inputs.push(Some(InputColumn::open(&handle, input.r#type)?));
        }

        let mut compiled: CompiledFnCtx = compiled_fn.context();
        if inputs.iter().all(Option::is_none) {
            // The expression does not depend on the document: evaluate it once.
            let args: Vec<VariableValue> = vec![VariableValue::none(); inputs.len()];
            // SAFETY: all of the arguments are null, which is valid for any input type.
            let result: VariableValue = unsafe { compiled.call(&args) };
            let mut term_dict = DynamicTermDict::default();
            // SAFETY: `result` is of type `result_type`.
            let Some(value) = (unsafe { encode_result(result, result_type, &mut term_dict) })
            else {
                return Ok(empty_source(reader, column_type));
            };
            return Ok(Box::new(ConstantSource {
                column_type,
                value,
                term_dict,
            }));
        }

        let num_inputs = inputs.len();
        let input_blocks: Vec<InputBlock> = std::iter::repeat_with(InputBlock::default)
            .take(num_inputs)
            .collect();
        Ok(Box::new(JitExprValueSource {
            compiled,
            result_type,
            column_type,
            inputs,
            cursors: vec![0; num_inputs],
            input_blocks,
            term_dict: DynamicTermDict::default(),
        }))
    }
}

/// Converts the column types an aggregation accepts into the result types the expression may
/// have.
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

/// A source without any value, for a segment where the expression is always null.
fn empty_source(reader: &SegmentReader, column_type: ColumnType) -> Box<dyn ValueSource> {
    Box::new((
        Column::<u64>::build_empty_column(reader.num_docs()),
        column_type,
    ))
}

/// Encodes a result in the monotonic `u64` mapping. Null results are `None`.
///
/// Text results are interned in `term_dict`, and encoded as their term ord.
///
/// # Safety
///
/// `result` must be of type `result_type`.
unsafe fn encode_result(
    result: VariableValue,
    result_type: VarType,
    term_dict: &mut DynamicTermDict,
) -> Option<u64> {
    // SAFETY: guaranteed by the caller.
    unsafe {
        match result_type {
            VarType::Bool => result.as_bool().map(bool::to_u64),
            VarType::I64 => result.as_i64().map(i64::to_u64),
            VarType::U64 => result.as_u64(),
            VarType::F64 => result.as_f64().map(f64::to_u64),
            VarType::Str => result
                .as_str()
                .map(|term: &str| term_dict.intern(term.as_bytes())),
            VarType::None => None,
        }
    }
}

/// A source with the same value for all of the documents.
#[derive(Debug)]
struct ConstantSource {
    column_type: ColumnType,
    /// In the monotonic `u64` mapping. A term ord of `term_dict` for `Str` sources.
    value: u64,
    /// Holds the single term of a `Str` source.
    term_dict: DynamicTermDict,
}

impl ValueSource for ConstantSource {
    fn column_type(&self) -> ColumnType {
        self.column_type
    }

    fn load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        _docids: &mut Vec<DocId>,
        _row_ids: &mut Vec<RowId>,
    ) -> Cardinality {
        values.clear();
        values.resize(docs.len(), self.value);
        Cardinality::Full
    }

    fn term_dictionary(&self) -> Option<&dyn ValueSourceDictionary> {
        if self.column_type != ColumnType::Str {
            return None;
        }
        Some(&self.term_dict)
    }
}

/// A column bound to an input of the compiled expression.
struct InputColumn {
    var_type: VarType,
    /// Values in their monotonic `u64` mapping. Term ords for `Str` inputs.
    column: Column<u64>,
    /// The dictionary of the term ords, for `Str` inputs.
    str_column: Option<StrColumn>,
}

impl InputColumn {
    fn open(handle: &DynamicColumnHandle, var_type: VarType) -> crate::Result<InputColumn> {
        if var_type == VarType::Str {
            let DynamicColumn::Str(str_column) = handle.open()? else {
                return Err(TantivyError::InternalError(
                    "a string input must be bound to a string column".to_string(),
                ));
            };
            return Ok(InputColumn {
                var_type,
                column: str_column.ords().clone(),
                str_column: Some(str_column),
            });
        }
        let column: Column<u64> = handle.open_u64_lenient()?.ok_or_else(|| {
            TantivyError::InternalError(format!(
                "could not open the column of type {} as u64",
                handle.column_type()
            ))
        })?;
        Ok(InputColumn {
            var_type,
            column,
            str_column: None,
        })
    }
}

/// The values of an input for the current block of documents.
#[derive(Default)]
struct InputBlock {
    /// If true, `values[i]` is the value of `docs[i]`, and `docids` is not used.
    is_full: bool,
    /// The documents of `values`, in increasing order. A document is repeated once per value.
    docids: Vec<DocId>,
    /// Values in their monotonic `u64` mapping.
    values: Vec<u64>,
    row_ids: Vec<RowId>,
    /// The terms of the `values`, for `Str` inputs.
    terms: BlockTerms,
}

impl InputBlock {
    fn load(&mut self, input: &InputColumn, docs: &[DocId]) {
        let mut source = (&input.column, ColumnType::U64);
        let cardinality =
            source.load_block(docs, &mut self.values, &mut self.docids, &mut self.row_ids);
        self.is_full = cardinality.is_full();
        if let Some(str_column) = &input.str_column {
            self.terms.load(str_column, &self.values);
        }
    }

    /// Returns the position in `values` of the first value of `doc`, if any.
    ///
    /// `doc_pos` is the position of `doc` in the block. `cursor` is a position in `docids` before
    /// which all of the documents are lower than `doc`: it must be reset to 0 after `load`, and
    /// `doc` must be greater than the documents of the previous calls since then.
    fn first_value_pos(&self, doc_pos: usize, doc: DocId, cursor: &mut usize) -> Option<usize> {
        if self.is_full {
            return Some(doc_pos);
        }
        while *cursor < self.docids.len() && self.docids[*cursor] < doc {
            *cursor += 1;
        }
        if *cursor < self.docids.len() && self.docids[*cursor] == doc {
            return Some(*cursor);
        }
        None
    }
}

/// The terms of the term ords of a block, resolved with a single dictionary lookup.
#[derive(Default)]
struct BlockTerms {
    /// Distinct term ords of the block, sorted.
    sorted_ords: Vec<u64>,
    /// Concatenated terms, in the order of `sorted_ords`.
    bytes: Vec<u8>,
    /// `bytes[offsets[i]..offsets[i + 1]]` is the term of `sorted_ords[i]`.
    offsets: Vec<usize>,
}

impl BlockTerms {
    fn load(&mut self, str_column: &StrColumn, term_ords: &[u64]) {
        self.sorted_ords.clear();
        self.sorted_ords.extend_from_slice(term_ords);
        self.sorted_ords.sort_unstable();
        self.sorted_ords.dedup();
        self.bytes.clear();
        self.offsets.clear();
        self.offsets.push(0);
        let bytes = &mut self.bytes;
        let offsets = &mut self.offsets;
        // `load_block` cannot return I/O errors; an unreadable dictionary therefore panics, like
        // in the jitexpr query predicate.
        let all_found = str_column
            .dictionary()
            .sorted_ords_to_term_cb(&self.sorted_ords, |term| {
                bytes.extend_from_slice(term);
                offsets.push(bytes.len());
            })
            .expect("fast-field string dictionary is corrupted");
        assert!(all_found, "fast-field string dictionary is corrupted");
    }

    /// Precondition: `term_ord` is one of the ords given to `load`.
    fn term(&self, term_ord: u64) -> Option<&str> {
        let idx = self.sorted_ords.binary_search(&term_ord).ok()?;
        let term_bytes = &self.bytes[self.offsets[idx]..self.offsets[idx + 1]];
        // Text fast fields are valid UTF-8 by construction.
        std::str::from_utf8(term_bytes).ok()
    }
}

/// The [`ValueSource`] produced by [`JitExprValueSourceProvider`] for one segment.
///
/// # Multivalued inputs
///
/// Each input takes the first value of the document, and a document without a value for an input
/// gets a null for that input. The expression is therefore evaluated exactly once per document,
/// and a document has at most one value: the other values of multivalued inputs are ignored.
///
/// Why: aggregating each of the values of a multivalued input would require evaluating the
/// cartesian product of the values of the inputs, whose size explodes with several multivalued
/// inputs.
pub(crate) struct JitExprValueSource {
    compiled: CompiledFnCtx,
    result_type: VarType,
    column_type: ColumnType,
    /// Inputs in the order of the compiled function inputs. `None` for unbound inputs.
    inputs: Vec<Option<InputColumn>>,
    /// One block per input, in the same order as `inputs`.
    input_blocks: Vec<InputBlock>,
    /// One cursor per input block. See `InputBlock::first_value_pos`.
    cursors: Vec<usize>,
    /// The term ords of `Str` results, attributed in first-seen order.
    term_dict: DynamicTermDict,
}

impl std::fmt::Debug for JitExprValueSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JitExprValueSource")
            .field("result_type", &self.result_type)
            .field("num_inputs", &self.inputs.len())
            .finish()
    }
}

/// Converts an input value from its monotonic `u64` mapping.
fn input_value<'a>(var_type: VarType, value: u64, terms: &'a BlockTerms) -> VariableValue<'a> {
    match var_type {
        VarType::Bool => VariableValue::from(bool::from_u64(value)),
        VarType::I64 => VariableValue::from(i64::from_u64(value)),
        VarType::U64 => VariableValue::from(value),
        VarType::F64 => VariableValue::from(f64::from_u64(value)),
        VarType::Str => terms
            .term(value)
            .map(VariableValue::from)
            .unwrap_or(VariableValue::none()),
        VarType::None => VariableValue::none(),
    }
}

impl ValueSource for JitExprValueSource {
    fn column_type(&self) -> ColumnType {
        self.column_type
    }

    fn load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
        _row_ids: &mut Vec<RowId>,
    ) -> Cardinality {
        values.clear();
        docids.clear();
        let JitExprValueSource {
            compiled,
            result_type,
            inputs,
            input_blocks,
            cursors,
            term_dict,
            ..
        } = self;
        let result_type = *result_type;
        for (input_opt, input_block) in inputs.iter().zip(input_blocks.iter_mut()) {
            if let Some(input) = input_opt {
                input_block.load(input, docs);
            }
        }
        cursors.fill(0);
        let input_blocks: &[InputBlock] = input_blocks;

        let mut args: Vec<VariableValue> = Vec::with_capacity(inputs.len());
        for (doc_pos, &doc) in docs.iter().enumerate() {
            args.clear();
            for (input_idx, input_opt) in inputs.iter().enumerate() {
                let Some(input) = input_opt else {
                    args.push(VariableValue::none());
                    continue;
                };
                let input_block = &input_blocks[input_idx];
                let Some(value_pos) =
                    input_block.first_value_pos(doc_pos, doc, &mut cursors[input_idx])
                else {
                    args.push(VariableValue::none());
                    continue;
                };
                args.push(input_value(
                    input.var_type,
                    input_block.values[value_pos],
                    &input_block.terms,
                ));
            }
            // SAFETY: the arguments follow the compiled inputs and their types.
            let result: VariableValue = unsafe { compiled.call(&args) };
            // SAFETY: `result` is of type `result_type`.
            if let Some(value) = unsafe { encode_result(result, result_type, term_dict) } {
                values.push(value);
                docids.push(doc);
            }
        }
        Cardinality::Optional
    }

    fn term_dictionary(&self) -> Option<&dyn ValueSourceDictionary> {
        if self.column_type != ColumnType::Str {
            return None;
        }
        Some(&self.term_dict)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::{json, Value};

    use super::*;
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::{AggContextParams, AggregationCollector, ValueSourceRegistry};
    use crate::query::AllQuery;
    use crate::schema::{Schema, FAST, STRING};
    use crate::Index;

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

    /// Two segments. `number` is a u64 field, `price` an f64 field, `delta` an i64 field, `flag` a
    /// bool field and `label` a text field. The last document of each segment has no value.
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
            json!({ "s": { "stats": { "field": "doubled" } } }),
        );
        // Documents without `number` evaluate to null, and have no value.
        assert_eq!(result["s"]["count"], 3);
        assert_eq!(result["s"]["sum"], 12.0);
        assert_eq!(result["s"]["min"], 2.0);
        assert_eq!(result["s"]["max"], 6.0);
    }

    #[test]
    fn test_histogram_and_range_over_calculated_field() {
        let index = create_index();
        let calculated_fields = [("shifted", "(ADD price 1.0f64)")];
        let result = run_agg(
            &index,
            &calculated_fields,
            json!({
                "h": { "histogram": { "field": "shifted", "interval": 5.0 } },
                "r": { "range": { "field": "shifted", "ranges": [{ "to": 0.0 }, { "from": 0.0 }] } }
            }),
        );
        let histogram_buckets = result["h"]["buckets"].as_array().unwrap();
        let histogram_counts: Vec<(f64, u64)> = histogram_buckets
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
    /// field itself: this checks the monotonic mapping of each type.
    #[test]
    fn test_identity_matches_fast_field_for_each_type() {
        let index = create_index();
        for (field, agg_kinds) in [
            ("number", &["terms", "stats"][..]),
            ("price", &["terms", "stats"][..]),
            ("delta", &["terms", "stats"][..]),
            ("flag", &["terms"][..]),
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
    fn test_constant_calculated_field() {
        let index = create_index();
        let result = run_agg(
            &index,
            &[("three", "(ADD 1u64 2u64)")],
            json!({ "s": { "stats": { "field": "three" } } }),
        );
        assert_eq!(result["s"]["count"], 5);
        assert_eq!(result["s"]["sum"], 15.0);
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

    fn terms_doc_counts(result: &Value, agg_name: &str) -> Vec<(Value, u64)> {
        result[agg_name]["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bucket| (bucket["key"].clone(), bucket["doc_count"].as_u64().unwrap()))
            .collect()
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

        // Each input takes the first value of the document. The document without `number` is
        // null.
        let result = run_agg(
            &index,
            &[("sum", "(ADD number other)")],
            json!({
                "s": { "stats": { "field": "sum" } },
                "t": { "terms": { "field": "sum", "order": { "_count": "desc" } } }
            }),
        );
        assert_eq!(result["s"]["count"], 2);
        assert_eq!(result["s"]["sum"], 11.0 + 33.0);
        let mut doc_counts = terms_doc_counts(&result, "t");
        doc_counts.sort_by_key(|(key, _)| key.as_f64().unwrap() as u64);
        assert_eq!(doc_counts, vec![(json!(11), 1), (json!(33), 1)]);

        // A document without a value for an input is evaluated with null.
        let result = run_agg(
            &index,
            &[("is_null", "(IS_NULL number)")],
            json!({ "t": { "terms": { "field": "is_null" } } }),
        );
        // Bool keys are serialized as 0 / 1, like for a bool fast field.
        let mut doc_counts = terms_doc_counts(&result, "t");
        doc_counts.sort_by_key(|(key, _)| key.to_string());
        assert_eq!(doc_counts, vec![(json!(0), 2), (json!(1), 1)]);
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
        // A multivalued document: only its first value, `a`, is used.
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
        let buckets = result["t"]["buckets"].as_array().unwrap();
        let keys_counts_sums: Vec<(&str, u64, f64)> = buckets
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
    fn test_identity_text_field_matches_fast_field() {
        let index = create_index();
        let aggs_on = |field_name: &str| {
            json!({
                "terms": { "terms": { "field": field_name } },
                "cardinality": { "cardinality": { "field": field_name } }
            })
        };
        let expected = run_agg(&index, &[], aggs_on("label"));
        let actual = run_agg(&index, &[("calculated", "label")], aggs_on("calculated"));
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_cardinality_over_calculated_text_field() {
        let index = create_label_index();
        let result = run_agg(
            &index,
            &[("first_letter", r#"(LEFT (UPPER label) 1u64)"#)],
            json!({ "c": { "cardinality": { "field": "first_letter" } } }),
        );
        assert_eq!(result["c"]["value"], 3.0);
    }

    #[test]
    fn test_constant_text_calculated_field() {
        let index = create_label_index();
        let result = run_agg(
            &index,
            &[("constant", r#""x""#)],
            json!({ "t": { "terms": { "field": "constant" } } }),
        );
        assert_eq!(terms_doc_counts(&result, "t"), vec![(json!("x"), 9)]);
    }

    #[test]
    fn test_unsupported_terms_options_on_calculated_text_field() {
        let index = create_label_index();
        let calculated_fields = [("upper", "(UPPER label)")];
        for (terms_req, expected_message) in [
            (
                json!({ "field": "upper", "order": { "_key": "asc" } }),
                "ordering by `_key`",
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
}
