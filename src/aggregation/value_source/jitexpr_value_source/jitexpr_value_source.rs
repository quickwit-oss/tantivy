//! The value sources of a jitexpr expression on a segment.

use columnar::{Cardinality, ColumnIndex, ColumnType, MonotonicallyMappableToU64, RowId};
use common::VersatileBuffer;
use jitexpr::compile::CompiledFnCtx;
use jitexpr::types::{VarType, VariableValue};

use super::dynamic_term_dictionary::DynamicTermDictionary;
use super::input_column::InputColumn;
use crate::aggregation::f64_from_fastfield_u64;
use crate::aggregation::value_source::{coerce_to_f64, ValueSource, ValueSourceDictionary};
use crate::DocId;

/// The value source of a jitexpr expression on a segment.
///
/// Each input takes the first value of the doc. A doc without a value for an input gets null for
/// that input. The expression is evaluated once per doc, and a null result means that the doc has
/// no value. A doc therefore has at most one value.
///
/// Why the first value only: using all of the values of multivalued inputs would require
/// evaluating the cartesian product of the values of the inputs, which explodes with several
/// multivalued inputs.
///
/// Values use the monotonic `u64` mapping of fast fields. `Str` results are interned in a
/// [`DynamicTermDictionary`] owned by the source: their values are term ords in first-seen order.
pub(crate) struct JitExprValueSource {
    compiled_fn: CompiledFnCtx,
    /// The column type of the result type of `compiled_fn`.
    column_type: ColumnType,
    /// Invariant: `inputs[i]` is bound to `compiled_fn.inputs()[i]`, and has the same type.
    inputs: Vec<InputColumn>,
    /// Interns the `Str` results.
    term_dictionary: DynamicTermDictionary,
    /// Reusable allocation for the arguments of `compiled_fn`, lent as a `Vec<VariableValue>` for
    /// each block.
    args: VersatileBuffer<VariableValue<'static>>,
}

impl JitExprValueSource {
    /// Precondition: `column_type` is the column type of the result type of `compiled_fn`.
    ///
    /// Panics if `inputs` do not match the inputs of `compiled_fn`.
    pub(super) fn new(
        compiled_fn: CompiledFnCtx,
        column_type: ColumnType,
        inputs: Vec<InputColumn>,
    ) -> JitExprValueSource {
        // Evaluating the function with inputs of the wrong type is undefined behavior.
        assert_eq!(compiled_fn.inputs().len(), inputs.len());
        for (compiled_input, input) in compiled_fn.inputs().iter().zip(&inputs) {
            assert_eq!(compiled_input.r#type, input.var_type());
        }
        let num_inputs = inputs.len();
        JitExprValueSource {
            compiled_fn,
            column_type,
            inputs,
            term_dictionary: DynamicTermDictionary::default(),
            args: VersatileBuffer::with_capacity(num_inputs),
        }
    }

    /// Evaluates the expression on `doc`, without going through `load_block`.
    ///
    /// Returns the value in the monotonic `u64` mapping (e.g. `true` is 1), or `None` if the
    /// result is null.
    ///
    /// Panics if the dictionary of a text input is corrupted.
    pub(crate) fn eval_single_doc(&mut self, doc: DocId) -> Option<u64> {
        let JitExprValueSource {
            compiled_fn,
            inputs,
            term_dictionary,
            args,
            ..
        } = self;
        let result_type: VarType = compiled_fn.result_type();
        let mut args = args.borrow::<VariableValue>();
        // Each input is borrowed once: the values can borrow their input.
        for input in inputs.iter_mut() {
            args.push(input.value_for_doc(doc));
        }
        // SAFETY: `args` follow the compiled inputs, with their types (see `new`). The strings
        // are borrowed from `inputs`, which are not modified during the call.
        let result: VariableValue = unsafe { compiled_fn.call(&args) };
        // SAFETY: `result` has the result type of the compiled function.
        unsafe { encode_result(result, result_type, term_dictionary) }
    }

    /// Returns the variable name and the column index of each input.
    pub(crate) fn input_column_indexes(&self) -> impl Iterator<Item = (&str, &ColumnIndex)> {
        self.compiled_fn
            .inputs()
            .iter()
            .zip(&self.inputs)
            .map(|(compiled_input, input)| {
                (compiled_input.variable_name.as_ref(), input.column_index())
            })
    }
}

impl std::fmt::Debug for JitExprValueSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JitExprValueSource")
            .field("column_type", &self.column_type)
            .field("num_inputs", &self.inputs.len())
            .finish()
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
        for input in &mut self.inputs {
            input.load_block(docs);
        }
        let JitExprValueSource {
            compiled_fn,
            inputs,
            term_dictionary,
            args,
            ..
        } = self;
        let result_type: VarType = compiled_fn.result_type();
        let mut args = args.borrow::<VariableValue>();
        for (doc_pos, &doc) in docs.iter().enumerate() {
            args.clear();
            for input in inputs.iter() {
                args.push(input.value(doc_pos));
            }
            // SAFETY: `args` follow the compiled inputs, with their types (see `new`). The strings
            // are borrowed from `inputs`, which are not modified during the call.
            let result: VariableValue = unsafe { compiled_fn.call(&args) };
            // SAFETY: `result` has the result type of the compiled function.
            let value_opt: Option<u64> =
                unsafe { encode_result(result, result_type, term_dictionary) };
            if let Some(value) = value_opt {
                values.push(value);
                docids.push(doc);
            }
        }
        if values.len() == docs.len() {
            // Each doc has exactly one value, in the order of `docs`.
            Cardinality::Full
        } else {
            Cardinality::Optional
        }
    }

    fn to_f64(mut self: Box<Self>) -> Box<dyn ValueSource> {
        if !matches!(self.column_type, ColumnType::U64 | ColumnType::I64) {
            return self;
        }
        // The expression wrapped in a coercion to `f64`, compiled once per compiled function.
        let coerced_fn = match self.compiled_fn.compiled_fn().coerce_result_to_f64() {
            Ok(coerced_fn) => coerced_fn,
            // Should not happen for an `i64` / `u64` result. We still have a correct (slower)
            // fallback: converting the values after evaluation.
            Err(_) => return coerce_to_f64(self),
        };
        // The coerced function has the same inputs: `self.inputs` stay bound to it.
        debug_assert!(coerced_fn.inputs() == self.compiled_fn.inputs());
        self.compiled_fn = coerced_fn.context();
        self.column_type = ColumnType::F64;
        self
    }

    fn term_dictionary(&self) -> Option<&dyn ValueSourceDictionary> {
        if self.column_type != ColumnType::Str {
            return None;
        }
        Some(&self.term_dictionary)
    }
}

/// A source with the same value for all of the docs.
#[derive(Debug)]
pub(crate) struct ConstantValueSource {
    column_type: ColumnType,
    /// In the monotonic `u64` mapping. For `Str`, the term ord of the only term of
    /// `term_dictionary`.
    value: u64,
    term_dictionary: DynamicTermDictionary,
}

impl ConstantValueSource {
    /// Evaluates a compiled function without any input. Returns `None` if the result is null.
    ///
    /// Precondition: `column_type` is the column type of the result type of `compiled_fn`.
    pub(super) fn evaluate(
        mut compiled_fn: CompiledFnCtx,
        column_type: ColumnType,
    ) -> Option<ConstantValueSource> {
        assert!(compiled_fn.inputs().is_empty());
        let result_type: VarType = compiled_fn.result_type();
        let mut term_dictionary = DynamicTermDictionary::default();
        // SAFETY: the function has no input.
        let result: VariableValue = unsafe { compiled_fn.call(&[]) };
        // SAFETY: `result` has the result type of the compiled function.
        let value = unsafe { encode_result(result, result_type, &mut term_dictionary) }?;
        Some(ConstantValueSource {
            column_type,
            value,
            term_dictionary,
        })
    }

    /// Returns the value of all of the docs, in the monotonic `u64` mapping.
    pub(crate) fn value(&self) -> u64 {
        self.value
    }
}

impl ValueSource for ConstantValueSource {
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

    fn to_f64(mut self: Box<Self>) -> Box<dyn ValueSource> {
        if matches!(self.column_type, ColumnType::U64 | ColumnType::I64) {
            self.value = f64_from_fastfield_u64(self.value, self.column_type).to_u64();
            self.column_type = ColumnType::F64;
        }
        self
    }

    fn term_dictionary(&self) -> Option<&dyn ValueSourceDictionary> {
        if self.column_type != ColumnType::Str {
            return None;
        }
        Some(&self.term_dictionary)
    }
}

/// Encodes a result in the monotonic `u64` mapping. Returns `None` for a null result.
///
/// `Str` results are interned in `term_dictionary`, and encoded as their term ord.
///
/// # Safety
///
/// `result` must have the type `result_type`.
unsafe fn encode_result(
    result: VariableValue,
    result_type: VarType,
    term_dictionary: &mut DynamicTermDictionary,
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
                .map(|term: &str| term_dictionary.intern(term.as_bytes())),
            VarType::None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregation::jitexpr_value_source::{
        JitExprSegmentSource, JitExprValueSourceProvider,
    };
    use crate::aggregation::ValueSourceProvider;
    use crate::schema::{Schema, FAST, STRING};
    use crate::Index;

    /// One segment: doc `i` has `number = i` and `label = labels[i]`, except for the docs of
    /// `null_docs`, which have neither.
    fn create_index(labels: &[&str], null_docs: &[usize]) -> Index {
        let mut schema_builder = Schema::builder();
        let number = schema_builder.add_u64_field("number", FAST);
        let label = schema_builder.add_text_field("label", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests().unwrap();
        for (doc_pos, label_value) in labels.iter().enumerate() {
            if null_docs.contains(&doc_pos) {
                writer.add_document(doc!()).unwrap();
            } else {
                writer
                    .add_document(doc!(number => doc_pos as u64, label => *label_value))
                    .unwrap();
            }
        }
        writer.commit().unwrap();
        index
    }

    fn open_source(index: &Index, expression: &str) -> Box<dyn ValueSource> {
        let expression = jitexpr::ast::deserialize(expression).unwrap();
        let provider = JitExprValueSourceProvider::new(expression).unwrap();
        let searcher = index.reader().unwrap().searcher();
        provider
            .for_segment(searcher.segment_reader(0), None)
            .unwrap()
            .expect("a jitexpr field always returns a source")
    }

    /// Returns the values and docids of a block. `docids` is only filled for a non-full block.
    fn load_block(
        source: &mut dyn ValueSource,
        docs: &[DocId],
    ) -> (Cardinality, Vec<u64>, Vec<DocId>) {
        let mut values = Vec::new();
        let mut docids = Vec::new();
        let mut row_ids = Vec::new();
        let cardinality = source.load_block(docs, &mut values, &mut docids, &mut row_ids);
        if cardinality.is_full() {
            docids.clear();
        }
        (cardinality, values, docids)
    }

    fn terms(source: &dyn ValueSource, term_ords: &[u64]) -> Vec<String> {
        let mut terms = Vec::new();
        let all_found = source
            .term_dictionary()
            .unwrap()
            .sorted_ords_to_term_cb(term_ords, &mut |term| {
                terms.push(String::from_utf8(term.to_vec()).unwrap())
            })
            .unwrap();
        assert!(all_found);
        terms
    }

    #[test]
    fn test_block_cardinality() {
        let index = create_index(&["a", "b", "c", "d"], &[2]);
        let mut source = open_source(&index, "(ADD number 10u64)");
        assert_eq!(source.column_type(), ColumnType::U64);
        assert!(source.term_dictionary().is_none());

        // All of the docs have a value.
        let (cardinality, values, _) = load_block(&mut *source, &[0, 1, 3]);
        assert_eq!(cardinality, Cardinality::Full);
        assert_eq!(values, vec![10, 11, 13]);

        // Doc 2 has no value. The buffers of the previous block are cleared.
        let (cardinality, values, docids) = load_block(&mut *source, &[1, 2, 3]);
        assert_eq!(cardinality, Cardinality::Optional);
        assert_eq!(values, vec![11, 13]);
        assert_eq!(docids, vec![1, 3]);

        let (cardinality, values, docids) = load_block(&mut *source, &[2]);
        assert_eq!(cardinality, Cardinality::Optional);
        assert!(values.is_empty());
        assert!(docids.is_empty());
    }

    #[test]
    fn test_text_term_ords_are_stable_across_blocks() {
        let index = create_index(&["b", "a", "b", "c", "a"], &[]);
        let mut source = open_source(&index, "(UPPER label)");
        assert_eq!(source.column_type(), ColumnType::Str);

        let (cardinality, first_block_ords, _) = load_block(&mut *source, &[0, 1, 2]);
        assert_eq!(cardinality, Cardinality::Full);
        // Ords are assigned in first-seen order.
        assert_eq!(first_block_ords, vec![0, 1, 0]);

        let (_, second_block_ords, _) = load_block(&mut *source, &[3, 4]);
        assert_eq!(second_block_ords, vec![2, 1]);
        assert_eq!(source.term_dictionary().unwrap().num_terms(), 3);
        assert_eq!(terms(&*source, &[0, 1, 2]), vec!["B", "A", "C"]);
    }

    #[test]
    fn test_constant_source() {
        let index = create_index(&["a", "b"], &[]);
        let mut source = open_source(&index, r#"(UPPER "x")"#);
        assert_eq!(source.column_type(), ColumnType::Str);
        let (cardinality, values, _) = load_block(&mut *source, &[0, 1]);
        assert_eq!(cardinality, Cardinality::Full);
        assert_eq!(values, vec![0, 0]);
        assert_eq!(terms(&*source, &[0]), vec!["X"]);

        // A null constant is a source without any value.
        let mut source = open_source(&index, "(ADD absent 1u64)");
        let (cardinality, values, docids) = load_block(&mut *source, &[0, 1]);
        assert!(!cardinality.is_full());
        assert!(values.is_empty());
        assert!(docids.is_empty());
    }

    #[test]
    fn test_eval_single_doc_matches_load_block() {
        let index = create_index(&["a", "b", "c", "b"], &[2]);
        let searcher = index.reader().unwrap().searcher();
        let docs: Vec<DocId> = vec![0, 1, 2, 3];
        let expressions = [
            "(ADD number 10u64)",
            "(UPPER label)",
            r#"(EQ label "b")"#,
            // Null for the docs whose label does not match.
            r#"(REGEXP_EXTRACT label "(b)" 1u64)"#,
        ];
        for expression in expressions {
            let provider =
                JitExprValueSourceProvider::new(jitexpr::ast::deserialize(expression).unwrap())
                    .unwrap();
            let segment_source = provider
                .open_segment(searcher.segment_reader(0), None)
                .unwrap();
            let JitExprSegmentSource::Variable(mut source) = segment_source else {
                panic!("expected a variable source for `{expression}`");
            };
            let (cardinality, block_values, block_docids) = load_block(&mut source, &docs);
            let mut expected: Vec<Option<u64>> = vec![None; docs.len()];
            if cardinality.is_full() {
                for (doc_pos, value) in block_values.iter().enumerate() {
                    expected[doc_pos] = Some(*value);
                }
            } else {
                for (doc, value) in block_docids.iter().zip(&block_values) {
                    expected[*doc as usize] = Some(*value);
                }
            }
            // `Str` results are interned in first-seen order: the block path saw them first.
            let single_doc_values: Vec<Option<u64>> = docs
                .iter()
                .map(|&doc| source.eval_single_doc(doc))
                .collect();
            assert_eq!(single_doc_values, expected, "expression `{expression}`");
            assert_eq!(single_doc_values[2], None, "doc 2 has no value");
        }
    }

    #[test]
    fn test_to_f64() {
        let index = create_index(&["a", "b", "c"], &[1]);
        let searcher = index.reader().unwrap().searcher();
        let docs: Vec<DocId> = vec![0, 1, 2];
        for expression in ["(ADD number 10u64)", "(ADD 1i64 2i64)"] {
            let provider =
                JitExprValueSourceProvider::new(jitexpr::ast::deserialize(expression).unwrap())
                    .unwrap();
            let source: Box<dyn ValueSource> = provider
                .for_segment(searcher.segment_reader(0), None)
                .unwrap()
                .expect("a jitexpr field always returns a source");
            assert_ne!(source.column_type(), ColumnType::F64);
            let mut source = source.to_f64();
            assert_eq!(source.column_type(), ColumnType::F64, "`{expression}`");
            let (_, values, docids) = load_block(&mut *source, &docs);
            let decoded: Vec<f64> = values.iter().map(|value| f64::from_u64(*value)).collect();
            if expression == "(ADD 1i64 2i64)" {
                // A constant: all of the docs have the value.
                assert_eq!(decoded, vec![3.0, 3.0, 3.0]);
            } else {
                // Doc 1 has no `number`.
                assert_eq!(docids, vec![0, 2]);
                assert_eq!(decoded, vec![10.0, 12.0]);
            }
        }
    }
}
