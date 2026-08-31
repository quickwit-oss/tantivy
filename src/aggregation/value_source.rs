use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use columnar::{Cardinality, Column, ColumnType, MonotonicallyMappableToU64, RowId};
use jitexpr::ast::{infer_types, Function, UntypedExpr};
use jitexpr::compile::{compile, CompiledFn, CompiledFnCtx};
use jitexpr::types::{VarType, VariablePrimitive, VariablePrimitiveOpt, VariableValue};

use super::block_accessor::BlockValueSource;
use super::CalculatedColumns;
use crate::{DocId, SegmentReader};

/// Whether the encoded values produced by a source preserve their semantic ordering.
///
/// Physical columns use Tantivy's monotonic encoding (or dictionary ordinals) and therefore
/// preserve semantic ordering. Calculated sources may use private ordinals whose ordering has no
/// semantic meaning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) enum OrdinalOrdering {
    Semantic,
    Unordered,
}

/// Immutable properties collectors may use to select safe fast paths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ValueSourceCapabilities {
    output_type: ColumnType,
    cardinality: Cardinality,
    encoded_bounds: Option<(u64, u64)>,
    ordinal_ordering: OrdinalOrdering,
    physical: bool,
}

#[allow(dead_code)]
impl ValueSourceCapabilities {
    pub(crate) fn output_type(self) -> ColumnType {
        self.output_type
    }

    pub(crate) fn cardinality(self) -> Cardinality {
        self.cardinality
    }

    pub(crate) fn encoded_bounds(self) -> Option<(u64, u64)> {
        self.encoded_bounds
    }

    pub(crate) fn ordinal_ordering(self) -> OrdinalOrdering {
        self.ordinal_ordering
    }

    pub(crate) fn is_physical(self) -> bool {
        self.physical
    }
}

/// Immutable and cheaply clonable recipe for a per-collector value source.
#[derive(Clone)]
pub(crate) enum SegmentValueSourcePlan {
    Physical(PhysicalValueSourcePlan),
    CalculatedNumber(CalculatedNumberValueSourcePlan),
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalValueSourcePlan {
    column: Column<u64>,
    capabilities: ValueSourceCapabilities,
}

#[derive(Clone)]
pub(crate) struct CalculatedNumberValueSourcePlan {
    name: String,
    compiled: Arc<CompiledFn>,
    bindings: Arc<[CalculatedInputBinding]>,
    capabilities: ValueSourceCapabilities,
}

#[derive(Clone, Debug)]
struct CalculatedInputBinding {
    variable_type: VarType,
    column: Option<Column<u64>>,
}

impl fmt::Debug for SegmentValueSourcePlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Physical(plan) => formatter.debug_tuple("Physical").field(plan).finish(),
            Self::CalculatedNumber(plan) => formatter
                .debug_struct("CalculatedNumber")
                .field("name", &plan.name)
                .field("bindings", &plan.bindings)
                .field("capabilities", &plan.capabilities)
                .finish_non_exhaustive(),
        }
    }
}

impl SegmentValueSourcePlan {
    pub(crate) fn physical(column: Column<u64>, output_type: ColumnType) -> Self {
        let encoded_bounds =
            (column.values.num_vals() != 0).then(|| (column.min_value(), column.max_value()));
        let capabilities = ValueSourceCapabilities {
            output_type,
            cardinality: column.get_cardinality(),
            encoded_bounds,
            ordinal_ordering: OrdinalOrdering::Semantic,
            physical: true,
        };
        Self::Physical(PhysicalValueSourcePlan {
            column,
            capabilities,
        })
    }

    pub(crate) fn calculated_number(
        name: &str,
        expression: &UntypedExpr,
        reader: &SegmentReader,
        calculated_columns: &CalculatedColumns,
    ) -> crate::Result<Self> {
        // `CompiledFn` does not expose its output type. Make the F64 output contract explicit in
        // the expression passed to inference and compilation, while retaining the original
        // expression in the public registry so later source kinds can apply their own contract.
        let f64_expression =
            Function::Add.call_untyped_expr(vec![expression.clone(), UntypedExpr::literal(0.0f64)]);
        let inferred = infer_types(&f64_expression).map_err(|error| {
            crate::TantivyError::InvalidArgument(format!(
                "Failed to infer types for calculated column {name:?}: {error}"
            ))
        })?;

        let mut variable_names: Vec<&str> = inferred.keys().copied().collect();
        variable_names.sort_unstable();
        let mut variable_types: HashMap<&str, VarType> = HashMap::new();
        let mut selected_bindings = HashMap::new();
        for variable_name in variable_names {
            if calculated_columns.contains(variable_name) {
                return Err(crate::TantivyError::InvalidArgument(format!(
                    "Calculated column {name:?} cannot depend on calculated column \
                     {variable_name:?}"
                )));
            }
            let accepted_types = &inferred[variable_name];
            let binding = select_numeric_binding(reader, variable_name, accepted_types, name)?;
            variable_types.insert(variable_name, binding.variable_type);
            selected_bindings.insert(variable_name, binding);
        }
        let compiled: Arc<CompiledFn> =
            compile(&f64_expression, &variable_types).map_err(|error| {
                crate::TantivyError::InvalidArgument(format!(
                    "Failed to compile calculated column {name:?} as an F64 expression: {error}"
                ))
            })?;

        let mut bindings = Vec::with_capacity(compiled.inputs.len());
        for input in compiled.inputs.iter() {
            let binding = selected_bindings
                .remove(input.variable_name.as_ref())
                .ok_or_else(|| {
                    crate::TantivyError::InternalError(format!(
                        "Calculated column {name:?} compiler returned unknown input {:?}",
                        input.variable_name
                    ))
                })?;
            if input.r#type != binding.variable_type {
                return Err(crate::TantivyError::InternalError(format!(
                    "Calculated column {name:?} compiler changed input {:?} from {:?} to {:?}",
                    input.variable_name, binding.variable_type, input.r#type
                )));
            }
            bindings.push(binding);
        }
        Ok(Self::CalculatedNumber(CalculatedNumberValueSourcePlan {
            name: name.to_string(),
            compiled,
            bindings: bindings.into(),
            capabilities: ValueSourceCapabilities {
                output_type: ColumnType::F64,
                cardinality: Cardinality::Optional,
                encoded_bounds: None,
                ordinal_ordering: OrdinalOrdering::Semantic,
                physical: false,
            },
        }))
    }

    pub(crate) fn capabilities(&self) -> ValueSourceCapabilities {
        match self {
            Self::Physical(plan) => plan.capabilities,
            Self::CalculatedNumber(plan) => plan.capabilities,
        }
    }

    /// Returns the physical column when this plan can participate in a physical-only fast path.
    pub(crate) fn physical_column(&self) -> Option<&Column<u64>> {
        match self {
            Self::Physical(plan) => Some(&plan.column),
            Self::CalculatedNumber(_) => None,
        }
    }

    /// Creates runtime state owned exclusively by one segment collector.
    pub(crate) fn instantiate(&self) -> SegmentValueSource {
        match self {
            Self::Physical(plan) => SegmentValueSource::Physical(PhysicalValueSource {
                column: plan.column.clone(),
                capabilities: plan.capabilities,
            }),
            Self::CalculatedNumber(plan) => {
                SegmentValueSource::CalculatedNumber(CalculatedNumberValueSource {
                    name: plan.name.clone(),
                    context: CompiledFnCtx::new(Arc::clone(&plan.compiled)),
                    bindings: Arc::clone(&plan.bindings),
                    input_values: Vec::with_capacity(plan.bindings.len()),
                    input_scratch: vec![Vec::new(); plan.bindings.len()],
                    capabilities: plan.capabilities,
                })
            }
        }
    }
}

fn select_numeric_binding(
    reader: &SegmentReader,
    variable_name: &str,
    accepted_types: &jitexpr::ast::InferredTypeSet,
    calculated_name: &str,
) -> crate::Result<CalculatedInputBinding> {
    // Keep this priority stable: lenient JSON paths can expose several compatible numerical
    // columns, and every segment must make the same choice for the same available types.
    let candidates = [
        (ColumnType::F64, VarType::F64),
        (ColumnType::U64, VarType::U64),
        (ColumnType::I64, VarType::I64),
    ];
    for (column_type, variable_type) in candidates {
        if !accepted_types.contains(variable_type) {
            continue;
        }
        if let Some((column, actual_type)) = reader
            .fast_fields()
            .u64_lenient_for_type(Some(&[column_type]), variable_name)?
        {
            debug_assert_eq!(actual_type, column_type);
            if column.get_cardinality().is_multivalue() {
                return Err(crate::TantivyError::InvalidArgument(format!(
                    "Calculated column {calculated_name:?} input {variable_name:?} is \
                     multivalued; calculated numerical columns require at most one value per \
                     document"
                )));
            }
            return Ok(CalculatedInputBinding {
                variable_type,
                column: Some(column),
            });
        }
    }

    let available_columns = reader.fast_fields().dynamic_column_handles(variable_name)?;
    if available_columns.is_empty() {
        return Ok(CalculatedInputBinding {
            variable_type: VarType::None,
            column: None,
        });
    }

    let mut available_types: Vec<ColumnType> = available_columns
        .iter()
        .map(|column| column.column_type())
        .collect();
    available_types.sort_unstable();
    available_types.dedup();
    Err(crate::TantivyError::InvalidArgument(format!(
        "Calculated column {calculated_name:?} input {variable_name:?} has incompatible physical \
         column types {available_types:?}"
    )))
}

pub(crate) fn validate_calculated_column_names(
    reader: &SegmentReader,
    calculated_columns: &CalculatedColumns,
) -> crate::Result<()> {
    for (name, _) in calculated_columns.iter() {
        let is_schema_field_or_json_path = reader.schema().find_field(name).is_some();
        let is_physical_path = !reader
            .fast_fields()
            .dynamic_column_handles(name)?
            .is_empty();
        if is_schema_field_or_json_path || is_physical_path {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "Calculated column {name:?} collides with a physical field or JSON path"
            )));
        }
    }
    Ok(())
}

pub(crate) fn resolve_segment_value_source(
    reader: &SegmentReader,
    field_name: &str,
    allowed_column_types: Option<&[ColumnType]>,
    calculated_columns: &CalculatedColumns,
    calculated_source_plans: &mut rustc_hash::FxHashMap<String, SegmentValueSourcePlan>,
) -> crate::Result<(SegmentValueSourcePlan, ColumnType)> {
    if let Some(expression) = calculated_columns.get(field_name) {
        if !allowed_column_types
            .map(|types| types.contains(&ColumnType::F64))
            .unwrap_or(true)
        {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "Calculated numerical column {field_name:?} is not supported by this aggregation"
            )));
        }
        if let Some(plan) = calculated_source_plans.get(field_name) {
            return Ok((plan.clone(), ColumnType::F64));
        }
        let plan = SegmentValueSourcePlan::calculated_number(
            field_name,
            expression,
            reader,
            calculated_columns,
        )?;
        calculated_source_plans.insert(field_name.to_string(), plan.clone());
        return Ok((plan, ColumnType::F64));
    }

    let (column, column_type) =
        super::accessor_helpers::get_ff_reader(reader, field_name, allowed_column_types)?;
    Ok((
        SegmentValueSourcePlan::physical(column, column_type),
        column_type,
    ))
}

/// Stateful value source owned by one segment collector.
///
/// This type intentionally does not implement `Clone`: future calculated variants carry mutable
/// execution contexts and scratch buffers which must never be shared between collectors.
pub(crate) enum SegmentValueSource {
    Physical(PhysicalValueSource),
    CalculatedNumber(CalculatedNumberValueSource),
}

#[derive(Debug)]
pub(crate) struct PhysicalValueSource {
    column: Column<u64>,
    capabilities: ValueSourceCapabilities,
}

pub(crate) struct CalculatedNumberValueSource {
    name: String,
    context: CompiledFnCtx,
    bindings: Arc<[CalculatedInputBinding]>,
    input_values: Vec<VariableValue<'static>>,
    input_scratch: Vec<Vec<Option<u64>>>,
    capabilities: ValueSourceCapabilities,
}

impl fmt::Debug for SegmentValueSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Physical(source) => formatter.debug_tuple("Physical").field(source).finish(),
            Self::CalculatedNumber(source) => formatter
                .debug_struct("CalculatedNumber")
                .field("name", &source.name)
                .field("bindings", &source.bindings)
                .field("capabilities", &source.capabilities)
                .finish_non_exhaustive(),
        }
    }
}

#[allow(dead_code)]
impl SegmentValueSource {
    pub(crate) fn capabilities(&self) -> ValueSourceCapabilities {
        match self {
            Self::Physical(source) => source.capabilities,
            Self::CalculatedNumber(source) => source.capabilities,
        }
    }

    /// Returns the physical column when this runtime can participate in a physical-only fast path.
    pub(crate) fn physical_column(&self) -> Option<&Column<u64>> {
        match self {
            Self::Physical(source) => Some(&source.column),
            Self::CalculatedNumber(_) => None,
        }
    }
}

impl BlockValueSource for SegmentValueSource {
    fn load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
        row_ids: &mut Vec<RowId>,
    ) -> Cardinality {
        match self {
            Self::Physical(source) => source.column.load_block(docs, values, docids, row_ids),
            Self::CalculatedNumber(source) => source.load_block(docs, values, docids, row_ids),
        }
    }
}

impl CalculatedNumberValueSource {
    fn load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
        row_ids: &mut Vec<RowId>,
    ) -> Cardinality {
        values.clear();
        docids.clear();
        row_ids.clear();

        for (binding, scratch) in self.bindings.iter().zip(&mut self.input_scratch) {
            scratch.clear();
            scratch.resize(docs.len(), None);
            if let Some(column) = &binding.column {
                column.first_vals(docs, scratch);
            }
        }

        self.input_values.clear();
        self.input_values.resize_with(self.bindings.len(), || {
            nullable_primitive(VarType::None, None)
        });
        for (doc_offset, &doc) in docs.iter().enumerate() {
            for (input_idx, binding) in self.bindings.iter().enumerate() {
                self.input_values[input_idx] = nullable_primitive(
                    binding.variable_type,
                    self.input_scratch[input_idx][doc_offset],
                );
            }

            // Safety: bindings follow `CompiledFn::inputs`; each value uses the exact union member
            // matching the compiler-provided `VarType`; and numeric-plan normalization forces the
            // expression result to F64 before compilation.
            let output = unsafe { self.context.call(&self.input_values).as_f64() };
            // Aggregation requests reject non-finite literal/missing values. Calculated results use
            // the same usable-value policy by treating non-finite values as missing.
            if let Some(output) = output.filter(|output| output.is_finite()) {
                values.push(output.to_u64());
                docids.push(doc);
            }
        }
        Cardinality::Optional
    }
}

fn nullable_primitive(
    variable_type: VarType,
    encoded_value: Option<u64>,
) -> VariableValue<'static> {
    let is_present = encoded_value.is_some();
    let encoded_value = encoded_value.unwrap_or_default();
    let value = match variable_type {
        VarType::F64 => VariablePrimitive {
            float: f64::from_u64(encoded_value),
        },
        VarType::U64 => VariablePrimitive {
            int_u64: encoded_value,
        },
        VarType::I64 => VariablePrimitive {
            int_i64: i64::from_u64(encoded_value),
        },
        VarType::None => VariablePrimitive { float: 0.0 },
        other => unreachable!("unsupported calculated numeric input type {other:?}"),
    };
    VariableValue::from(VariablePrimitiveOpt { is_present, value })
}

#[cfg(test)]
mod tests {
    use columnar::column_index::{ColumnIndex, OptionalIndex};
    use columnar::column_values::{
        serialize_and_load_u64_based_column_values, ALL_U64_CODEC_TYPES,
    };
    use jitexpr::ast::Function;

    use super::*;
    use crate::aggregation::{CalculatedColumns, ColumnBlockAccessor};
    use crate::schema::{Schema, FAST};
    use crate::Index;

    fn optional_column() -> Column<u64> {
        let values = serialize_and_load_u64_based_column_values::<u64>(
            &&[10u64, 30][..],
            &ALL_U64_CODEC_TYPES,
        );
        Column {
            index: ColumnIndex::Optional(OptionalIndex::for_test(4, &[1, 3])),
            values,
        }
    }

    fn segment_reader(index: &Index) -> SegmentReader {
        index.reader().unwrap().searcher().segment_reader(0).clone()
    }

    fn calculated_plan(
        reader: &SegmentReader,
        name: &str,
        expression: UntypedExpr,
    ) -> crate::Result<SegmentValueSourcePlan> {
        let mut columns = CalculatedColumns::new();
        columns.register(name, expression)?;
        SegmentValueSourcePlan::calculated_number(
            name,
            columns.get(name).unwrap(),
            reader,
            &columns,
        )
    }

    fn decoded_values(accessor: &ColumnBlockAccessor) -> Vec<f64> {
        accessor
            .values()
            .iter()
            .map(|&value| f64::from_u64(value))
            .collect()
    }

    #[test]
    fn physical_plan_reports_capabilities_and_handle() {
        let plan = SegmentValueSourcePlan::physical(optional_column(), ColumnType::U64);
        let capabilities = plan.capabilities();
        assert_eq!(capabilities.output_type(), ColumnType::U64);
        assert_eq!(capabilities.cardinality(), Cardinality::Optional);
        assert_eq!(capabilities.encoded_bounds(), Some((10, 30)));
        assert_eq!(capabilities.ordinal_ordering(), OrdinalOrdering::Semantic);
        assert!(capabilities.is_physical());
        assert!(plan.physical_column().is_some());
    }

    #[test]
    fn empty_physical_plan_has_no_encoded_bounds() {
        let plan = SegmentValueSourcePlan::physical(Column::build_empty_column(3), ColumnType::F64);
        assert_eq!(plan.capabilities().encoded_bounds(), None);
    }

    #[test]
    fn cloned_plans_instantiate_independent_runtimes() {
        let plan = SegmentValueSourcePlan::physical(optional_column(), ColumnType::U64);
        let mut left = plan.instantiate();
        let mut right = plan.clone().instantiate();
        let mut left_block = ColumnBlockAccessor::default();
        let mut right_block = ColumnBlockAccessor::default();

        left_block.fetch_block(&[1], &mut left);
        right_block.fetch_block(&[3], &mut right);

        assert_eq!(left_block.values(), &[10]);
        assert_eq!(right_block.values(), &[30]);
        assert_eq!(left.capabilities(), right.capabilities());
        assert!(left.physical_column().is_some());
    }

    #[test]
    fn calculated_inputs_follow_compiler_order_and_convert_numeric_types() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let f64_field = schema_builder.add_f64_field("f64_input", FAST);
        let u64_field = schema_builder.add_u64_field("u64_input", FAST);
        let i64_field = schema_builder.add_i64_field("i64_input", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!(
            f64_field => 1.5f64,
            u64_field => 2u64,
            i64_field => 3i64,
        ))?;
        writer.commit()?;

        // Deliberately use a non-alphabetical expression order. The plan follows the compiler's
        // public input order, not inference-map iteration order.
        let expression = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("i64_input"),
            Function::Add.call_untyped_expr(vec![
                UntypedExpr::variable("f64_input"),
                UntypedExpr::variable("u64_input"),
            ]),
        ]);
        let plan = calculated_plan(&segment_reader(&index), "calculated", expression)?;
        let mut source = plan.instantiate();
        let mut accessor = ColumnBlockAccessor::default();
        accessor.fetch_block(&[0], &mut source);
        assert_eq!(decoded_values(&accessor), vec![6.5]);
        Ok(())
    }

    #[test]
    fn calculated_selection_prefers_f64_then_u64_then_i64() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!(json_field => json!({"mixed": 7u64})))?;
        writer.add_document(doc!(json_field => json!({"mixed": 2.5f64})))?;
        writer.commit()?;

        let plan = calculated_plan(
            &segment_reader(&index),
            "calculated",
            UntypedExpr::variable("json.mixed"),
        )?;
        let SegmentValueSourcePlan::CalculatedNumber(calculated_plan) = &plan else {
            panic!("expected calculated plan");
        };
        assert_eq!(calculated_plan.bindings[0].variable_type, VarType::F64);

        let mut source = plan.instantiate();
        let mut accessor = ColumnBlockAccessor::default();
        accessor.fetch_block(&[0, 1], &mut source);
        assert_eq!(accessor.docids(), &[0, 1]);
        assert_eq!(decoded_values(&accessor), vec![7.0, 2.5]);
        Ok(())
    }

    #[test]
    fn calculated_resolver_caches_compiled_plan_but_instantiates_fresh_contexts(
    ) -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_f64_field("value", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!(field => 2.0f64))?;
        writer.commit()?;
        let reader = segment_reader(&index);
        let mut columns = CalculatedColumns::new();
        columns.register("calculated", UntypedExpr::variable("value"))?;
        let mut cache = rustc_hash::FxHashMap::default();

        let (left_plan, _) = resolve_segment_value_source(
            &reader,
            "calculated",
            Some(&[ColumnType::F64]),
            &columns,
            &mut cache,
        )?;
        let (right_plan, _) = resolve_segment_value_source(
            &reader,
            "calculated",
            Some(&[ColumnType::F64]),
            &columns,
            &mut cache,
        )?;
        let (
            SegmentValueSourcePlan::CalculatedNumber(left_calculated_plan),
            SegmentValueSourcePlan::CalculatedNumber(right_calculated_plan),
        ) = (&left_plan, &right_plan)
        else {
            panic!("expected calculated plans");
        };
        assert!(Arc::ptr_eq(
            &left_calculated_plan.compiled,
            &right_calculated_plan.compiled
        ));
        assert_eq!(cache.len(), 1);

        let mut left = left_plan.instantiate();
        let mut right = right_plan.instantiate();
        let mut left_block = ColumnBlockAccessor::default();
        let mut right_block = ColumnBlockAccessor::default();
        left_block.fetch_block(&[0], &mut left);
        right_block.fetch_block(&[], &mut right);
        right_block.fetch_block(&[0], &mut right);
        assert_eq!(decoded_values(&left_block), vec![2.0]);
        assert_eq!(decoded_values(&right_block), vec![2.0]);
        Ok(())
    }

    #[test]
    fn calculated_multiple_optional_inputs_are_aligned_by_doc() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let left = schema_builder.add_f64_field("left", FAST);
        let right = schema_builder.add_f64_field("right", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!(left => 1.0f64))?;
        writer.add_document(doc!(right => 2.0f64))?;
        writer.add_document(doc!(left => 3.0f64, right => 4.0f64))?;
        writer.add_document(doc!())?;
        writer.commit()?;

        let expression = Function::Add.call_untyped_expr(vec![
            UntypedExpr::variable("left"),
            UntypedExpr::variable("right"),
        ]);
        let plan = calculated_plan(&segment_reader(&index), "calculated", expression)?;
        let mut source = plan.instantiate();
        let mut accessor = ColumnBlockAccessor::default();
        accessor.fetch_block(&[0, 1, 2, 3], &mut source);
        assert_eq!(accessor.docids(), &[2]);
        assert_eq!(decoded_values(&accessor), vec![7.0]);
        Ok(())
    }

    #[test]
    fn calculated_optional_absent_and_repeated_blocks_align_inputs() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_f64_field("optional", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!(field => 1.0f64))?;
        writer.add_document(doc!())?;
        writer.add_document(doc!(field => 3.0f64))?;
        writer.add_document(doc!())?;
        writer.commit()?;
        let reader = segment_reader(&index);
        let plan = calculated_plan(&reader, "calculated", UntypedExpr::variable("optional"))?;
        let mut left = plan.instantiate();
        let mut right = plan.clone().instantiate();
        let mut left_block = ColumnBlockAccessor::default();
        let mut right_block = ColumnBlockAccessor::default();

        left_block.fetch_block(&[], &mut left);
        assert!(left_block.values().is_empty());
        left_block.fetch_block(&[0, 2], &mut left);
        assert_eq!(decoded_values(&left_block), vec![1.0, 3.0]);
        right_block.fetch_block(&[1, 3], &mut right);
        assert!(right_block.values().is_empty());
        left_block.fetch_block(&[0, 1, 2], &mut left);
        let first = decoded_values(&left_block);
        left_block.fetch_block(&[0, 1, 2], &mut left);
        assert_eq!(decoded_values(&left_block), first);
        assert_eq!(left_block.docids(), &[0, 2]);

        let absent = calculated_plan(
            &reader,
            "absent_calculated",
            UntypedExpr::variable("not_in_the_segment"),
        )?;
        let mut absent = absent.instantiate();
        let mut values = vec![123];
        let mut docids = vec![123];
        let mut row_ids = vec![123];
        assert_eq!(
            absent.load_block(&[0, 1], &mut values, &mut docids, &mut row_ids),
            Cardinality::Optional
        );
        assert!(values.is_empty());
        assert!(docids.is_empty());
        assert!(row_ids.is_empty());
        Ok(())
    }

    #[test]
    fn calculated_rejects_multivalue_and_incompatible_inputs() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let numbers = schema_builder.add_f64_field("numbers", FAST);
        let text = schema_builder.add_text_field("text", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!(numbers => 1.0f64, numbers => 2.0f64, text => "x"))?;
        writer.commit()?;
        let reader = segment_reader(&index);

        let multivalue =
            calculated_plan(&reader, "multivalue", UntypedExpr::variable("numbers")).unwrap_err();
        assert!(multivalue.to_string().contains("multivalued"));
        let incompatible =
            calculated_plan(&reader, "incompatible", UntypedExpr::variable("text")).unwrap_err();
        assert!(incompatible.to_string().contains("incompatible"));
        Ok(())
    }

    #[test]
    fn calculated_non_finite_results_are_missing() -> crate::Result<()> {
        let schema = Schema::builder().build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!())?;
        writer.commit()?;
        let plan = calculated_plan(
            &segment_reader(&index),
            "non_finite",
            UntypedExpr::literal(f64::INFINITY),
        )?;
        let mut source = plan.instantiate();
        let mut accessor = ColumnBlockAccessor::default();
        accessor.fetch_block(&[0], &mut source);
        assert!(accessor.values().is_empty());

        let plan = calculated_plan(
            &segment_reader(&index),
            "nan",
            UntypedExpr::literal(f64::NAN),
        )?;
        let mut source = plan.instantiate();
        accessor.fetch_block(&[0], &mut source);
        assert!(accessor.values().is_empty());

        let unsupported = calculated_plan(
            &segment_reader(&index),
            "string_output",
            UntypedExpr::literal("not numerical"),
        )
        .unwrap_err();
        assert!(unsupported.to_string().contains("infer types"));
        Ok(())
    }

    #[test]
    fn calculated_name_collision_is_rejected() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_f64_field("physical", FAST);
        schema_builder.add_json_field("json", FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!(field => 1.0f64))?;
        writer.commit()?;
        let mut columns = CalculatedColumns::new();
        columns.register("physical", UntypedExpr::literal(1.0f64))?;
        let error =
            validate_calculated_column_names(&segment_reader(&index), &columns).unwrap_err();
        assert!(error.to_string().contains("collides"));

        let duplicate = columns
            .register("physical", UntypedExpr::literal(2.0f64))
            .unwrap_err();
        assert!(duplicate.to_string().contains("more than once"));

        let mut columns = CalculatedColumns::new();
        columns.register("json.not_in_this_segment", UntypedExpr::literal(1.0f64))?;
        let error =
            validate_calculated_column_names(&segment_reader(&index), &columns).unwrap_err();
        assert!(error.to_string().contains("collides"));
        Ok(())
    }
}
