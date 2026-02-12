use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, Statistics};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_datasource::source::DataSourceExec;
use datafusion_expr_common::groups_accumulator::{EmitTo, GroupsAccumulator};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_expr::{aggregate::AggregateFunctionExpr, PhysicalExpr};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::coop::CooperativeExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::ExecutionPlanProperties;
use futures::stream;

use crate::table_provider::FastFieldDataSource;

// ---------------------------------------------------------------------------
// Optimizer rule
// ---------------------------------------------------------------------------

/// A physical optimizer rule that replaces `AggregateExec` with
/// `DenseOrdinalAggExec` when the GROUP BY column is `Dictionary(Int32, Utf8)`
/// from a `FastFieldDataSource`.
///
/// Dictionary keys are per-segment ordinals, so they can be used as dense
/// array indices for grouping — no hash table needed. This is significantly
/// faster than DataFusion's hash-based GROUP BY for low-cardinality string
/// columns (e.g. terms aggregation with 7 distinct values over 1M docs).
///
/// Handles both single-phase (`Single`/`SinglePartitioned`) and two-phase
/// (`Final`+`Partial`) aggregation patterns.
#[derive(Debug)]
pub struct OrdinalGroupByOptimization;

impl OrdinalGroupByOptimization {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for OrdinalGroupByOptimization {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(try_rewrite_ordinal).map(|t| t.data)
    }

    fn name(&self) -> &str {
        "OrdinalGroupByOptimization"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Whether the DenseOrdinalAggExec should emit intermediate state or final values.
#[derive(Debug, Clone, Copy)]
enum OrdinalEmitMode {
    /// Emit intermediate state fields (for Partial phase — Final merges above).
    State,
    /// Emit final evaluated values (for Single phase — no merge above).
    Final,
}

fn try_rewrite_ordinal(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() else {
        return Ok(Transformed::no(plan));
    };

    match agg.mode() {
        AggregateMode::Single | AggregateMode::SinglePartitioned => {
            try_rewrite_single(agg, &plan)
        }
        AggregateMode::Final | AggregateMode::FinalPartitioned => {
            try_rewrite_two_phase(agg, &plan)
        }
        AggregateMode::Partial => {
            // Partial on its own — not the top-level, skip
            Ok(Transformed::no(plan))
        }
    }
}

/// Rewrite single-phase: AggregateExec(Single) → [safe ops] → DataSourceExec.
/// Replaces the entire AggregateExec with DenseOrdinalAggExec(Final mode).
fn try_rewrite_single(
    agg: &AggregateExec,
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let (group_col_idx, group_col_name) = match check_ordinal_eligible(agg, agg.input()) {
        Some(v) => v,
        None => return Ok(Transformed::no(plan.clone())),
    };

    let dse_plan = match find_data_source_exec(agg.input()) {
        Some(v) => v,
        None => return Ok(Transformed::no(plan.clone())),
    };

    // Single mode: output is the final schema (group_col + evaluated agg values).
    // Group col becomes Utf8 (resolved from dict), agg values from evaluate().
    let output_schema = agg.schema();

    let ordinal_exec = Arc::new(DenseOrdinalAggExec::new(
        dse_plan,
        group_col_idx,
        group_col_name,
        agg.aggr_expr().to_vec(),
        agg.filter_expr().to_vec(),
        output_schema,
        OrdinalEmitMode::Final,
    ));

    Ok(Transformed::yes(ordinal_exec))
}

/// Rewrite two-phase: AggregateExec(Final) → ... → AggregateExec(Partial) → ... → DataSourceExec.
/// Replaces the entire subtree with:
///   AggregateExec(Final, 1 partition) → CoalescePartitionsExec → DenseOrdinalAggExec(State)
/// This avoids the stateful RepartitionExec(Hash) since the partial output is tiny.
fn try_rewrite_two_phase(
    final_agg: &AggregateExec,
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    // Must have exactly one GROUP BY expression.
    let group_expr = final_agg.group_expr();
    if group_expr.expr().len() != 1 || !group_expr.is_single() {
        return Ok(Transformed::no(plan.clone()));
    }

    // Walk to find the Partial aggregate.
    let Some((partial_agg, _partial_plan)) = find_partial_with_plan(final_agg.input())? else {
        return Ok(Transformed::no(plan.clone()));
    };

    let (group_col_idx, group_col_name) =
        match check_ordinal_eligible(partial_agg, partial_agg.input()) {
            Some(v) => v,
            None => return Ok(Transformed::no(plan.clone())),
        };

    let dse_plan = match find_data_source_exec(partial_agg.input()) {
        Some(v) => v,
        None => return Ok(Transformed::no(plan.clone())),
    };

    // Partial mode: output schema is [group_col: Dict(Int32, Utf8), state_fields...].
    let partial_input_schema = partial_agg.input().schema();
    let group_data_type = partial_input_schema
        .field(group_col_idx)
        .data_type()
        .clone();
    let mut output_fields: Vec<Field> =
        vec![Field::new(&group_col_name, group_data_type, true)];
    for agg_expr in partial_agg.aggr_expr() {
        for field in agg_expr.state_fields()? {
            output_fields.push(field.as_ref().clone());
        }
    }
    let output_schema = Arc::new(Schema::new(output_fields));

    let ordinal_exec: Arc<dyn ExecutionPlan> = Arc::new(DenseOrdinalAggExec::new(
        dse_plan,
        group_col_idx,
        group_col_name,
        partial_agg.aggr_expr().to_vec(),
        partial_agg.filter_expr().to_vec(),
        output_schema,
        OrdinalEmitMode::State,
    ));

    // CoalescePartitionsExec merges N partitions into 1 — just concatenation,
    // no hash channels, no statefulness. The partial output is tiny (~7 rows
    // per partition), so this is effectively free.
    let coalesced: Arc<dyn ExecutionPlan> =
        Arc::new(CoalescePartitionsExec::new(ordinal_exec));

    // Rebuild Final with the coalesced single-partition input.
    let plan: Arc<dyn ExecutionPlan> = Arc::new(final_agg.clone());
    let new_final = plan.with_new_children(vec![coalesced])?;

    Ok(Transformed::yes(new_final))
}

/// Check if an AggregateExec is eligible for ordinal optimization.
/// Returns (group_col_idx, group_col_name) if eligible.
fn check_ordinal_eligible(
    agg: &AggregateExec,
    input: &Arc<dyn ExecutionPlan>,
) -> Option<(usize, String)> {
    let group = agg.group_expr();
    if group.expr().len() != 1 || !group.is_single() {
        return None;
    }

    let (_expr, group_col_name) = &group.expr()[0];
    let input_schema = input.schema();
    let group_col_idx = input_schema.index_of(group_col_name).ok()?;
    let group_field = input_schema.field(group_col_idx);

    let is_dict_i32_utf8 = matches!(group_field.data_type(), DataType::Dictionary(k, v)
        if k.as_ref() == &DataType::Int32 && v.as_ref() == &DataType::Utf8
    );
    if !is_dict_i32_utf8 {
        return None;
    }

    // Must be backed by FastFieldDataSource.
    find_fast_field_datasource(input)?;

    Some((group_col_idx, group_col_name.clone()))
}

// ---------------------------------------------------------------------------
// Tree helpers
// ---------------------------------------------------------------------------

/// Walk through safe operators between Final and Partial, returning the Partial.
fn find_partial_with_plan(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<(&AggregateExec, Arc<dyn ExecutionPlan>)>> {
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        if matches!(agg.mode(), AggregateMode::Partial) {
            return Ok(Some((agg, plan.clone())));
        }
        return Ok(None);
    }

    if is_safe_intermediate(plan) {
        let children = plan.children();
        if children.len() == 1 {
            return find_partial_with_plan(children[0]);
        }
    }

    Ok(None)
}

fn is_safe_intermediate(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().downcast_ref::<RepartitionExec>().is_some()
        || plan
            .as_any()
            .downcast_ref::<CoalesceBatchesExec>()
            .is_some()
        || plan.as_any().downcast_ref::<CooperativeExec>().is_some()
}

fn find_fast_field_datasource(plan: &Arc<dyn ExecutionPlan>) -> Option<&FastFieldDataSource> {
    if let Some(dse) = plan.as_any().downcast_ref::<DataSourceExec>() {
        return dse
            .data_source()
            .as_any()
            .downcast_ref::<FastFieldDataSource>();
    }
    if is_safe_intermediate(plan) {
        let children = plan.children();
        if children.len() == 1 {
            return find_fast_field_datasource(children[0]);
        }
    }
    None
}

fn find_data_source_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.as_any().downcast_ref::<DataSourceExec>().is_some() {
        return Some(plan.clone());
    }
    if is_safe_intermediate(plan) {
        let children = plan.children();
        if children.len() == 1 {
            return find_data_source_exec(children[0]);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// DenseOrdinalAggExec
// ---------------------------------------------------------------------------

/// A custom `ExecutionPlan` that replaces `AggregateExec` for
/// dictionary-encoded GROUP BY columns from tantivy fast fields.
///
/// Uses dictionary keys (ordinals) as dense array indices into
/// `GroupsAccumulator`s — no hash table needed.
pub(crate) struct DenseOrdinalAggExec {
    input: Arc<dyn ExecutionPlan>,
    group_col_idx: usize,
    group_col_name: String,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
    output_schema: SchemaRef,
    emit_mode: OrdinalEmitMode,
    properties: PlanProperties,
}

impl DenseOrdinalAggExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        group_col_idx: usize,
        group_col_name: String,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
        filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
        output_schema: SchemaRef,
        emit_mode: OrdinalEmitMode,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            input.output_partitioning().clone(),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            input,
            group_col_idx,
            group_col_name,
            aggr_expr,
            filter_expr,
            output_schema,
            emit_mode,
            properties,
        }
    }
}

impl fmt::Debug for DenseOrdinalAggExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DenseOrdinalAggExec")
            .field("group_col", &self.group_col_name)
            .field("num_aggs", &self.aggr_expr.len())
            .field("emit_mode", &self.emit_mode)
            .finish()
    }
}

impl DisplayAs for DenseOrdinalAggExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DenseOrdinalAggExec(gby={}, aggs={}, mode={:?})",
            self.group_col_name,
            self.aggr_expr.len(),
            self.emit_mode,
        )
    }
}

impl ExecutionPlan for DenseOrdinalAggExec {
    fn name(&self) -> &str {
        "DenseOrdinalAggExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "DenseOrdinalAggExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(DenseOrdinalAggExec::new(
            children[0].clone(),
            self.group_col_idx,
            self.group_col_name.clone(),
            self.aggr_expr.clone(),
            self.filter_expr.clone(),
            self.output_schema.clone(),
            self.emit_mode,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let group_col_idx = self.group_col_idx;
        let aggr_expr = self.aggr_expr.clone();
        let filter_expr = self.filter_expr.clone();
        let output_schema = self.output_schema.clone();
        let emit_mode = self.emit_mode;

        let stream = stream::once(async move {
            execute_ordinal_agg(
                input_stream,
                group_col_idx,
                &aggr_expr,
                &filter_expr,
                &output_schema,
                emit_mode,
            )
            .await
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.output_schema))
    }
}

// ---------------------------------------------------------------------------
// Core execution
// ---------------------------------------------------------------------------

async fn execute_ordinal_agg(
    mut input: SendableRecordBatchStream,
    group_col_idx: usize,
    aggr_expr: &[Arc<AggregateFunctionExpr>],
    filter_expr: &[Option<Arc<dyn PhysicalExpr>>],
    output_schema: &SchemaRef,
    emit_mode: OrdinalEmitMode,
) -> Result<RecordBatch> {
    use futures::StreamExt;

    let mut accumulators: Vec<Box<dyn GroupsAccumulator>> = aggr_expr
        .iter()
        .map(|expr| expr.create_groups_accumulator())
        .collect::<Result<Vec<_>>>()?;

    let mut dict_values: Option<ArrayRef> = None;
    let mut num_groups: usize = 0;

    while let Some(batch_result) = input.next().await {
        let batch = batch_result?;
        if batch.num_rows() == 0 {
            continue;
        }

        let dict = batch.column(group_col_idx).as_dictionary::<Int32Type>();
        let keys = dict.keys();

        if dict_values.is_none() {
            dict_values = Some(dict.values().clone());
            // +1 for a potential null group
            num_groups = dict.values().len() + 1;
        }

        let null_group = num_groups - 1;

        // Fast path: if no nulls (common for tantivy fast fields), read values
        // directly without per-element null checks.
        let group_indices: Vec<usize> = if keys.null_count() == 0 {
            keys.values().iter().map(|&k| k as usize).collect()
        } else {
            (0..keys.len())
                .map(|i| {
                    if keys.is_null(i) {
                        null_group
                    } else {
                        keys.value(i) as usize
                    }
                })
                .collect()
        };

        for (acc_idx, acc) in accumulators.iter_mut().enumerate() {
            let input_exprs = aggr_expr[acc_idx].expressions();
            let input_arrays: Vec<ArrayRef> = input_exprs
                .iter()
                .map(|expr| {
                    expr.evaluate(&batch)
                        .and_then(|cv| cv.into_array(batch.num_rows()))
                })
                .collect::<Result<Vec<_>>>()?;

            let opt_filter = match &filter_expr[acc_idx] {
                Some(f) => {
                    let filter_result = f.evaluate(&batch)?;
                    let arr = filter_result.into_array(batch.num_rows())?;
                    Some(arr.as_boolean().clone())
                }
                None => None,
            };

            acc.update_batch(&input_arrays, &group_indices, opt_filter.as_ref(), num_groups)?;
        }
    }

    if dict_values.is_none() {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    let dict_values = dict_values.unwrap();
    let dict_strings = dict_values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "DenseOrdinalAggExec: expected Utf8 dictionary values".into(),
            )
        })?;

    let num_dict_entries = dict_strings.len();

    let mut output_columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());

    // Group column: emit as Dict(Int32, Utf8) with identity key mapping
    let keys = Int32Array::from((0..num_dict_entries as i32).collect::<Vec<_>>());
    let group_col: ArrayRef = Arc::new(
        arrow::array::DictionaryArray::<Int32Type>::try_new(keys, Arc::new(dict_strings.clone()))
            .map_err(|e| {
                DataFusionError::Internal(format!("DenseOrdinalAggExec dict output: {e}"))
            })?,
    );
    // If the output schema expects Utf8 (not Dict), cast
    let group_col = if matches!(
        output_schema.field(0).data_type(),
        DataType::Dictionary(_, _)
    ) {
        group_col
    } else {
        Arc::new(dict_strings.clone()) as ArrayRef
    };
    output_columns.push(group_col);

    match emit_mode {
        OrdinalEmitMode::State => {
            // Partial mode: emit intermediate state fields
            for acc in accumulators.iter_mut() {
                for state_arr in acc.state(EmitTo::All)? {
                    output_columns.push(state_arr.slice(0, num_dict_entries));
                }
            }
        }
        OrdinalEmitMode::Final => {
            // Single mode: emit final evaluated values
            for acc in accumulators.iter_mut() {
                let evaluated = acc.evaluate(EmitTo::All)?;
                output_columns.push(evaluated.slice(0, num_dict_entries));
            }
        }
    }

    RecordBatch::try_new(output_schema.clone(), output_columns)
        .map_err(|e| DataFusionError::Internal(format!("DenseOrdinalAggExec output: {e}")))
}
