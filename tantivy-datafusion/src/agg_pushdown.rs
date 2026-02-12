use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::coop::CooperativeExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;

use crate::agg_exec::TantivyAggregateExec;
use crate::table_provider::FastFieldDataSource;

/// A physical optimizer rule that replaces DataFusion's `AggregateExec`
/// with tantivy's native `AggregationSegmentCollector` when the
/// `FastFieldDataSource` has tantivy `Aggregations` stashed.
///
/// This eliminates the overhead of DataFusion's hash-based GROUP BY and
/// Arrow materialization, achieving near-native tantivy aggregation
/// performance.
///
/// The rule only fires for **bucket aggregations** (terms, histogram, range)
/// where the hash GROUP BY overhead is significant. Simple metric-only
/// aggregations (avg, stats, count) are left to DataFusion's optimized
/// vectorized Arrow path, which is already efficient for single-pass scans.
///
/// The rule only fires when `FastFieldDataSource.aggregations` is `Some`,
/// which is set by `execute_aggregations`. Regular SQL queries (without
/// `execute_aggregations`) are unaffected.
#[derive(Debug)]
pub struct AggPushdown;

impl AggPushdown {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for AggPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(try_rewrite).map(|t| t.data)
    }

    fn name(&self) -> &str {
        "AggPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Attempt to replace an `AggregateExec` subtree with `TantivyAggregateExec`.
fn try_rewrite(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() else {
        return Ok(Transformed::no(plan));
    };

    // Handle both single-phase and two-phase aggregation patterns.
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

/// Check if the aggregate has GROUP BY expressions (bucket aggregation).
/// We only push down bucket aggs; metric-only aggs (no GROUP BY) are
/// faster via DataFusion's native vectorized Arrow path.
fn has_group_by(agg: &AggregateExec) -> bool {
    !agg.group_expr().is_empty()
}

/// Rewrite single-phase: AggregateExec(Single) → [safe ops] → DataSourceExec.
fn try_rewrite_single(
    agg: &AggregateExec,
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if !has_group_by(agg) {
        return Ok(Transformed::no(plan.clone()));
    }

    let input = agg.input();
    if let Some(ff_ds) = find_fast_field_datasource(input) {
        if let Some(tantivy_aggs) = ff_ds.aggregations() {
            let new_exec = TantivyAggregateExec::new(
                ff_ds.opener().clone(),
                tantivy_aggs.clone(),
                ff_ds.query().map(|q| Arc::from(q.box_clone())),
                agg.schema(),
            );
            return Ok(Transformed::yes(Arc::new(new_exec)));
        }
    }
    Ok(Transformed::no(plan.clone()))
}

/// Rewrite two-phase: AggregateExec(Final) → ... → AggregateExec(Partial) → [safe ops] → DataSourceExec.
fn try_rewrite_two_phase(
    _final_agg: &AggregateExec,
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let final_agg = plan.as_any().downcast_ref::<AggregateExec>().unwrap();

    if !has_group_by(final_agg) {
        return Ok(Transformed::no(plan.clone()));
    }

    // Walk through safe operators between Final and Partial
    let partial_agg = find_partial_aggregate(final_agg.input())?;
    let Some(partial_agg) = partial_agg else {
        return Ok(Transformed::no(plan.clone()));
    };

    let partial_input = partial_agg.input();
    if let Some(ff_ds) = find_fast_field_datasource(partial_input) {
        if let Some(tantivy_aggs) = ff_ds.aggregations() {
            let new_exec = TantivyAggregateExec::new(
                ff_ds.opener().clone(),
                tantivy_aggs.clone(),
                ff_ds.query().map(|q| Arc::from(q.box_clone())),
                final_agg.schema(),
            );
            return Ok(Transformed::yes(Arc::new(new_exec)));
        }
    }

    Ok(Transformed::no(plan.clone()))
}

/// Walk through safe operators to find a `FastFieldDataSource`.
fn find_fast_field_datasource(plan: &Arc<dyn ExecutionPlan>) -> Option<&FastFieldDataSource> {
    // Direct: DataSourceExec wrapping FastFieldDataSource
    if let Some(dse) = plan.as_any().downcast_ref::<DataSourceExec>() {
        if let Some(ff) = dse.data_source().as_any().downcast_ref::<FastFieldDataSource>() {
            return Some(ff);
        }
        return None;
    }

    // Safe to traverse through these single-child operators
    if plan
        .as_any()
        .downcast_ref::<CoalesceBatchesExec>()
        .is_some()
        || plan.as_any().downcast_ref::<CooperativeExec>().is_some()
        || plan.as_any().downcast_ref::<ProjectionExec>().is_some()
        || plan.as_any().downcast_ref::<RepartitionExec>().is_some()
    {
        let children = plan.children();
        if children.len() == 1 {
            return find_fast_field_datasource(children[0]);
        }
    }

    None
}

/// Walk through safe operators between Final and Partial aggregate to find
/// the Partial AggregateExec.
fn find_partial_aggregate(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<&AggregateExec>> {
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        if matches!(agg.mode(), AggregateMode::Partial) {
            return Ok(Some(agg));
        }
        return Ok(None);
    }

    // Safe to traverse between Final and Partial
    if plan.as_any().downcast_ref::<RepartitionExec>().is_some()
        || plan
            .as_any()
            .downcast_ref::<CoalesceBatchesExec>()
            .is_some()
        || plan.as_any().downcast_ref::<CooperativeExec>().is_some()
    {
        let children = plan.children();
        if children.len() == 1 {
            return find_partial_aggregate(children[0]);
        }
    }

    Ok(None)
}
