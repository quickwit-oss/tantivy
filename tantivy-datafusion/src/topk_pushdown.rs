use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::coop::CooperativeExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort::SortExec;

use crate::inverted_index_provider::InvertedIndexDataSource;

/// A physical optimizer rule that pushes `ORDER BY _score DESC LIMIT K`
/// into the `InvertedIndexDataSource` as a topK hint.
///
/// This enables Block-WAND pruning inside tantivy, achieving sub-linear
/// performance for ranked retrieval queries.
///
/// **Safety:** The rule only fires when there is no `FilterExec` or
/// `HashJoinExec` between the `SortExec` and the `DataSourceExec`
/// wrapping `InvertedIndexDataSource`. This ensures the topK doesn't
/// silently drop rows that would otherwise survive filtering/joining.
#[derive(Debug)]
pub struct TopKPushdown;

impl TopKPushdown {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for TopKPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(try_rewrite).map(|t| t.data)
    }

    fn name(&self) -> &str {
        "TopKPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Attempt to rewrite a `SortExec(fetch=K, _score DESC)` by pushing
/// `topk=K` into a child `InvertedIndexDataSource`.
fn try_rewrite(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(sort) = plan.as_any().downcast_ref::<SortExec>() else {
        return Ok(Transformed::no(plan));
    };

    let Some(k) = sort.fetch() else {
        return Ok(Transformed::no(plan));
    };

    // Check that the sort expression is `_score DESC`
    if !is_score_desc_sort(sort) {
        return Ok(Transformed::no(plan));
    }

    // Walk the child tree looking for InvertedIndexDataSource.
    // Only traverse through safe, row-preserving operators.
    let child = Arc::clone(sort.input());
    let Some(new_child) = try_inject_topk(&child, k)? else {
        return Ok(Transformed::no(plan));
    };

    // Rebuild the SortExec with the modified child tree.
    // The sort stays in place for cross-partition merge correctness.
    let new_sort: Arc<dyn ExecutionPlan> =
        Arc::new(sort.clone()).with_new_children(vec![new_child])?;
    Ok(Transformed::yes(new_sort))
}

/// Check if the SortExec sorts by `_score DESC`.
fn is_score_desc_sort(sort: &SortExec) -> bool {
    let exprs = sort.expr();
    if exprs.len() != 1 {
        return false;
    }
    let sort_expr = &exprs[0];
    // Must be descending
    if !sort_expr.options.descending {
        return false;
    }
    // The expression must reference a column named `_score`
    resolve_column_name(sort_expr.expr.as_ref()) == Some("_score")
}

/// Resolve the underlying column name from a physical expression.
fn resolve_column_name(expr: &dyn PhysicalExpr) -> Option<&str> {
    expr.as_any()
        .downcast_ref::<Column>()
        .map(|c| c.name())
}

/// Recursively walk down the plan tree from the SortExec's child,
/// looking for a DataSourceExec wrapping InvertedIndexDataSource.
///
/// Returns `Some(rebuilt_tree)` if topK was injected, or `None` if
/// the path is unsafe or no InvertedIndexDataSource was found.
fn try_inject_topk(
    plan: &Arc<dyn ExecutionPlan>,
    k: usize,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Target: DataSourceExec wrapping InvertedIndexDataSource
    if let Some(dse) = plan.as_any().downcast_ref::<DataSourceExec>() {
        if let Some(inv) = dse.data_source().as_any().downcast_ref::<InvertedIndexDataSource>() {
            if inv.query.is_some() {
                let new_ds = Arc::new(inv.with_topk(k));
                let new_exec = dse.clone().with_data_source(new_ds);
                return Ok(Some(Arc::new(new_exec)));
            }
        }
        return Ok(None);
    }

    // Safe to traverse through these operators (row-preserving, 1:1)
    if plan.as_any().downcast_ref::<CoalesceBatchesExec>().is_some()
        || plan.as_any().downcast_ref::<CooperativeExec>().is_some()
        || plan.as_any().downcast_ref::<ProjectionExec>().is_some()
    {
        let children = plan.children();
        if children.len() == 1 {
            if let Some(new_child) = try_inject_topk(children[0], k)? {
                let rebuilt = Arc::clone(plan).with_new_children(vec![new_child])?;
                return Ok(Some(rebuilt));
            }
        }
        return Ok(None);
    }

    // Anything else (FilterExec, HashJoinExec, RepartitionExec, etc.)
    // is unsafe — bail out.
    Ok(None)
}
