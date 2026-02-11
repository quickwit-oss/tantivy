use std::ops::Bound;
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{JoinType, Operator};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, DynamicFilterPhysicalExpr, Literal};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::coop::CooperativeExec;
use datafusion_physical_plan::joins::HashJoinExec;
use datafusion_physical_plan::projection::ProjectionExec;
use tantivy::query::RangeQuery;
use tantivy::schema::{FieldType, IndexRecordOption, Schema as TantivySchema, Term};
use tantivy::DateTime;

use crate::inverted_index_provider::InvertedIndexDataSource;
use crate::table_provider::FastFieldDataSource;

/// A physical optimizer rule that moves fast field predicates from the probe
/// side of a hash join into the inverted index's tantivy query on the build side.
///
/// After this rule fires, the hash join becomes a pure 1:1 enrichment join,
/// which allows the TopK rule to safely traverse it.
#[derive(Debug)]
pub struct FastFieldFilterPushdown;

impl FastFieldFilterPushdown {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for FastFieldFilterPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(try_push_filters).map(|t| t.data)
    }

    fn name(&self) -> &str {
        "FastFieldFilterPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Attempt to move fast field filters from the probe side into the build side's
/// inverted index query at a `HashJoinExec(Inner)` node.
fn try_push_filters(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
        return Ok(Transformed::no(plan));
    };

    if *hash_join.join_type() != JoinType::Inner {
        return Ok(Transformed::no(plan));
    }

    // Find InvertedIndexDataSource on build side (left)
    let Some((inv_ds, inv_dse, inv_path)) = find_data_source::<InvertedIndexDataSource>(hash_join.left()) else {
        return Ok(Transformed::no(plan));
    };

    if inv_ds.query.is_none() {
        return Ok(Transformed::no(plan));
    }

    // Find FastFieldDataSource on probe side (right)
    let Some((ff_ds, ff_dse, ff_path)) = find_data_source::<FastFieldDataSource>(hash_join.right()) else {
        return Ok(Transformed::no(plan));
    };

    let tantivy_schema = inv_ds.index().schema();

    // Partition filters into convertible (→ tantivy) and remaining
    let mut tantivy_queries: Vec<Box<dyn tantivy::query::Query>> = Vec::new();
    let mut remaining_filters: Vec<Arc<dyn PhysicalExpr>> = Vec::new();

    for filter in ff_ds.pushed_filters() {
        // Skip DynamicFilterPhysicalExpr — these are join-specific
        if filter
            .as_any()
            .downcast_ref::<DynamicFilterPhysicalExpr>()
            .is_some()
        {
            remaining_filters.push(Arc::clone(filter));
            continue;
        }

        match physical_expr_to_tantivy_query(filter.as_ref(), &tantivy_schema) {
            Some(q) => tantivy_queries.push(q),
            None => remaining_filters.push(Arc::clone(filter)),
        }
    }

    if tantivy_queries.is_empty() {
        return Ok(Transformed::no(plan));
    }

    // Build new InvertedIndexDataSource with additional queries
    let new_inv_ds = Arc::new(inv_ds.with_additional_queries(tantivy_queries));
    let new_inv_exec: Arc<dyn ExecutionPlan> =
        Arc::new(inv_dse.clone().with_data_source(new_inv_ds));
    let new_build = rebuild_path(hash_join.left(), &inv_path, new_inv_exec)?;

    // Build new FastFieldDataSource with remaining filters only
    let new_ff_ds = Arc::new(ff_ds.with_pushed_filters(remaining_filters));
    let new_ff_exec: Arc<dyn ExecutionPlan> =
        Arc::new(ff_dse.clone().with_data_source(new_ff_ds));
    let new_probe = rebuild_path(hash_join.right(), &ff_path, new_ff_exec)?;

    // Rebuild HashJoinExec with new children
    let new_join = Arc::clone(&plan).with_new_children(vec![new_build, new_probe])?;
    Ok(Transformed::yes(new_join))
}

// ---------------------------------------------------------------------------
// Plan traversal helpers
// ---------------------------------------------------------------------------

/// Walk down through safe, single-child operators to find a `DataSourceExec`
/// wrapping a specific `DataSource` type `T`.
///
/// Returns `(datasource_ref, datasource_exec_ref, path_of_indices)` where
/// the path records which child index was taken at each step (always 0 for
/// single-child operators).
fn find_data_source<T: 'static>(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<(&T, &DataSourceExec, Vec<usize>)> {
    find_data_source_inner::<T>(plan, vec![])
}

fn find_data_source_inner<T: 'static>(
    plan: &Arc<dyn ExecutionPlan>,
    path: Vec<usize>,
) -> Option<(&T, &DataSourceExec, Vec<usize>)> {
    if let Some(dse) = plan.as_any().downcast_ref::<DataSourceExec>() {
        if let Some(ds) = dse.data_source().as_any().downcast_ref::<T>() {
            return Some((ds, dse, path));
        }
        return None;
    }

    // Safe to traverse through single-child, row-preserving operators
    if plan
        .as_any()
        .downcast_ref::<CoalesceBatchesExec>()
        .is_some()
        || plan.as_any().downcast_ref::<CooperativeExec>().is_some()
        || plan.as_any().downcast_ref::<ProjectionExec>().is_some()
    {
        let children = plan.children();
        if children.len() == 1 {
            let mut new_path = path;
            new_path.push(0);
            return find_data_source_inner::<T>(children[0], new_path);
        }
    }

    None
}

/// Rebuild the plan tree along the recorded path, replacing the leaf with
/// `new_leaf`.
fn rebuild_path(
    root: &Arc<dyn ExecutionPlan>,
    path: &[usize],
    new_leaf: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if path.is_empty() {
        return Ok(new_leaf);
    }

    let children = root.children();
    let child_idx = path[0];
    let new_child = rebuild_path(children[child_idx], &path[1..], new_leaf)?;

    let mut new_children: Vec<Arc<dyn ExecutionPlan>> =
        children.iter().map(|c| Arc::clone(c)).collect();
    new_children[child_idx] = new_child;

    Arc::clone(root).with_new_children(new_children)
}

// ---------------------------------------------------------------------------
// PhysicalExpr → tantivy Query conversion
// ---------------------------------------------------------------------------

/// Try to convert a physical expression to a tantivy query.
///
/// Handles simple comparisons: `column op literal` or `literal op column`
/// where `op` is one of `=, >, >=, <, <=` and the column is a tantivy FAST field.
pub(crate) fn physical_expr_to_tantivy_query(
    expr: &dyn PhysicalExpr,
    tantivy_schema: &TantivySchema,
) -> Option<Box<dyn tantivy::query::Query>> {
    let binary = expr.as_any().downcast_ref::<BinaryExpr>()?;
    let op = binary.op();

    // Extract (column_name, scalar_value, column_on_left)
    let (col_name, scalar, col_on_left) = extract_column_literal(binary)?;

    // Look up field in tantivy schema and verify it's a fast field
    let field = tantivy_schema.get_field(&col_name).ok()?;
    let field_entry = tantivy_schema.get_field_entry(field);
    if !field_entry.is_fast() {
        return None;
    }

    // Normalize the operator if the column was on the right side
    // e.g. `2 < price` becomes `price > 2`
    let normalized_op = if col_on_left {
        *op
    } else {
        flip_operator(op)?
    };

    let term = scalar_to_term(field, field_entry.field_type(), &scalar)?;

    match normalized_op {
        Operator::Eq => {
            Some(Box::new(tantivy::query::TermQuery::new(
                term,
                IndexRecordOption::Basic,
            )))
        }
        Operator::Gt => {
            Some(Box::new(RangeQuery::new(Bound::Excluded(term), Bound::Unbounded)))
        }
        Operator::GtEq => {
            Some(Box::new(RangeQuery::new(Bound::Included(term), Bound::Unbounded)))
        }
        Operator::Lt => {
            Some(Box::new(RangeQuery::new(Bound::Unbounded, Bound::Excluded(term))))
        }
        Operator::LtEq => {
            Some(Box::new(RangeQuery::new(Bound::Unbounded, Bound::Included(term))))
        }
        _ => None,
    }
}

/// Extract `(column_name, ScalarValue, column_is_on_left)` from a BinaryExpr.
fn extract_column_literal(
    binary: &BinaryExpr,
) -> Option<(String, ScalarValue, bool)> {
    // Try column on left, literal on right
    if let (Some(col), Some(lit)) = (
        binary.left().as_any().downcast_ref::<Column>(),
        binary.right().as_any().downcast_ref::<Literal>(),
    ) {
        return Some((col.name().to_string(), lit.value().clone(), true));
    }

    // Try literal on left, column on right
    if let (Some(lit), Some(col)) = (
        binary.left().as_any().downcast_ref::<Literal>(),
        binary.right().as_any().downcast_ref::<Column>(),
    ) {
        return Some((col.name().to_string(), lit.value().clone(), false));
    }

    None
}

/// Flip a comparison operator for when the column is on the right side.
fn flip_operator(op: &Operator) -> Option<Operator> {
    match op {
        Operator::Eq => Some(Operator::Eq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        _ => None,
    }
}

/// Convert a DataFusion `ScalarValue` to a tantivy `Term` for the given field.
fn scalar_to_term(
    field: tantivy::schema::Field,
    field_type: &FieldType,
    scalar: &ScalarValue,
) -> Option<Term> {
    match (field_type, scalar) {
        (FieldType::U64(_), ScalarValue::UInt64(Some(v))) => {
            Some(Term::from_field_u64(field, *v))
        }
        (FieldType::I64(_), ScalarValue::Int64(Some(v))) => {
            Some(Term::from_field_i64(field, *v))
        }
        (FieldType::F64(_), ScalarValue::Float64(Some(v))) => {
            Some(Term::from_field_f64(field, *v))
        }
        (FieldType::Bool(_), ScalarValue::Boolean(Some(v))) => {
            Some(Term::from_field_bool(field, *v))
        }
        (FieldType::Str(_), ScalarValue::Utf8(Some(s))) => {
            Some(Term::from_field_text(field, s))
        }
        // Handle numeric type coercions that DataFusion may apply
        (FieldType::F64(_), ScalarValue::Int64(Some(v))) => {
            Some(Term::from_field_f64(field, *v as f64))
        }
        (FieldType::F64(_), ScalarValue::Float32(Some(v))) => {
            Some(Term::from_field_f64(field, *v as f64))
        }
        (FieldType::I64(_), ScalarValue::Int32(Some(v))) => {
            Some(Term::from_field_i64(field, *v as i64))
        }
        (FieldType::U64(_), ScalarValue::Int64(Some(v))) if *v >= 0 => {
            Some(Term::from_field_u64(field, *v as u64))
        }
        // Date — tantivy DateTime stores nanoseconds internally.
        // Arrow Timestamp(Microsecond) is what schema_mapping produces.
        (FieldType::Date(_), ScalarValue::TimestampMicrosecond(Some(v), _)) => {
            Some(Term::from_field_date(field, DateTime::from_timestamp_micros(*v)))
        }
        (FieldType::Date(_), ScalarValue::TimestampSecond(Some(v), _)) => {
            Some(Term::from_field_date(field, DateTime::from_timestamp_secs(*v)))
        }
        (FieldType::Date(_), ScalarValue::TimestampMillisecond(Some(v), _)) => {
            Some(Term::from_field_date(field, DateTime::from_timestamp_millis(*v)))
        }
        (FieldType::Date(_), ScalarValue::TimestampNanosecond(Some(v), _)) => {
            Some(Term::from_field_date(field, DateTime::from_timestamp_nanos(*v)))
        }
        // IpAddr — mapped to Utf8 in schema_mapping, tantivy stores as Ipv6Addr
        (FieldType::IpAddr(_), ScalarValue::Utf8(Some(s))) => {
            let ip: std::net::IpAddr = s.parse().ok()?;
            let ipv6 = match ip {
                std::net::IpAddr::V4(v4) => v4.to_ipv6_mapped(),
                std::net::IpAddr::V6(v6) => v6,
            };
            Some(Term::from_field_ip_addr(field, ipv6))
        }
        // Bytes — mapped to Binary in schema_mapping
        (FieldType::Bytes(_), ScalarValue::Binary(Some(b))) => {
            Some(Term::from_field_bytes(field, b))
        }
        _ => None,
    }
}
