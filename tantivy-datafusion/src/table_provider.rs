use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::{Result, Statistics};
use datafusion::datasource::TableProvider;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::source::{DataSource, DataSourceExec};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::projection::ProjectionExpr;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayFormatType, Partitioning, SendableRecordBatchStream};
use futures::stream;
use tantivy::query::{EnableScoring, Query};
use tantivy::{DocId, Index};

use crate::fast_field_reader::read_segment_fast_fields_to_batch;
use crate::schema_mapping::tantivy_schema_to_arrow_from_index;

/// A DataFusion table provider backed by a tantivy index.
///
/// Exposes fast fields from the index as Arrow columns. DataFusion handles
/// all SQL filtering, sorting, and aggregation natively on Arrow output.
///
/// For inverted index (full-text) queries, use `new_with_query()` to pass
/// a tantivy `Query` that pre-filters documents at the segment level.
pub struct TantivyTableProvider {
    index: Index,
    arrow_schema: SchemaRef,
    query: Option<Arc<dyn Query>>,
}

impl fmt::Debug for TantivyTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TantivyTableProvider")
            .field("arrow_schema", &self.arrow_schema)
            .field("has_query", &self.query.is_some())
            .finish()
    }
}

impl TantivyTableProvider {
    pub fn new(index: Index) -> Self {
        let arrow_schema = tantivy_schema_to_arrow_from_index(&index);
        Self {
            index,
            arrow_schema,
            query: None,
        }
    }

    /// Create a provider with a pre-set tantivy query for direct pushdown.
    ///
    /// The query runs through the inverted index to produce a doc ID set,
    /// then only matching documents have their fast fields read.
    pub fn new_with_query(index: Index, query: Box<dyn Query>) -> Self {
        let arrow_schema = tantivy_schema_to_arrow_from_index(&index);
        Self {
            index,
            arrow_schema,
            query: Some(Arc::from(query)),
        }
    }
}

#[async_trait]
impl TableProvider for TantivyTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| self.arrow_schema.field(i).clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => self.arrow_schema.clone(),
        };

        let combined_query: Option<Arc<dyn Query>> =
            self.query.as_ref().map(|q| Arc::from(q.box_clone()));

        let reader = self
            .index
            .reader()
            .map_err(|e| DataFusionError::Internal(format!("open reader: {e}")))?;
        let num_segments = reader.searcher().segment_readers().len();

        let data_source = FastFieldDataSource {
            index: self.index.clone(),
            arrow_schema: self.arrow_schema.clone(),
            projected_schema,
            projection: projection.cloned(),
            query: combined_query,
            limit,
            num_segments,
            pushed_filters: vec![],
        };
        Ok(Arc::new(DataSourceExec::new(Arc::new(data_source))))
    }
}

// ---------------------------------------------------------------------------
// DataSource implementation
// ---------------------------------------------------------------------------

/// A [`DataSource`] backed by tantivy fast fields.
///
/// Accepts dynamic filters pushed down from the optimizer (e.g. from hash join
/// build-side min/max bounds) and applies them after batch generation.
#[derive(Debug)]
struct FastFieldDataSource {
    index: Index,
    arrow_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    query: Option<Arc<dyn Query>>,
    limit: Option<usize>,
    num_segments: usize,
    pushed_filters: Vec<Arc<dyn PhysicalExpr>>,
}

impl FastFieldDataSource {
    fn clone_with(&self, f: impl FnOnce(&mut Self)) -> Self {
        let mut new = FastFieldDataSource {
            index: self.index.clone(),
            arrow_schema: self.arrow_schema.clone(),
            projected_schema: self.projected_schema.clone(),
            projection: self.projection.clone(),
            query: self.query.as_ref().map(|q| Arc::from(q.box_clone())),
            limit: self.limit,
            num_segments: self.num_segments,
            pushed_filters: self.pushed_filters.clone(),
        };
        f(&mut new);
        new
    }
}

impl DataSource for FastFieldDataSource {
    fn open(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let index = self.index.clone();
        let segment_idx = partition;
        let projected_schema = self.projected_schema.clone();
        let query = self.query.as_ref().map(|q| Arc::from(q.box_clone()));
        let limit = self.limit;
        let pushed_filters = self.pushed_filters.clone();

        let schema = self.projected_schema.clone();
        // Lazy: generate the batch inside the stream so dynamic filters
        // pushed after the build side completes are evaluated at poll time.
        let stream = stream::once(async move {
            generate_and_filter_batch(
                &index,
                segment_idx,
                &projected_schema,
                query.as_ref(),
                limit,
                &pushed_filters,
            )
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "FastFieldDataSource(segments={}, query={}, limit={:?}",
            self.num_segments,
            self.query.is_some(),
            self.limit,
        )?;
        if !self.pushed_filters.is_empty() {
            write!(f, ", pushed_filters=[")?;
            for (i, filter) in self.pushed_filters.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{filter}")?;
            }
            write!(f, "]")?;
        }
        write!(f, ")")
    }

    fn output_partitioning(&self) -> Partitioning {
        segment_hash_partitioning(&self.projected_schema, self.num_segments)
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new(self.projected_schema.clone())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.projected_schema))
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        // Decline optimizer limit pushdown. The scan already applies the
        // limit passed via TableProvider::scan() when a tantivy query is
        // set. For SQL-filtered queries, DataFusion keeps a GlobalLimitExec
        // above the FilterExec so limits are applied after filtering.
        None
    }

    fn fetch(&self) -> Option<usize> {
        self.limit
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        ExecutionPlanMetricsSet::new()
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &[ProjectionExpr],
    ) -> Result<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        let results: Vec<PushedDown> = filters.iter().map(|_| PushedDown::Yes).collect();
        let mut new_filters = self.pushed_filters.clone();
        new_filters.extend(filters);
        let updated = self.clone_with(|s| s.pushed_filters = new_filters);
        Ok(
            FilterPushdownPropagation::with_parent_pushdown_result(results)
                .with_updated_node(Arc::new(updated) as Arc<dyn DataSource>),
        )
    }
}

// ---------------------------------------------------------------------------
// Batch generation + filter application
// ---------------------------------------------------------------------------

fn generate_and_filter_batch(
    index: &Index,
    segment_idx: usize,
    projected_schema: &SchemaRef,
    query: Option<&Arc<dyn Query>>,
    limit: Option<usize>,
    pushed_filters: &[Arc<dyn PhysicalExpr>],
) -> Result<arrow::record_batch::RecordBatch> {
    let reader = index
        .reader()
        .map_err(|e| DataFusionError::Internal(format!("open reader: {e}")))?;
    let searcher = reader.searcher();
    let segment_reader = searcher.segment_reader(segment_idx as u32);

    // If a query is set, run it to get matching doc IDs
    let doc_ids: Option<Vec<DocId>> = match query {
        Some(query) => {
            let tantivy_schema = index.schema();
            let weight = query
                .weight(EnableScoring::disabled_from_schema(&tantivy_schema))
                .map_err(|e| DataFusionError::Internal(format!("create weight: {e}")))?;
            let mut matching_docs: Vec<DocId> = Vec::new();
            weight
                .for_each_no_score(segment_reader, &mut |docs| {
                    if let Some(lim) = limit {
                        let remaining = lim.saturating_sub(matching_docs.len());
                        if remaining > 0 {
                            matching_docs.extend_from_slice(&docs[..docs.len().min(remaining)]);
                        }
                    } else {
                        matching_docs.extend_from_slice(docs);
                    }
                })
                .map_err(|e| DataFusionError::Internal(format!("query execution: {e}")))?;
            if let Some(lim) = limit {
                matching_docs.truncate(lim);
            }
            Some(matching_docs)
        }
        None => None,
    };

    let mut batch = read_segment_fast_fields_to_batch(
        segment_reader,
        projected_schema,
        doc_ids.as_deref(),
        limit,
        segment_idx as u32,
    )?;

    // Apply pushed-down filters (e.g. dynamic join filters).
    // When a DynamicFilterPhysicalExpr hasn't received bounds yet it
    // evaluates to `Scalar(Boolean(true))` → all-true mask → no-op.
    for filter in pushed_filters {
        if batch.num_rows() == 0 {
            break;
        }
        let result = filter.evaluate(&batch)?;
        let mask = match result {
            datafusion::physical_plan::ColumnarValue::Array(arr) => arr
                .as_boolean()
                .clone(),
            datafusion::physical_plan::ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Boolean(Some(true)),
            ) => {
                // All-true — nothing to filter.
                continue;
            }
            datafusion::physical_plan::ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Boolean(Some(false)),
            ) => {
                // All-false — empty result.
                batch = batch.slice(0, 0);
                break;
            }
            other => {
                let arr = other.into_array(batch.num_rows())?;
                arr.as_boolean().clone()
            }
        };
        batch = filter_record_batch(&batch, &mask)?;
    }

    if batch.num_rows() == 0 {
        // Return an empty batch with the correct schema so the stream is
        // well-typed even when nothing matches.
        return Ok(arrow::record_batch::RecordBatch::new_empty(
            projected_schema.clone(),
        ));
    }

    Ok(batch)
}

// ---------------------------------------------------------------------------
// Shared partitioning helper
// ---------------------------------------------------------------------------

/// Declare `Hash([_doc_id, _segment_ord], num_segments)` when both columns
/// are present in the projected schema. All tantivy-backed providers use
/// this so the optimizer recognises them as co-partitioned and skips
/// unnecessary shuffles in join plans.
pub(crate) fn segment_hash_partitioning(
    projected_schema: &SchemaRef,
    num_segments: usize,
) -> Partitioning {
    if let (Ok(doc_id_idx), Ok(seg_ord_idx)) = (
        projected_schema.index_of("_doc_id"),
        projected_schema.index_of("_segment_ord"),
    ) {
        Partitioning::Hash(
            vec![
                Arc::new(Column::new("_doc_id", doc_id_idx)),
                Arc::new(Column::new("_segment_ord", seg_ord_idx)),
            ],
            num_segments,
        )
    } else {
        Partitioning::UnknownPartitioning(num_segments)
    }
}
