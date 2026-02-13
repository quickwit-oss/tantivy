use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{NullEquality, Result};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, JoinType, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_expr::expressions::{self, Column};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::projection::ProjectionExec;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::Index;

use crate::document_provider::DocumentDataSource;
use crate::full_text_udf::extract_full_text_call;
use crate::index_opener::{DirectIndexOpener, IndexOpener};
use crate::inverted_index_provider::{InvertedIndexDataSource, TantivyInvertedIndexProvider};
use crate::schema_mapping::{tantivy_schema_to_arrow, tantivy_schema_to_arrow_from_index};
use crate::table_provider::{FastFieldDataSource, PartitionRange};

/// A unified DataFusion table provider that exposes fast fields, BM25 scores,
/// and stored documents from a tantivy index as a single flat table.
///
/// Internally, `scan()` composes `HashJoinExec` nodes wiring the existing
/// `FastFieldDataSource`, `InvertedIndexDataSource`, and `DocumentDataSource`
/// together. Only the sub-providers actually needed for the query are activated:
///
/// - **Fast fields only** — no `_score`, no `_document`, no `full_text()` filter
/// - **+ inverted index** — when `_score` is projected or `full_text()` is used
/// - **+ documents** — when `_document` is projected
///
/// All existing optimizer rules (TopKPushdown, FastFieldFilterPushdown,
/// AggPushdown, OrdinalGroupByOptimization) continue working unchanged because
/// the physical plan shapes are identical to hand-written JOINs.
///
/// # Example
///
/// ```sql
/// SELECT id, price, _score, _document
/// FROM my_index
/// WHERE full_text(category, 'books') AND price > 2
/// ORDER BY _score DESC LIMIT 10
/// ```
pub struct UnifiedTantivyTableProvider {
    opener: Arc<dyn IndexOpener>,
    unified_schema: SchemaRef,
    fast_field_schema: SchemaRef,
    inverted_index_schema: SchemaRef,
    document_schema: SchemaRef,
    /// Index of `_score` in the unified schema.
    score_column_idx: usize,
    /// Index of `_document` in the unified schema.
    document_column_idx: usize,
    /// Optional stashed aggregations for the AggPushdown rule.
    aggregations: Option<Arc<Aggregations>>,
}

impl UnifiedTantivyTableProvider {
    /// Create a unified provider from an already-opened tantivy index.
    pub fn new(index: Index) -> Self {
        let ff_schema = tantivy_schema_to_arrow_from_index(&index);
        Self::from_opener_with_ff_schema(Arc::new(DirectIndexOpener::new(index)), ff_schema)
    }

    /// Create a unified provider from an [`IndexOpener`] for deferred opening.
    ///
    /// Uses `tantivy_schema_to_arrow` which cannot detect multi-valued fields
    /// (those require segment inspection). For multi-valued support, use `new()`.
    pub fn from_opener(opener: Arc<dyn IndexOpener>) -> Self {
        let ff_schema = tantivy_schema_to_arrow(&opener.schema());
        Self::from_opener_with_ff_schema(opener, ff_schema)
    }

    fn from_opener_with_ff_schema(
        opener: Arc<dyn IndexOpener>,
        fast_field_schema: SchemaRef,
    ) -> Self {
        let tantivy_schema = opener.schema();

        // Inverted index schema: _doc_id, _segment_ord, [virtual text cols], _score
        let inv_fields = TantivyInvertedIndexProvider::build_arrow_fields(&tantivy_schema);
        let inverted_index_schema = Arc::new(Schema::new(inv_fields));

        // Document schema: _doc_id, _segment_ord, _document
        let document_schema = Arc::new(Schema::new(vec![
            Field::new("_doc_id", DataType::UInt32, false),
            Field::new("_segment_ord", DataType::UInt32, false),
            Field::new("_document", DataType::Utf8, false),
        ]));

        // Unified schema: fast fields + _score + _document
        let mut unified_fields: Vec<Arc<Field>> = fast_field_schema.fields().to_vec();
        let score_column_idx = unified_fields.len();
        unified_fields.push(Arc::new(Field::new("_score", DataType::Float32, true)));
        let document_column_idx = unified_fields.len();
        unified_fields.push(Arc::new(Field::new("_document", DataType::Utf8, false)));

        let unified_schema = Arc::new(Schema::new(unified_fields));

        Self {
            opener,
            unified_schema,
            fast_field_schema,
            inverted_index_schema,
            document_schema,
            score_column_idx,
            document_column_idx,
            aggregations: None,
        }
    }

    /// Stash tantivy aggregations for the AggPushdown optimizer rule.
    pub fn set_aggregations(&mut self, aggs: Arc<Aggregations>) {
        self.aggregations = Some(aggs);
    }
}

impl fmt::Debug for UnifiedTantivyTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnifiedTantivyTableProvider")
            .field("unified_schema", &self.unified_schema)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Projection analysis
// ---------------------------------------------------------------------------

struct ProjectionAnalysis {
    needs_inverted_index: bool,
    needs_document: bool,
    /// Projection for FastFieldDataSource (indices into fast_field_schema).
    ff_projection: Option<Vec<usize>>,
    /// Projection for InvertedIndexDataSource (indices into inverted_index_schema).
    inv_projection: Option<Vec<usize>>,
    /// Projection for DocumentDataSource (indices into document_schema).
    doc_projection: Option<Vec<usize>>,
}

fn analyze_projection(
    unified_schema: &Schema,
    fast_field_schema: &Schema,
    inverted_index_schema: &Schema,
    _document_schema: &Schema,
    score_column_idx: usize,
    document_column_idx: usize,
    projection: Option<&Vec<usize>>,
    has_full_text: bool,
) -> ProjectionAnalysis {
    let projected_indices: Vec<usize> = match projection {
        Some(indices) => indices.clone(),
        None => (0..unified_schema.fields().len()).collect(),
    };

    let mut needs_score = false;
    let mut needs_document = false;
    let mut ff_indices = Vec::new();

    for &idx in &projected_indices {
        if idx == score_column_idx {
            needs_score = true;
        } else if idx == document_column_idx {
            needs_document = true;
        } else {
            // Fast field column — map unified index to fast field index.
            // The first N columns of unified schema are identical to fast_field_schema.
            ff_indices.push(idx);
        }
    }

    let needs_inverted_index = needs_score || has_full_text;

    // When joining, fast fields must include _doc_id and _segment_ord for join keys.
    let needs_join = needs_inverted_index || needs_document;
    if needs_join {
        let doc_id_idx = fast_field_schema.index_of("_doc_id").unwrap();
        let seg_ord_idx = fast_field_schema.index_of("_segment_ord").unwrap();
        if !ff_indices.contains(&doc_id_idx) {
            ff_indices.push(doc_id_idx);
        }
        if !ff_indices.contains(&seg_ord_idx) {
            ff_indices.push(seg_ord_idx);
        }
        ff_indices.sort();
    }

    let ff_projection = Some(ff_indices);

    // Inverted index: always project _doc_id, _segment_ord, and _score when needed.
    let inv_projection = if needs_inverted_index {
        let doc_id_idx = inverted_index_schema.index_of("_doc_id").unwrap();
        let seg_ord_idx = inverted_index_schema.index_of("_segment_ord").unwrap();
        let score_idx = inverted_index_schema.index_of("_score").unwrap();
        let mut indices = vec![doc_id_idx, seg_ord_idx];
        if needs_score {
            indices.push(score_idx);
        }
        Some(indices)
    } else {
        None
    };

    // Document: always project _doc_id, _segment_ord, _document.
    let doc_projection = if needs_document {
        // Project all columns (join keys + _document)
        None
    } else {
        None
    };

    ProjectionAnalysis {
        needs_inverted_index,
        needs_document,
        ff_projection,
        inv_projection,
        doc_projection,
    }
}

// ---------------------------------------------------------------------------
// TableProvider implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl TableProvider for UnifiedTantivyTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.unified_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if extract_full_text_call(f).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // 1. Extract full_text() calls from pushed-down filters.
        let tantivy_schema = self.opener.schema();
        let mut raw_queries: Vec<(String, String)> = Vec::new();
        for filter in filters {
            if let Some((field_name, query_string)) = extract_full_text_call(filter) {
                tantivy_schema.get_field(&field_name).map_err(|e| {
                    DataFusionError::Plan(format!(
                        "full_text: field '{field_name}' not found: {e}"
                    ))
                })?;
                raw_queries.push((field_name, query_string));
            }
        }
        let has_full_text = !raw_queries.is_empty();

        // 2. Analyze projection.
        let analysis = analyze_projection(
            &self.unified_schema,
            &self.fast_field_schema,
            &self.inverted_index_schema,
            &self.document_schema,
            self.score_column_idx,
            self.document_column_idx,
            projection,
            has_full_text,
        );

        // 3. Compute segments and partition ranges.
        let segment_sizes = self.opener.segment_sizes();
        let num_segments = segment_sizes.len().max(1);
        let needs_join = analysis.needs_inverted_index || analysis.needs_document;

        let target_partitions = state.config_options().execution.target_partitions;

        let partition_ranges = if needs_join {
            // 1 partition per segment — preserve co-partitioning for joins.
            (0..segment_sizes.len())
                .map(|seg| PartitionRange {
                    segment_idx: seg,
                    segment_ord: seg as u32,
                    doc_start: 0,
                    doc_end: segment_sizes[seg],
                })
                .collect()
        } else if segment_sizes.is_empty() {
            vec![PartitionRange {
                segment_idx: 0,
                segment_ord: 0,
                doc_start: 0,
                doc_end: 0,
            }]
        } else {
            // Chunk segments for parallelism when no joins needed.
            let chunks_per_segment = (target_partitions / num_segments).max(1);
            let mut ranges = Vec::new();
            for (seg, &max_doc) in segment_sizes.iter().enumerate() {
                let chunk_size = ((max_doc as usize) / chunks_per_segment).max(1) as u32;
                let mut start = 0u32;
                while start < max_doc {
                    let end = (start + chunk_size).min(max_doc);
                    ranges.push(PartitionRange {
                        segment_idx: seg,
                        segment_ord: seg as u32,
                        doc_start: start,
                        doc_end: end,
                    });
                    start = end;
                }
            }
            ranges
        };

        // 4. Build FastFieldDataSource.
        let ff_projected_schema = match &analysis.ff_projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| self.fast_field_schema.field(i).clone())
                    .collect();
                Arc::new(Schema::new(fields))
            }
            None => self.fast_field_schema.clone(),
        };

        let ff_ds = FastFieldDataSource::new(
            self.opener.clone(),
            self.fast_field_schema.clone(),
            ff_projected_schema,
            analysis.ff_projection.clone(),
            None, // no tantivy query (handled by inverted index side)
            limit,
            partition_ranges,
            self.aggregations.clone(),
        );
        let mut plan: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(Arc::new(ff_ds)));

        // Scenario A: fast fields only — return directly.
        if !analysis.needs_inverted_index && !analysis.needs_document {
            return Ok(plan);
        }

        let partition_mode = if num_segments <= 1 {
            PartitionMode::CollectLeft
        } else {
            PartitionMode::Partitioned
        };

        // 5. If inverted index needed, build inv ⋈ ff join.
        if analysis.needs_inverted_index {
            let inv_projected_schema = match &analysis.inv_projection {
                Some(indices) => {
                    let fields: Vec<_> = indices
                        .iter()
                        .map(|&i| self.inverted_index_schema.field(i).clone())
                        .collect();
                    Arc::new(Schema::new(fields))
                }
                None => self.inverted_index_schema.clone(),
            };

            let inv_ds = InvertedIndexDataSource::new(
                self.opener.clone(),
                self.inverted_index_schema.clone(),
                inv_projected_schema,
                analysis.inv_projection.clone(),
                raw_queries,
                num_segments,
            );
            let inv_exec: Arc<dyn ExecutionPlan> =
                Arc::new(DataSourceExec::new(Arc::new(inv_ds)));

            let join_on = build_join_on(&inv_exec.schema(), &plan.schema())?;
            plan = Arc::new(HashJoinExec::try_new(
                inv_exec,  // build (left)
                plan,      // probe (right)
                join_on,
                None,                             // no join filter
                &JoinType::Inner,
                None,                             // no projection
                partition_mode,
                NullEquality::NullEqualsNothing,
            )?);
        }

        // 6. If documents needed, build result ⋈ doc join.
        if analysis.needs_document {
            let doc_ds = DocumentDataSource::new(
                self.opener.clone(),
                self.document_schema.clone(),
                self.document_schema.clone(),
                analysis.doc_projection.clone(),
                num_segments,
            );
            let doc_exec: Arc<dyn ExecutionPlan> =
                Arc::new(DataSourceExec::new(Arc::new(doc_ds)));

            let join_on = build_join_on(&plan.schema(), &doc_exec.schema())?;
            plan = Arc::new(HashJoinExec::try_new(
                plan,      // build (left)
                doc_exec,  // probe (right)
                join_on,
                None,
                &JoinType::Inner,
                None,
                partition_mode,
                NullEquality::NullEqualsNothing,
            )?);
        }

        // 7. Add projection to map join output → user's requested columns.
        plan = build_output_projection(
            plan,
            &self.unified_schema,
            projection,
        )?;

        Ok(plan)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build join-on pairs: `[(left._doc_id, right._doc_id), (left._segment_ord, right._segment_ord)]`.
fn build_join_on(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
) -> Result<Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>> {
    let l_doc = left_schema.index_of("_doc_id").map_err(|e| {
        DataFusionError::Internal(format!("left schema missing _doc_id: {e}"))
    })?;
    let l_seg = left_schema.index_of("_segment_ord").map_err(|e| {
        DataFusionError::Internal(format!("left schema missing _segment_ord: {e}"))
    })?;
    let r_doc = right_schema.index_of("_doc_id").map_err(|e| {
        DataFusionError::Internal(format!("right schema missing _doc_id: {e}"))
    })?;
    let r_seg = right_schema.index_of("_segment_ord").map_err(|e| {
        DataFusionError::Internal(format!("right schema missing _segment_ord: {e}"))
    })?;

    Ok(vec![
        (
            Arc::new(Column::new("_doc_id", l_doc)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("_doc_id", r_doc)) as Arc<dyn PhysicalExpr>,
        ),
        (
            Arc::new(Column::new("_segment_ord", l_seg)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("_segment_ord", r_seg)) as Arc<dyn PhysicalExpr>,
        ),
    ])
}

/// Build a `ProjectionExec` that maps from the join output schema to the
/// user's requested columns in the unified schema.
fn build_output_projection(
    input: Arc<dyn ExecutionPlan>,
    unified_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();
    let requested: Vec<usize> = match projection {
        Some(indices) => indices.clone(),
        None => (0..unified_schema.fields().len()).collect(),
    };

    let projection_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = requested
        .iter()
        .map(|&unified_idx| {
            let field = unified_schema.field(unified_idx);
            let col_name = field.name();

            // Find the column in the join output schema.
            match input_schema.index_of(col_name) {
                Ok(input_idx) => Ok((
                    Arc::new(Column::new(col_name, input_idx)) as Arc<dyn PhysicalExpr>,
                    col_name.clone(),
                )),
                Err(_) => {
                    // Column not in join output — synthesize null.
                    // This handles _score when no inverted index is needed.
                    Ok((
                        Arc::new(expressions::Literal::new(
                            datafusion::common::ScalarValue::Float32(None),
                        )) as Arc<dyn PhysicalExpr>,
                        col_name.clone(),
                    ))
                }
            }
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(ProjectionExec::try_new(projection_exprs, input)?))
}
