use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{new_null_array, Float32Array, RecordBatch, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::{Result, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::source::{DataSource, DataSourceExec};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::projection::ProjectionExpr;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayFormatType, Partitioning, SendableRecordBatchStream};
use futures::stream;
use tantivy::collector::TopNComputer;
use tantivy::query::{BooleanQuery, EnableScoring, QueryParser};
use tantivy::schema::FieldType;
use tantivy::{DocId, Index, Score};

use crate::full_text_udf::extract_full_text_call;
use crate::index_opener::{DirectIndexOpener, IndexOpener};
use crate::table_provider::segment_hash_partitioning;

/// A DataFusion table provider backed by a tantivy inverted index.
///
/// Exposes `_doc_id` and `_segment_ord` columns (for joining with the fast
/// field provider) plus one virtual Utf8 column per indexed text field,
/// and a `_score` column (Float32, nullable) for BM25 relevance scores.
///
/// The text columns exist so the optimizer can push `full_text(inv.col, ...)`
/// predicates down — they always return null when projected.
/// The `_score` column is populated with BM25 scores when a query is active
/// and `_score` is projected; otherwise it is null.
///
/// Register with:
/// ```ignore
/// ctx.register_table("inverted_index", Arc::new(TantivyInvertedIndexProvider::new(index)));
/// ```
///
/// Then query via an explicit join:
/// ```sql
/// SELECT f.id, f.price, inv._score
/// FROM fast_fields f
/// JOIN inverted_index inv
///   ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord
/// WHERE full_text(inv.category, 'electronics') AND f.price > 2.0
/// ORDER BY inv._score DESC LIMIT 10
/// ```
pub struct TantivyInvertedIndexProvider {
    opener: Arc<dyn IndexOpener>,
    arrow_schema: SchemaRef,
}

impl TantivyInvertedIndexProvider {
    pub fn new(index: Index) -> Self {
        Self::from_opener(Arc::new(DirectIndexOpener::new(index)))
    }

    /// Create a provider from an [`IndexOpener`] for deferred index opening.
    pub fn from_opener(opener: Arc<dyn IndexOpener>) -> Self {
        let schema = opener.schema();
        let arrow_schema = Arc::new(Schema::new(Self::build_arrow_fields(&schema)));
        Self { opener, arrow_schema }
    }

    fn build_arrow_fields(schema: &tantivy::schema::Schema) -> Vec<Field> {
        let mut fields: Vec<Field> = vec![
            Field::new("_doc_id", DataType::UInt32, false),
            Field::new("_segment_ord", DataType::UInt32, false),
        ];

        // Add one Utf8 column per indexed text field
        for (_field, field_entry) in schema.fields() {
            if field_entry.is_indexed() {
                if let FieldType::Str(_) = field_entry.field_type() {
                    fields.push(Field::new(field_entry.name(), DataType::Utf8, true));
                }
            }
        }

        // _score column — nullable Float32 for BM25 relevance scores
        fields.push(Field::new("_score", DataType::Float32, true));

        fields
    }
}

impl fmt::Debug for TantivyInvertedIndexProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TantivyInvertedIndexProvider")
            .field("arrow_schema", &self.arrow_schema)
            .finish()
    }
}

#[async_trait]
impl TableProvider for TantivyInvertedIndexProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| self.arrow_schema.field(i).clone())
                    .collect();
                Arc::new(Schema::new(fields))
            }
            None => self.arrow_schema.clone(),
        };

        // Extract full_text() calls from pushed-down filters.
        // Validate field names against the schema now (planning time),
        // but defer query parsing to execution time (open()).
        let tantivy_schema = self.opener.schema();
        let mut raw_queries: Vec<(String, String)> = Vec::new();
        for filter in filters {
            if let Some((field_name, query_string)) = extract_full_text_call(filter) {
                // Validate field exists in schema
                tantivy_schema.get_field(&field_name).map_err(|e| {
                    DataFusionError::Plan(format!(
                        "full_text: field '{field_name}' not found: {e}"
                    ))
                })?;
                raw_queries.push((field_name, query_string));
            }
        }

        let segment_sizes = self.opener.segment_sizes();
        let num_segments = segment_sizes.len().max(1);

        let data_source = InvertedIndexDataSource {
            opener: self.opener.clone(),
            full_schema: self.arrow_schema.clone(),
            projected_schema,
            projection: projection.cloned(),
            query: None,
            raw_queries,
            num_segments,
            topk: None,
        };
        Ok(Arc::new(DataSourceExec::new(Arc::new(data_source))))
    }
}

// ---------------------------------------------------------------------------
// DataSource implementation
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct InvertedIndexDataSource {
    opener: Arc<dyn IndexOpener>,
    full_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    pub(crate) query: Option<Arc<dyn tantivy::query::Query>>,
    /// Raw `(field_name, query_string)` pairs deferred from scan time.
    /// Parsed into tantivy queries at execution time (in `open()`).
    raw_queries: Vec<(String, String)>,
    num_segments: usize,
    pub(crate) topk: Option<usize>,
}

impl InvertedIndexDataSource {
    /// Create a copy with the topk limit set.
    pub(crate) fn with_topk(&self, topk: usize) -> Self {
        InvertedIndexDataSource {
            opener: self.opener.clone(),
            full_schema: self.full_schema.clone(),
            projected_schema: self.projected_schema.clone(),
            projection: self.projection.clone(),
            query: self.query.as_ref().map(|q| Arc::from(q.box_clone())),
            raw_queries: self.raw_queries.clone(),
            num_segments: self.num_segments,
            topk: Some(topk),
        }
    }

    /// Access the index opener (for schema inspection by optimizer rules).
    pub(crate) fn opener(&self) -> &Arc<dyn IndexOpener> {
        &self.opener
    }

    /// Whether this data source has an active query (pre-parsed or deferred).
    pub(crate) fn has_query(&self) -> bool {
        self.query.is_some() || !self.raw_queries.is_empty()
    }

    /// Create a copy with additional tantivy queries combined via intersection.
    pub(crate) fn with_additional_queries(
        &self,
        extra: Vec<Box<dyn tantivy::query::Query>>,
    ) -> Self {
        if extra.is_empty() {
            return self.with_topk(self.topk.unwrap_or(0));
        }
        let mut all_queries: Vec<Box<dyn tantivy::query::Query>> = Vec::new();
        if let Some(q) = &self.query {
            all_queries.push(q.box_clone());
        }
        all_queries.extend(extra);
        let combined: Arc<dyn tantivy::query::Query> =
            Arc::new(BooleanQuery::intersection(all_queries));
        InvertedIndexDataSource {
            opener: self.opener.clone(),
            full_schema: self.full_schema.clone(),
            projected_schema: self.projected_schema.clone(),
            projection: self.projection.clone(),
            query: Some(combined),
            raw_queries: self.raw_queries.clone(),
            num_segments: self.num_segments,
            topk: self.topk,
        }
    }
}

impl DataSource for InvertedIndexDataSource {
    fn open(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let opener = self.opener.clone();
        let segment_idx = partition;
        let pre_parsed_query = self.query.as_ref().map(|q| Arc::from(q.box_clone()));
        let raw_queries = self.raw_queries.clone();
        let projection = self.projection.clone();
        let full_schema = self.full_schema.clone();
        let topk = self.topk;

        let schema = self.projected_schema.clone();
        let stream = stream::once(async move {
            let index = opener.open().await?;

            // Parse deferred raw queries now that we have an opened Index
            let query = build_combined_query(
                &index,
                pre_parsed_query.as_ref(),
                &raw_queries,
            )?;

            generate_inverted_index_batch(
                &index,
                segment_idx,
                query.as_ref(),
                projection.as_deref(),
                &full_schema,
                topk,
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
            "InvertedIndexDataSource(segments={}, query={}, topk={:?})",
            self.num_segments,
            self.has_query(),
            self.topk,
        )
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
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
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
        filters: Vec<Arc<dyn datafusion_physical_expr::PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<
        datafusion_physical_plan::filter_pushdown::FilterPushdownPropagation<
            Arc<dyn DataSource>,
        >,
    > {
        // The inverted index doesn't apply dynamic filters — it's always
        // the build side of the join. Return No for all filters.
        Ok(
            datafusion_physical_plan::filter_pushdown::FilterPushdownPropagation::with_parent_pushdown_result(
                vec![datafusion_physical_plan::filter_pushdown::PushedDown::No; filters.len()],
            ),
        )
    }
}

// ---------------------------------------------------------------------------
// Query parsing (deferred to execution time)
// ---------------------------------------------------------------------------

/// Combine pre-parsed queries with raw query strings into a single tantivy query.
/// Raw queries are parsed using `QueryParser::for_index` which requires an opened `Index`.
fn build_combined_query(
    index: &Index,
    pre_parsed: Option<&Arc<dyn tantivy::query::Query>>,
    raw_queries: &[(String, String)],
) -> Result<Option<Arc<dyn tantivy::query::Query>>> {
    let mut queries: Vec<Box<dyn tantivy::query::Query>> = Vec::new();

    if let Some(q) = pre_parsed {
        queries.push(q.box_clone());
    }

    let tantivy_schema = index.schema();
    for (field_name, query_string) in raw_queries {
        let field = tantivy_schema.get_field(field_name).map_err(|e| {
            DataFusionError::Plan(format!(
                "full_text: field '{field_name}' not found: {e}"
            ))
        })?;
        let parser = QueryParser::for_index(index, vec![field]);
        let parsed = parser.parse_query(query_string).map_err(|e| {
            DataFusionError::Plan(format!(
                "full_text: failed to parse '{query_string}': {e}"
            ))
        })?;
        queries.push(parsed);
    }

    match queries.len() {
        0 => Ok(None),
        1 => Ok(Some(Arc::from(queries.into_iter().next().unwrap()))),
        _ => Ok(Some(Arc::new(BooleanQuery::intersection(queries)))),
    }
}

// ---------------------------------------------------------------------------
// Batch generation
// ---------------------------------------------------------------------------

fn generate_inverted_index_batch(
    index: &Index,
    segment_idx: usize,
    query: Option<&Arc<dyn tantivy::query::Query>>,
    projection: Option<&[usize]>,
    full_schema: &SchemaRef,
    topk: Option<usize>,
) -> Result<RecordBatch> {
    let reader = index
        .reader()
        .map_err(|e| DataFusionError::Internal(format!("open reader: {e}")))?;
    let searcher = reader.searcher();
    let segment_reader = searcher.segment_reader(segment_idx as u32);

    // Determine if _score is projected
    let score_col_idx = full_schema.index_of("_score").unwrap();
    let needs_scoring = projection.map_or(true, |p| p.contains(&score_col_idx));

    // Collect matching docs (with optional scores)
    let (doc_ids, scores): (Vec<u32>, Option<Vec<f32>>) = match query {
        Some(query) => {
            if needs_scoring {
                // BM25 scoring enabled
                let weight = query
                    .weight(EnableScoring::enabled_from_searcher(&searcher))
                    .map_err(|e| DataFusionError::Internal(format!("create weight: {e}")))?;

                if let Some(k) = topk {
                    // TopK with Block-WAND pruning via TopNComputer
                    let mut top_n: TopNComputer<Score, DocId, _> =
                        TopNComputer::new(k);

                    let alive_bitset = segment_reader.alive_bitset();
                    if let Some(alive_bitset) = alive_bitset {
                        let mut threshold = Score::MIN;
                        top_n.threshold = Some(threshold);
                        weight
                            .for_each_pruning(
                                Score::MIN,
                                segment_reader,
                                &mut |doc, score| {
                                    if alive_bitset.is_deleted(doc) {
                                        return threshold;
                                    }
                                    top_n.push(score, doc);
                                    threshold =
                                        top_n.threshold.unwrap_or(Score::MIN);
                                    threshold
                                },
                            )
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "topk query execution: {e}"
                                ))
                            })?;
                    } else {
                        weight
                            .for_each_pruning(
                                Score::MIN,
                                segment_reader,
                                &mut |doc, score| {
                                    top_n.push(score, doc);
                                    top_n.threshold.unwrap_or(Score::MIN)
                                },
                            )
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "topk query execution: {e}"
                                ))
                            })?;
                    }

                    let results = top_n.into_sorted_vec();
                    let mut ids = Vec::with_capacity(results.len());
                    let mut sc = Vec::with_capacity(results.len());
                    for item in results {
                        ids.push(item.doc);
                        sc.push(item.sort_key);
                    }
                    (ids, Some(sc))
                } else {
                    // Full scoring without topK
                    let mut ids = Vec::new();
                    let mut sc = Vec::new();
                    weight
                        .for_each(segment_reader, &mut |doc, score| {
                            ids.push(doc);
                            sc.push(score);
                        })
                        .map_err(|e| {
                            DataFusionError::Internal(format!("query execution: {e}"))
                        })?;

                    // Filter deleted docs
                    if let Some(alive_bitset) = segment_reader.alive_bitset() {
                        let mut filtered_ids = Vec::new();
                        let mut filtered_sc = Vec::new();
                        for (doc, score) in ids.into_iter().zip(sc) {
                            if alive_bitset.is_alive(doc) {
                                filtered_ids.push(doc);
                                filtered_sc.push(score);
                            }
                        }
                        (filtered_ids, Some(filtered_sc))
                    } else {
                        (ids, Some(sc))
                    }
                }
            } else {
                // No scoring needed — boolean filter only
                let tantivy_schema = index.schema();
                let weight = query
                    .weight(EnableScoring::disabled_from_schema(&tantivy_schema))
                    .map_err(|e| DataFusionError::Internal(format!("create weight: {e}")))?;
                let mut matching_docs = Vec::new();
                weight
                    .for_each_no_score(segment_reader, &mut |docs| {
                        matching_docs.extend_from_slice(docs);
                    })
                    .map_err(|e| {
                        DataFusionError::Internal(format!("query execution: {e}"))
                    })?;
                // Filter deleted docs
                if let Some(alive_bitset) = segment_reader.alive_bitset() {
                    matching_docs.retain(|&doc| alive_bitset.is_alive(doc));
                }
                (matching_docs, None)
            }
        }
        None => {
            // No query — iterate all alive docs, scores are null
            let max_doc = segment_reader.max_doc();
            let alive_bitset = segment_reader.alive_bitset();
            let ids: Vec<u32> = (0..max_doc)
                .filter(|&doc_id| {
                    alive_bitset.map_or(true, |bitset| bitset.is_alive(doc_id))
                })
                .collect();
            (ids, None)
        }
    };

    if doc_ids.is_empty() {
        let projected_schema = match projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| full_schema.field(i).clone())
                    .collect();
                Arc::new(Schema::new(fields))
            }
            None => full_schema.clone(),
        };
        return Ok(RecordBatch::new_empty(projected_schema));
    }

    let num_docs = doc_ids.len();
    let seg_ord = segment_idx as u32;

    // Build all columns: _doc_id, _segment_ord, null text arrays, _score
    let mut all_columns: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(UInt32Array::from(doc_ids)),
        Arc::new(UInt32Array::from(vec![seg_ord; num_docs])),
    ];

    // Add null arrays for each virtual text column (all columns between
    // _segment_ord and _score)
    for i in 2..full_schema.fields().len() - 1 {
        let field = full_schema.field(i);
        all_columns.push(new_null_array(field.data_type(), num_docs));
    }

    // _score column
    match scores {
        Some(s) => all_columns.push(Arc::new(Float32Array::from(s))),
        None => all_columns.push(new_null_array(&DataType::Float32, num_docs)),
    }

    let (projected_schema, projected_columns) = match projection {
        Some(indices) => {
            let fields: Vec<_> = indices
                .iter()
                .map(|&i| full_schema.field(i).clone())
                .collect();
            let cols: Vec<_> = indices.iter().map(|&i| all_columns[i].clone()).collect();
            (Arc::new(Schema::new(fields)), cols)
        }
        None => (full_schema.clone(), all_columns),
    };

    RecordBatch::try_new(projected_schema, projected_columns)
        .map_err(|e| DataFusionError::Internal(format!("build record batch: {e}")))
}
