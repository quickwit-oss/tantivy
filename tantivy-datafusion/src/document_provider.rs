use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringBuilder, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::{Result, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::source::{DataSource, DataSourceExec};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::projection::ProjectionExpr;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayFormatType, Partitioning, SendableRecordBatchStream};
use futures::stream;
use tantivy::{Document, Index};

use crate::table_provider::segment_hash_partitioning;

/// A DataFusion table provider that returns full tantivy documents as JSON.
///
/// Exposes three columns:
/// - `_doc_id` (UInt32) — the segment-local document ID
/// - `_segment_ord` (UInt32) — the segment ordinal
/// - `_document` (Utf8) — the full document serialized as JSON
///
/// Join with the fast field provider or inverted index provider on
/// `(_doc_id, _segment_ord)` to retrieve document bodies after filtering.
///
/// Example:
/// ```sql
/// SELECT d._document
/// FROM inv
/// JOIN d ON d._doc_id = inv._doc_id AND d._segment_ord = inv._segment_ord
/// WHERE full_text(inv.category, 'electronics')
/// ```
pub struct TantivyDocumentProvider {
    index: Index,
    arrow_schema: SchemaRef,
}

impl TantivyDocumentProvider {
    pub fn new(index: Index) -> Self {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("_doc_id", DataType::UInt32, false),
            Field::new("_segment_ord", DataType::UInt32, false),
            Field::new("_document", DataType::Utf8, false),
        ]));
        Self { index, arrow_schema }
    }
}

impl fmt::Debug for TantivyDocumentProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TantivyDocumentProvider").finish()
    }
}

#[async_trait]
impl TableProvider for TantivyDocumentProvider {
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

        let reader = self
            .index
            .reader()
            .map_err(|e| DataFusionError::Internal(format!("open reader: {e}")))?;
        let num_segments = reader.searcher().segment_readers().len();

        let data_source = DocumentDataSource {
            index: self.index.clone(),
            schema: projected_schema,
            full_schema: self.arrow_schema.clone(),
            projection: projection.cloned(),
            num_segments,
        };
        Ok(Arc::new(DataSourceExec::new(Arc::new(data_source))))
    }
}

// ---------------------------------------------------------------------------
// DataSource implementation
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct DocumentDataSource {
    index: Index,
    schema: SchemaRef,
    full_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    num_segments: usize,
}

impl DataSource for DocumentDataSource {
    fn open(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let index = self.index.clone();
        let segment_idx = partition;
        let projection = self.projection.clone();
        let full_schema = self.full_schema.clone();

        let schema = self.schema.clone();
        let stream = stream::once(async move {
            generate_document_batch(&index, segment_idx, projection.as_deref(), &full_schema)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DocumentDataSource(segments={})",
            self.num_segments,
        )
    }

    fn output_partitioning(&self) -> Partitioning {
        segment_hash_partitioning(&self.schema, self.num_segments)
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new(self.schema.clone())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
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
        Ok(
            datafusion_physical_plan::filter_pushdown::FilterPushdownPropagation::with_parent_pushdown_result(
                vec![datafusion_physical_plan::filter_pushdown::PushedDown::No; filters.len()],
            ),
        )
    }
}

// ---------------------------------------------------------------------------
// Batch generation
// ---------------------------------------------------------------------------

fn generate_document_batch(
    index: &Index,
    segment_idx: usize,
    projection: Option<&[usize]>,
    full_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let reader = index
        .reader()
        .map_err(|e| DataFusionError::Internal(format!("open reader: {e}")))?;
    let searcher = reader.searcher();
    let segment_reader = searcher.segment_reader(segment_idx as u32);
    let tantivy_schema = index.schema();

    let max_doc = segment_reader.max_doc();
    let alive_bitset = segment_reader.alive_bitset();

    let doc_ids: Vec<u32> = (0..max_doc)
        .filter(|&doc_id| alive_bitset.map_or(true, |bitset| bitset.is_alive(doc_id)))
        .collect();

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

    let store_reader = segment_reader
        .get_store_reader(100)
        .map_err(|e| DataFusionError::Internal(format!("open store reader: {e}")))?;

    let num_docs = doc_ids.len();
    let seg_ord = segment_idx as u32;

    let mut doc_id_builder = Vec::with_capacity(num_docs);
    let mut seg_ord_builder = Vec::with_capacity(num_docs);
    let mut doc_builder = StringBuilder::with_capacity(num_docs, num_docs * 256);

    for &doc_id in &doc_ids {
        let doc: tantivy::TantivyDocument = store_reader.get(doc_id).map_err(|e| {
            DataFusionError::Internal(format!("read doc {doc_id}: {e}"))
        })?;
        let json = doc.to_json(&tantivy_schema);

        doc_id_builder.push(doc_id);
        seg_ord_builder.push(seg_ord);
        doc_builder.append_value(&json);
    }

    let all_columns: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(UInt32Array::from(doc_id_builder)),
        Arc::new(UInt32Array::from(seg_ord_builder)),
        Arc::new(doc_builder.finish()),
    ];

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
