use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::memory::LazyMemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use parking_lot::RwLock;
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

        // One LazyBatchGenerator per segment
        let generators: Vec<Arc<RwLock<dyn datafusion::physical_plan::memory::LazyBatchGenerator>>> =
            (0..num_segments)
                .map(|seg_idx| {
                    let gen = FastFieldBatchGenerator {
                        index: self.index.clone(),
                        segment_idx: seg_idx,
                        projected_schema: projected_schema.clone(),
                        query: combined_query.as_ref().map(|q| Arc::from(q.box_clone())),
                        limit,
                        exhausted: false,
                    };
                    Arc::new(RwLock::new(gen))
                        as Arc<RwLock<dyn datafusion::physical_plan::memory::LazyBatchGenerator>>
                })
                .collect();

        Ok(Arc::new(LazyMemoryExec::try_new(
            projected_schema,
            generators,
        )?))
    }
}

/// Generates Arrow RecordBatches from tantivy fast fields for a single segment.
///
/// If a tantivy `Query` is set, only matching documents are read.
/// Otherwise all alive documents in the segment are read.
struct FastFieldBatchGenerator {
    index: Index,
    segment_idx: usize,
    projected_schema: SchemaRef,
    query: Option<Arc<dyn Query>>,
    limit: Option<usize>,
    exhausted: bool,
}

impl fmt::Debug for FastFieldBatchGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FastFieldBatchGenerator")
            .field("segment_idx", &self.segment_idx)
            .field("has_query", &self.query.is_some())
            .field("limit", &self.limit)
            .field("exhausted", &self.exhausted)
            .finish()
    }
}

impl fmt::Display for FastFieldBatchGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FastFieldBatchGenerator(segment={}, query={}, limit={:?})",
            self.segment_idx,
            self.query.is_some(),
            self.limit,
        )
    }
}

impl datafusion::physical_plan::memory::LazyBatchGenerator for FastFieldBatchGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(&mut self) -> Result<Option<arrow::record_batch::RecordBatch>> {
        if self.exhausted {
            return Ok(None);
        }
        self.exhausted = true;

        let reader = self
            .index
            .reader()
            .map_err(|e| DataFusionError::Internal(format!("open reader: {e}")))?;
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(self.segment_idx as u32);

        // If a query is set, run it to get matching doc IDs
        let doc_ids: Option<Vec<DocId>> = match &self.query {
            Some(query) => {
                let tantivy_schema = self.index.schema();
                let weight = query
                    .weight(EnableScoring::disabled_from_schema(&tantivy_schema))
                    .map_err(|e| DataFusionError::Internal(format!("create weight: {e}")))?;
                let limit = self.limit;
                let mut matching_docs: Vec<DocId> = Vec::new();
                weight
                    .for_each_no_score(segment_reader, &mut |docs| {
                        if let Some(lim) = limit {
                            let remaining = lim.saturating_sub(matching_docs.len());
                            if remaining > 0 {
                                matching_docs
                                    .extend_from_slice(&docs[..docs.len().min(remaining)]);
                            }
                        } else {
                            matching_docs.extend_from_slice(docs);
                        }
                    })
                    .map_err(|e| DataFusionError::Internal(format!("query execution: {e}")))?;
                if let Some(lim) = self.limit {
                    matching_docs.truncate(lim);
                }
                Some(matching_docs)
            }
            None => None,
        };

        let batch = read_segment_fast_fields_to_batch(
            segment_reader,
            &self.projected_schema,
            doc_ids.as_deref(),
            self.limit,
            self.segment_idx as u32,
        )?;

        if batch.num_rows() == 0 {
            return Ok(None);
        }

        Ok(Some(batch))
    }
}
