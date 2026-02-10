use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use tantivy::query::{EnableScoring, Query};
use tantivy::{DocId, Index};

use crate::fast_field_reader::read_segment_fast_fields_to_batch;

/// An execution plan that reads tantivy fast fields as Arrow record batches.
///
/// Each tantivy segment maps to one partition, enabling parallel reads.
/// If a tantivy `Query` is set, only matching documents are materialized.
pub struct TantivyFastFieldExec {
    index: Index,
    projected_schema: SchemaRef,
    properties: PlanProperties,
    query: Option<Arc<dyn Query>>,
    /// Per-partition row limit. Each partition emits at most this many rows.
    limit: Option<usize>,
}

impl Clone for TantivyFastFieldExec {
    fn clone(&self) -> Self {
        Self {
            index: self.index.clone(),
            projected_schema: self.projected_schema.clone(),
            properties: self.properties.clone(),
            query: self.query.as_ref().map(|q| Arc::from(q.box_clone())),
            limit: self.limit,
        }
    }
}

impl fmt::Debug for TantivyFastFieldExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TantivyFastFieldExec")
            .field("projected_schema", &self.projected_schema)
            .field("query", &self.query)
            .finish()
    }
}

impl TantivyFastFieldExec {
    pub fn new(
        index: Index,
        projected_schema: SchemaRef,
        num_segments: usize,
        limit: Option<usize>,
    ) -> Self {
        Self::new_inner(index, projected_schema, None, num_segments, limit)
    }

    pub fn new_with_query(
        index: Index,
        projected_schema: SchemaRef,
        query: Box<dyn Query>,
        num_segments: usize,
        limit: Option<usize>,
    ) -> Self {
        Self::new_inner(index, projected_schema, Some(Arc::from(query)), num_segments, limit)
    }

    fn new_inner(
        index: Index,
        projected_schema: SchemaRef,
        query: Option<Arc<dyn Query>>,
        num_segments: usize,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(num_segments),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            index,
            projected_schema,
            properties,
            query,
            limit,
        }
    }
}

impl DisplayAs for TantivyFastFieldExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TantivyFastFieldExec: query={:?}, limit={:?}",
            self.query.as_ref().map(|q| format!("{q:?}")),
            self.limit,
        )
    }
}

impl ExecutionPlan for TantivyFastFieldExec {
    fn name(&self) -> &str {
        "TantivyFastFieldExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let reader = self
            .index
            .reader()
            .map_err(|e| DataFusionError::Internal(format!("open reader: {e}")))?;
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(partition as u32);
        let limit = self.limit;

        let doc_ids = match &self.query {
            Some(query) => {
                let tantivy_schema = self.index.schema();
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
                Some(matching_docs)
            }
            None => None,
        };

        // Apply limit to the doc list (handles the no-query path and caps the
        // query path in case for_each_no_score overshot slightly).
        let doc_ids = match (doc_ids, limit) {
            (Some(mut ids), Some(lim)) => {
                ids.truncate(lim);
                Some(ids)
            }
            (ids, _) => ids,
        };

        let batch = read_segment_fast_fields_to_batch(
            segment_reader,
            &self.projected_schema,
            doc_ids.as_deref(),
            limit,
        )?;

        let schema = self.projected_schema.clone();
        let stream = futures::stream::iter(vec![Ok(batch)]);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
