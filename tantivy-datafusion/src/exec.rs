use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::physical_expr::EquivalenceProperties;
use tantivy::Index;

use crate::fast_field_reader::read_segment_fast_fields_to_batch;

/// An execution plan that reads tantivy fast fields as Arrow record batches.
///
/// Each tantivy segment maps to one partition, enabling parallel reads.
#[derive(Debug, Clone)]
pub struct TantivyFastFieldExec {
    index: Index,
    projected_schema: SchemaRef,
    properties: PlanProperties,
}

impl TantivyFastFieldExec {
    pub fn new(index: Index, projected_schema: SchemaRef, num_segments: usize) -> Self {
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
        }
    }
}

impl DisplayAs for TantivyFastFieldExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TantivyFastFieldExec")
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
            .map_err(|e| datafusion::error::DataFusionError::Internal(format!("open reader: {e}")))?;
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(partition as u32);
        let batch = read_segment_fast_fields_to_batch(segment_reader, &self.projected_schema)?;

        let schema = self.projected_schema.clone();
        let stream = futures::stream::iter(vec![Ok(batch)]);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
