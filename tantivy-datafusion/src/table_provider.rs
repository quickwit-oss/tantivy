use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use tantivy::Index;

use crate::exec::TantivyFastFieldExec;
use crate::schema_mapping::tantivy_schema_to_arrow;

/// A DataFusion table provider backed by a tantivy index.
///
/// Exposes fast fields from the index as Arrow columns.
pub struct TantivyTableProvider {
    index: Index,
    arrow_schema: SchemaRef,
}

impl fmt::Debug for TantivyTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TantivyTableProvider")
            .field("arrow_schema", &self.arrow_schema)
            .finish()
    }
}

impl TantivyTableProvider {
    pub fn new(index: Index) -> Self {
        let arrow_schema = tantivy_schema_to_arrow(&index.schema());
        Self {
            index,
            arrow_schema,
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
        _limit: Option<usize>,
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

        let reader = self
            .index
            .reader()
            .map_err(|e| datafusion::error::DataFusionError::Internal(format!("open reader: {e}")))?;
        let num_segments = reader.searcher().segment_readers().len();

        Ok(Arc::new(TantivyFastFieldExec::new(
            self.index.clone(),
            projected_schema,
            num_segments,
        )))
    }
}
