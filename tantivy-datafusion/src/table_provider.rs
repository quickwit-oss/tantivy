use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use datafusion::datasource::TableType;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use tantivy::query::{BooleanQuery, Query};
use tantivy::Index;

use crate::exec::TantivyFastFieldExec;
use crate::expr_to_tantivy::{can_convert_expr, df_expr_to_tantivy_query};
use crate::schema_mapping::tantivy_schema_to_arrow_from_index;

/// A DataFusion table provider backed by a tantivy index.
///
/// Exposes fast fields from the index as Arrow columns. Supports two modes
/// of query pushdown:
///
/// 1. **SQL filter pushdown**: DataFusion passes `WHERE` clause filters to
///    `scan()`, which converts supported expressions to tantivy queries.
/// 2. **Direct tantivy query**: Pass a `Box<dyn Query>` via `new_with_query()`
///    to filter at the segment level without Expr conversion.
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let tantivy_schema = self.index.schema();
        Ok(filters
            .iter()
            .map(|f| {
                if can_convert_expr(f, &tantivy_schema, &self.arrow_schema) {
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
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => self.arrow_schema.clone(),
        };

        let reader = self
            .index
            .reader()
            .map_err(|e| datafusion::error::DataFusionError::Internal(format!("open reader: {e}")))?;
        let num_segments = reader.searcher().segment_readers().len();

        // Convert pushed-down DF filters to tantivy queries
        let tantivy_schema = self.index.schema();
        let mut tantivy_queries: Vec<Box<dyn Query>> = Vec::new();

        // Include the pre-set direct query if present
        if let Some(q) = &self.query {
            tantivy_queries.push(q.box_clone());
        }

        // Convert each DF filter expression
        for filter in filters {
            if let Some(q) = df_expr_to_tantivy_query(filter, &tantivy_schema)? {
                tantivy_queries.push(q);
            }
        }

        // Combine into a single query
        let combined_query = match tantivy_queries.len() {
            0 => None,
            1 => Some(tantivy_queries.into_iter().next().unwrap()),
            _ => Some(Box::new(BooleanQuery::intersection(tantivy_queries)) as Box<dyn Query>),
        };

        let exec = match combined_query {
            Some(q) => TantivyFastFieldExec::new_with_query(
                self.index.clone(),
                projected_schema,
                q,
                num_segments,
            ),
            None => TantivyFastFieldExec::new(
                self.index.clone(),
                projected_schema,
                num_segments,
            ),
        };

        Ok(Arc::new(exec))
    }
}
