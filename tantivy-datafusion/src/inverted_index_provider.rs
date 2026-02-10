use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{Float32Array, RecordBatch, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{Result, ScalarValue};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::memory::LazyMemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use parking_lot::RwLock;
use tantivy::query::QueryParser;
use tantivy::{Index, TERMINATED};

/// A DataFusion table function that runs a tantivy full-text query and returns
/// matching `(_doc_id, _segment_ord, _score)` tuples.
///
/// Register with:
/// ```ignore
/// ctx.register_udtf("tantivy_search", Arc::new(TantivySearchFunction::new(index)));
/// ```
///
/// Then query:
/// ```sql
/// SELECT * FROM tantivy_search('field_name', 'query string')
/// ```
pub struct TantivySearchFunction {
    index: Index,
}

impl TantivySearchFunction {
    pub fn new(index: Index) -> Self {
        Self { index }
    }
}

impl fmt::Debug for TantivySearchFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TantivySearchFunction").finish()
    }
}

impl TableFunctionImpl for TantivySearchFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if args.len() != 2 {
            return Err(DataFusionError::Plan(
                "tantivy_search requires 2 arguments: (field_name, query_string)".to_string(),
            ));
        }

        let field_name = match &args[0] {
            Expr::Literal(ScalarValue::Utf8(Some(s)), _) => s.clone(),
            _ => {
                return Err(DataFusionError::Plan(
                    "tantivy_search first argument must be a string literal".to_string(),
                ))
            }
        };

        let query_string = match &args[1] {
            Expr::Literal(ScalarValue::Utf8(Some(s)), _) => s.clone(),
            _ => {
                return Err(DataFusionError::Plan(
                    "tantivy_search second argument must be a string literal".to_string(),
                ))
            }
        };

        let schema = self.index.schema();
        let field = schema.get_field(&field_name).map_err(|e| {
            DataFusionError::Plan(format!("field '{field_name}' not found in index: {e}"))
        })?;

        let parser = QueryParser::for_index(&self.index, vec![field]);
        let query = parser.parse_query(&query_string).map_err(|e| {
            DataFusionError::Plan(format!("failed to parse query '{query_string}': {e}"))
        })?;

        Ok(Arc::new(InvertedIndexResultProvider {
            index: self.index.clone(),
            query: Arc::from(query),
        }))
    }
}

fn inverted_index_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("_doc_id", DataType::UInt32, false),
        Field::new("_segment_ord", DataType::UInt32, false),
        Field::new("_score", DataType::Float32, false),
    ]))
}

struct InvertedIndexResultProvider {
    index: Index,
    query: Arc<dyn tantivy::query::Query>,
}

impl fmt::Debug for InvertedIndexResultProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InvertedIndexResultProvider").finish()
    }
}

#[async_trait]
impl TableProvider for InvertedIndexResultProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        inverted_index_schema()
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
        let full_schema = inverted_index_schema();
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

        let reader = self
            .index
            .reader()
            .map_err(|e| DataFusionError::Internal(format!("open reader: {e}")))?;
        let num_segments = reader.searcher().segment_readers().len();

        let generators: Vec<
            Arc<RwLock<dyn datafusion::physical_plan::memory::LazyBatchGenerator>>,
        > = (0..num_segments)
            .map(|seg_idx| {
                let gen = InvertedIndexBatchGenerator {
                    index: self.index.clone(),
                    segment_idx: seg_idx,
                    query: Arc::from(self.query.box_clone()),
                    projection: projection.cloned(),
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

struct InvertedIndexBatchGenerator {
    index: Index,
    segment_idx: usize,
    query: Arc<dyn tantivy::query::Query>,
    projection: Option<Vec<usize>>,
    exhausted: bool,
}

impl fmt::Debug for InvertedIndexBatchGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InvertedIndexBatchGenerator")
            .field("segment_idx", &self.segment_idx)
            .field("exhausted", &self.exhausted)
            .finish()
    }
}

impl fmt::Display for InvertedIndexBatchGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "InvertedIndexBatchGenerator(segment={})",
            self.segment_idx
        )
    }
}

impl datafusion::physical_plan::memory::LazyBatchGenerator for InvertedIndexBatchGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
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

        let weight = self
            .query
            .weight(tantivy::query::EnableScoring::enabled_from_searcher(
                &searcher,
            ))
            .map_err(|e| DataFusionError::Internal(format!("create weight: {e}")))?;

        let mut scorer = weight
            .scorer(segment_reader, 1.0)
            .map_err(|e| DataFusionError::Internal(format!("create scorer: {e}")))?;

        let mut doc_ids = Vec::new();
        let mut scores = Vec::new();

        let mut doc = scorer.doc();
        while doc != TERMINATED {
            doc_ids.push(doc);
            scores.push(scorer.score());
            doc = scorer.advance();
        }

        if doc_ids.is_empty() {
            return Ok(None);
        }

        let seg_ord = self.segment_idx as u32;
        let full_schema = inverted_index_schema();

        // Build all three columns, then project
        let all_columns: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(UInt32Array::from(doc_ids)),
            Arc::new(UInt32Array::from(vec![seg_ord; scores.len()])),
            Arc::new(Float32Array::from(scores)),
        ];

        let (projected_schema, projected_columns) = match &self.projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| full_schema.field(i).clone())
                    .collect();
                let cols: Vec<_> = indices.iter().map(|&i| all_columns[i].clone()).collect();
                (Arc::new(Schema::new(fields)), cols)
            }
            None => (full_schema, all_columns),
        };

        let batch = RecordBatch::try_new(projected_schema, projected_columns)
            .map_err(|e| DataFusionError::Internal(format!("build record batch: {e}")))?;

        Ok(Some(batch))
    }
}
