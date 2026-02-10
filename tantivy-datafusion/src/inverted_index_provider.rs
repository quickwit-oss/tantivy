use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{new_null_array, RecordBatch, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::memory::LazyMemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use parking_lot::RwLock;
use tantivy::query::{BooleanQuery, EnableScoring, QueryParser};
use tantivy::schema::FieldType;
use tantivy::Index;

use crate::full_text_udf::extract_full_text_call;

/// A DataFusion table provider backed by a tantivy inverted index.
///
/// Exposes `_doc_id` and `_segment_ord` columns (for joining with the fast
/// field provider) plus one virtual Utf8 column per indexed text field.
/// The text columns exist so the optimizer can push `full_text(inv.col, ...)`
/// predicates down — they always return null when projected.
///
/// Register with:
/// ```ignore
/// ctx.register_table("inverted_index", Arc::new(TantivyInvertedIndexProvider::new(index)));
/// ```
///
/// Then query via an explicit join:
/// ```sql
/// SELECT f.id, f.price
/// FROM fast_fields f
/// JOIN inverted_index inv
///   ON f._doc_id = inv._doc_id AND f._segment_ord = inv._segment_ord
/// WHERE full_text(inv.category, 'electronics') AND f.price > 2.0
/// ```
pub struct TantivyInvertedIndexProvider {
    index: Index,
    arrow_schema: SchemaRef,
}

impl TantivyInvertedIndexProvider {
    pub fn new(index: Index) -> Self {
        let schema = index.schema();

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

        let arrow_schema = Arc::new(Schema::new(fields));
        Self { index, arrow_schema }
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

        // Extract full_text() calls from pushed-down filters
        let mut queries: Vec<Box<dyn tantivy::query::Query>> = Vec::new();
        let schema = self.index.schema();
        for filter in filters {
            if let Some((field_name, query_string)) = extract_full_text_call(filter) {
                let field = schema.get_field(&field_name).map_err(|e| {
                    DataFusionError::Plan(format!(
                        "full_text: field '{field_name}' not found: {e}"
                    ))
                })?;
                let parser = QueryParser::for_index(&self.index, vec![field]);
                let parsed = parser.parse_query(&query_string).map_err(|e| {
                    DataFusionError::Plan(format!(
                        "full_text: failed to parse '{query_string}': {e}"
                    ))
                })?;
                queries.push(parsed);
            }
        }

        let combined_query: Option<Arc<dyn tantivy::query::Query>> = match queries.len() {
            0 => None,
            1 => Some(Arc::from(queries.into_iter().next().unwrap())),
            _ => Some(Arc::new(BooleanQuery::intersection(queries))),
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
                    query: combined_query.as_ref().map(|q| Arc::from(q.box_clone())),
                    projection: projection.cloned(),
                    full_schema: self.arrow_schema.clone(),
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
    query: Option<Arc<dyn tantivy::query::Query>>,
    projection: Option<Vec<usize>>,
    full_schema: SchemaRef,
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

        let doc_ids: Vec<u32> = match &self.query {
            Some(query) => {
                // Run the query to get matching doc IDs (no scoring needed)
                let tantivy_schema = self.index.schema();
                let weight = query
                    .weight(EnableScoring::disabled_from_schema(&tantivy_schema))
                    .map_err(|e| DataFusionError::Internal(format!("create weight: {e}")))?;
                let mut matching_docs = Vec::new();
                weight
                    .for_each_no_score(segment_reader, &mut |docs| {
                        matching_docs.extend_from_slice(docs);
                    })
                    .map_err(|e| DataFusionError::Internal(format!("query execution: {e}")))?;
                matching_docs
            }
            None => {
                // Full scan: return all alive doc IDs
                let max_doc = segment_reader.max_doc();
                let alive_bitset = segment_reader.alive_bitset();
                (0..max_doc)
                    .filter(|&doc_id| {
                        alive_bitset
                            .map_or(true, |bitset| bitset.is_alive(doc_id))
                    })
                    .collect()
            }
        };

        if doc_ids.is_empty() {
            return Ok(None);
        }

        let num_docs = doc_ids.len();
        let seg_ord = self.segment_idx as u32;

        // Build all columns: _doc_id, _segment_ord, then null arrays for text columns
        let mut all_columns: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(UInt32Array::from(doc_ids)),
            Arc::new(UInt32Array::from(vec![seg_ord; num_docs])),
        ];

        // Add null arrays for each virtual text column
        for i in 2..self.full_schema.fields().len() {
            let field = self.full_schema.field(i);
            all_columns.push(new_null_array(field.data_type(), num_docs));
        }

        let (projected_schema, projected_columns) = match &self.projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| self.full_schema.field(i).clone())
                    .collect();
                let cols: Vec<_> = indices.iter().map(|&i| all_columns[i].clone()).collect();
                (Arc::new(Schema::new(fields)), cols)
            }
            None => (self.full_schema.clone(), all_columns),
        };

        let batch = RecordBatch::try_new(projected_schema, projected_columns)
            .map_err(|e| DataFusionError::Internal(format!("build record batch: {e}")))?;

        Ok(Some(batch))
    }
}
