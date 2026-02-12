use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{Result, Statistics};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::Partitioning;
use futures::stream;
use tantivy::aggregation::agg_req::{Aggregation, AggregationVariants, Aggregations};
use tantivy::aggregation::agg_result::{
    AggregationResult, AggregationResults, BucketEntry, BucketResult, MetricResult,
    RangeBucketEntry,
};
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::aggregation::metric::{PercentilesMetricResult, SingleMetricResult};
use tantivy::aggregation::{AggContextParams, AggregationSegmentCollector, Key};
use tantivy::collector::SegmentCollector;
use tantivy::query::{EnableScoring, Query};
use tantivy::Index;

use crate::index_opener::IndexOpener;

/// A custom leaf `ExecutionPlan` that calls tantivy's native
/// `AggregationSegmentCollector` directly, bypassing DataFusion's
/// hash-based `AggregateExec` for near-native aggregation performance.
pub(crate) struct TantivyAggregateExec {
    opener: Arc<dyn IndexOpener>,
    aggregations: Arc<Aggregations>,
    query: Option<Arc<dyn Query>>,
    output_schema: SchemaRef,
    properties: PlanProperties,
}

impl TantivyAggregateExec {
    pub fn new(
        opener: Arc<dyn IndexOpener>,
        aggregations: Arc<Aggregations>,
        query: Option<Arc<dyn Query>>,
        output_schema: SchemaRef,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            opener,
            aggregations,
            query,
            output_schema,
            properties,
        }
    }
}

impl fmt::Debug for TantivyAggregateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TantivyAggregateExec")
            .field("output_schema", &self.output_schema)
            .field("has_query", &self.query.is_some())
            .finish()
    }
}

impl DisplayAs for TantivyAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TantivyAggregateExec(aggs={}, query={})",
            self.aggregations.len(),
            self.query.is_some(),
        )
    }
}

impl ExecutionPlan for TantivyAggregateExec {
    fn name(&self) -> &str {
        "TantivyAggregateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "TantivyAggregateExec has no children".into(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "TantivyAggregateExec only supports partition 0, got {partition}"
            )));
        }

        let opener = self.opener.clone();
        let aggs = self.aggregations.clone();
        let query = self.query.as_ref().map(|q| Arc::from(q.box_clone()));
        let schema = self.output_schema.clone();

        let stream = stream::once(async move {
            let index = opener.open().await?;
            execute_tantivy_agg(&index, &aggs, query.as_ref(), &schema)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.output_schema))
    }
}

// ---------------------------------------------------------------------------
// Core tantivy aggregation execution
// ---------------------------------------------------------------------------

fn execute_tantivy_agg(
    index: &Index,
    aggs: &Aggregations,
    query: Option<&Arc<dyn Query>>,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let reader = index
        .reader()
        .map_err(|e| DataFusionError::Internal(format!("open reader: {e}")))?;
    let searcher = reader.searcher();

    let context = AggContextParams::default();
    let mut intermediates: Vec<IntermediateAggregationResults> = Vec::new();

    for (seg_ord, seg_reader) in searcher.segment_readers().iter().enumerate() {
        let mut collector = AggregationSegmentCollector::from_agg_req_and_reader(
            aggs,
            seg_reader,
            seg_ord as u32,
            &context,
        )
        .map_err(|e| DataFusionError::Internal(format!("create segment collector: {e}")))?;

        // Stream doc IDs in cache-friendly chunks via for_each_no_score,
        // matching tantivy's native collection pattern (512-doc blocks).
        collect_into_segment_collector(seg_reader, query, &index.schema(), &mut collector)
            .map_err(|e| DataFusionError::Internal(format!("collect segment: {e}")))?;

        let intermediate = collector
            .harvest()
            .map_err(|e| DataFusionError::Internal(format!("harvest segment results: {e}")))?;
        intermediates.push(intermediate);
    }

    // Merge across segments
    let merged = merge_intermediates(intermediates)
        .map_err(|e| DataFusionError::Internal(format!("merge intermediates: {e}")))?;

    // Convert to final results
    let final_result = merged
        .into_final_result(aggs.clone(), Default::default())
        .map_err(|e| DataFusionError::Internal(format!("finalize results: {e}")))?;

    agg_results_to_batch(&final_result, aggs, output_schema)
}

/// Stream doc IDs into a segment collector in cache-friendly blocks,
/// matching tantivy's native collection pattern.
///
/// When a query is set, uses `for_each_no_score` which delivers docs in
/// ~512-doc chunks directly from the scorer. When no query, generates
/// chunks from the alive doc ID range.
const COLLECT_BLOCK_SIZE: usize = 512;

fn collect_into_segment_collector(
    seg_reader: &tantivy::SegmentReader,
    query: Option<&Arc<dyn Query>>,
    schema: &tantivy::schema::Schema,
    collector: &mut AggregationSegmentCollector,
) -> tantivy::Result<()> {
    let alive_bitset = seg_reader.alive_bitset();

    match query {
        Some(q) => {
            let weight = q.weight(EnableScoring::disabled_from_schema(schema))?;
            // for_each_no_score delivers docs in ~512-doc chunks from the scorer.
            // Feed each chunk directly to the collector — no intermediate Vec.
            if let Some(alive) = alive_bitset {
                let mut buf = Vec::with_capacity(COLLECT_BLOCK_SIZE);
                weight.for_each_no_score(seg_reader, &mut |docs| {
                    buf.clear();
                    buf.extend(docs.iter().filter(|&&d| alive.is_alive(d)));
                    collector.collect_block(&buf);
                })?;
            } else {
                weight.for_each_no_score(seg_reader, &mut |docs| {
                    collector.collect_block(docs);
                })?;
            }
        }
        None => {
            let max_doc = seg_reader.max_doc();
            let mut block = Vec::with_capacity(COLLECT_BLOCK_SIZE);
            for doc_id in 0..max_doc {
                if alive_bitset.map_or(true, |b| b.is_alive(doc_id)) {
                    block.push(doc_id);
                    if block.len() == COLLECT_BLOCK_SIZE {
                        collector.collect_block(&block);
                        block.clear();
                    }
                }
            }
            if !block.is_empty() {
                collector.collect_block(&block);
            }
        }
    }
    Ok(())
}

/// Merge multiple segment intermediate results into one.
fn merge_intermediates(
    mut intermediates: Vec<IntermediateAggregationResults>,
) -> tantivy::Result<IntermediateAggregationResults> {
    if intermediates.is_empty() {
        return Ok(IntermediateAggregationResults::default());
    }
    let mut merged = intermediates.pop().unwrap();
    for other in intermediates {
        merged.merge_fruits(other)?;
    }
    Ok(merged)
}

// ---------------------------------------------------------------------------
// AggregationResults → Arrow RecordBatch conversion
// ---------------------------------------------------------------------------

/// Convert tantivy `AggregationResults` into an Arrow `RecordBatch`
/// matching the schema produced by `translate_aggregations`.
fn agg_results_to_batch(
    results: &AggregationResults,
    aggs: &Aggregations,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    // The schema has columns from `translate_aggregations`.
    // We need to map the tantivy result structure to those columns.
    //
    // For a single top-level agg (the common case when optimizer replaces
    // one AggregateExec), the schema columns come directly from that agg.

    // Determine the agg type. Since this exec replaces a single DataFrame's
    // AggregateExec, we operate on a single aggregation key.
    // The optimizer should only replace plans for a single top-level agg.
    let (agg_name, agg_def) = aggs
        .iter()
        .next()
        .ok_or_else(|| DataFusionError::Internal("empty aggregations".into()))?;

    let agg_result = results
        .0
        .get(agg_name)
        .ok_or_else(|| DataFusionError::Internal(format!("missing result for '{agg_name}'")))?;

    match (&agg_def.agg, agg_result) {
        // Metric-only aggregations → 1 row
        (_, AggregationResult::MetricResult(metric)) => {
            metric_to_batch(metric, agg_name, agg_def, schema)
        }
        // Bucket aggregations → N rows
        (_, AggregationResult::BucketResult(bucket)) => {
            bucket_to_batch(bucket, agg_def, schema)
        }
    }
}

/// Convert a metric result to a single-row RecordBatch.
fn metric_to_batch(
    metric: &MetricResult,
    name: &str,
    _agg_def: &Aggregation,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let col_name = field.name();
        let value = extract_metric_value(metric, name, col_name);
        let array = scalar_to_array(value, field.data_type());
        columns.push(array);
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::Internal(format!("build metric batch: {e}")))
}

/// Extract a single f64 value from a MetricResult for a given column name.
fn extract_metric_value(metric: &MetricResult, agg_name: &str, col_name: &str) -> Option<f64> {
    match metric {
        MetricResult::Average(SingleMetricResult { value })
        | MetricResult::Sum(SingleMetricResult { value })
        | MetricResult::Min(SingleMetricResult { value })
        | MetricResult::Max(SingleMetricResult { value })
        | MetricResult::Count(SingleMetricResult { value })
        | MetricResult::Cardinality(SingleMetricResult { value }) => *value,

        MetricResult::Stats(stats) => {
            // Schema columns: {name}_min, {name}_max, {name}_sum, {name}_count, {name}_avg
            let suffix = col_name.strip_prefix(&format!("{agg_name}_")).unwrap_or(col_name);
            match suffix {
                "min" => stats.min,
                "max" => stats.max,
                "sum" => Some(stats.sum),
                "count" => Some(stats.count as f64),
                "avg" => stats.avg,
                _ => None,
            }
        }

        MetricResult::ExtendedStats(es) => {
            let suffix = col_name.strip_prefix(&format!("{agg_name}_")).unwrap_or(col_name);
            match suffix {
                "min" => es.min,
                "max" => es.max,
                "sum" => Some(es.sum),
                "count" => Some(es.count as f64),
                "avg" => es.avg,
                "variance_population" => es.variance_population,
                "std_deviation_population" => es.std_deviation_population,
                _ => None,
            }
        }

        MetricResult::Percentiles(p) => extract_percentile_value(p, agg_name, col_name),

        _ => None,
    }
}

fn extract_percentile_value(
    p: &PercentilesMetricResult,
    agg_name: &str,
    col_name: &str,
) -> Option<f64> {
    // Column names like "{name}_p1", "{name}_p50", etc.
    let suffix = col_name.strip_prefix(&format!("{agg_name}_p"))?;
    match &p.values {
        tantivy::aggregation::metric::PercentileValues::Vec(entries) => {
            for entry in entries {
                let key_str = if entry.key == entry.key.floor() {
                    format!("{}", entry.key as i64)
                } else {
                    format!("{}", entry.key)
                };
                if key_str == suffix {
                    return if entry.value.is_nan() {
                        None
                    } else {
                        Some(entry.value)
                    };
                }
            }
            None
        }
        tantivy::aggregation::metric::PercentileValues::HashMap(map) => {
            map.get(suffix).copied()
        }
    }
}

/// Convert an Option<f64> to a single-element Arrow array of the target type.
fn scalar_to_array(value: Option<f64>, data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Float64 => Arc::new(Float64Array::from(vec![value])),
        DataType::Int64 => Arc::new(Int64Array::from(vec![value.map(|v| v as i64)])),
        DataType::UInt64 => Arc::new(UInt64Array::from(vec![value.map(|v| v as u64)])),
        // Fallback: use Float64
        _ => Arc::new(Float64Array::from(vec![value])),
    }
}

/// Convert a bucket result to an N-row RecordBatch.
fn bucket_to_batch(
    bucket: &BucketResult,
    agg_def: &Aggregation,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    match bucket {
        BucketResult::Terms {
            buckets,
            sum_other_doc_count: _,
            ..
        } => terms_bucket_to_batch(buckets, agg_def, schema),
        BucketResult::Histogram { buckets } => {
            let entries: Vec<&BucketEntry> = buckets.iter().collect();
            histogram_bucket_to_batch(&entries, agg_def, schema)
        }
        BucketResult::Range { buckets } => {
            let entries: Vec<&RangeBucketEntry> = buckets.iter().collect();
            range_bucket_to_batch(&entries, agg_def, schema)
        }
        _ => Err(DataFusionError::Internal(
            "unsupported bucket type for agg pushdown".into(),
        )),
    }
}

fn terms_bucket_to_batch(
    buckets: &[BucketEntry],
    agg_def: &Aggregation,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let num_rows = buckets.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let col_name = field.name().as_str();

        if is_group_key_column(col_name, agg_def) {
            // Bucket key column
            let values: Vec<Option<String>> = buckets
                .iter()
                .map(|b| Some(key_to_string(&b.key)))
                .collect();
            columns.push(cast_key_column(&values, field.data_type()));
        } else if col_name == "doc_count" {
            let values: Vec<i64> = buckets.iter().map(|b| b.doc_count as i64).collect();
            columns.push(Arc::new(Int64Array::from(values)));
        } else {
            // Sub-aggregation metric column
            let values: Vec<Option<f64>> = buckets
                .iter()
                .map(|b| extract_sub_agg_value(&b.sub_aggregation, col_name, &agg_def.sub_aggregation))
                .collect();
            columns.push(typed_f64_column(&values, field.data_type(), num_rows));
        }
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::Internal(format!("build terms batch: {e}")))
}

fn histogram_bucket_to_batch(
    buckets: &[&BucketEntry],
    agg_def: &Aggregation,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let num_rows = buckets.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let col_name = field.name().as_str();

        if col_name == "bucket" {
            let values: Vec<Option<f64>> = buckets
                .iter()
                .map(|b| key_to_f64(&b.key))
                .collect();
            columns.push(Arc::new(Float64Array::from(values)));
        } else if col_name == "doc_count" {
            let values: Vec<i64> = buckets.iter().map(|b| b.doc_count as i64).collect();
            columns.push(Arc::new(Int64Array::from(values)));
        } else {
            let values: Vec<Option<f64>> = buckets
                .iter()
                .map(|b| extract_sub_agg_value(&b.sub_aggregation, col_name, &agg_def.sub_aggregation))
                .collect();
            columns.push(typed_f64_column(&values, field.data_type(), num_rows));
        }
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::Internal(format!("build histogram batch: {e}")))
}

fn range_bucket_to_batch(
    buckets: &[&RangeBucketEntry],
    agg_def: &Aggregation,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let num_rows = buckets.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let col_name = field.name().as_str();

        if col_name == "bucket" {
            let values: Vec<Option<&str>> = buckets
                .iter()
                .map(|b| Some(key_as_str(&b.key)))
                .collect();
            columns.push(Arc::new(StringArray::from(values)));
        } else if col_name == "doc_count" {
            let values: Vec<i64> = buckets.iter().map(|b| b.doc_count as i64).collect();
            columns.push(Arc::new(Int64Array::from(values)));
        } else {
            let values: Vec<Option<f64>> = buckets
                .iter()
                .map(|b| extract_sub_agg_value(&b.sub_aggregation, col_name, &agg_def.sub_aggregation))
                .collect();
            columns.push(typed_f64_column(&values, field.data_type(), num_rows));
        }
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::Internal(format!("build range batch: {e}")))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check if a column name is the GROUP BY key for this aggregation.
fn is_group_key_column(col_name: &str, agg_def: &Aggregation) -> bool {
    match &agg_def.agg {
        AggregationVariants::Terms(t) => col_name == t.field,
        AggregationVariants::Histogram(_) => col_name == "bucket",
        AggregationVariants::DateHistogram(_) => col_name == "bucket",
        AggregationVariants::Range(_) => col_name == "bucket",
        _ => false,
    }
}

fn key_to_string(key: &Key) -> String {
    match key {
        Key::Str(s) => s.clone(),
        Key::F64(v) => v.to_string(),
        Key::I64(v) => v.to_string(),
        Key::U64(v) => v.to_string(),
    }
}

fn key_to_f64(key: &Key) -> Option<f64> {
    match key {
        Key::F64(v) => Some(*v),
        Key::I64(v) => Some(*v as f64),
        Key::U64(v) => Some(*v as f64),
        Key::Str(_) => None,
    }
}

fn key_as_str(key: &Key) -> &str {
    match key {
        Key::Str(s) => s.as_str(),
        _ => "",
    }
}

/// Cast string key values to the target data type.
fn cast_key_column(values: &[Option<String>], data_type: &DataType) -> ArrayRef {
    let string_arr: ArrayRef = Arc::new(StringArray::from(
        values.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
    ));
    match data_type {
        DataType::Utf8 => string_arr,
        DataType::Dictionary(_, _) => {
            // Produce a Dictionary<Int32, Utf8> array matching the schema
            arrow::compute::cast(&string_arr, data_type).unwrap_or(string_arr)
        }
        DataType::Float64 => {
            let nums: Vec<Option<f64>> = values
                .iter()
                .map(|v| v.as_ref().and_then(|s| s.parse::<f64>().ok()))
                .collect();
            Arc::new(Float64Array::from(nums))
        }
        DataType::Int64 => {
            let nums: Vec<Option<i64>> = values
                .iter()
                .map(|v| v.as_ref().and_then(|s| s.parse::<i64>().ok()))
                .collect();
            Arc::new(Int64Array::from(nums))
        }
        DataType::UInt64 => {
            let nums: Vec<Option<u64>> = values
                .iter()
                .map(|v| v.as_ref().and_then(|s| s.parse::<u64>().ok()))
                .collect();
            Arc::new(UInt64Array::from(nums))
        }
        // Fallback: produce string
        _ => string_arr,
    }
}

/// Create a typed column from f64 values, casting to the target data type.
fn typed_f64_column(values: &[Option<f64>], data_type: &DataType, _num_rows: usize) -> ArrayRef {
    match data_type {
        DataType::Float64 => Arc::new(Float64Array::from(values.to_vec())),
        DataType::Int64 => {
            let ints: Vec<Option<i64>> = values.iter().map(|v| v.map(|f| f as i64)).collect();
            Arc::new(Int64Array::from(ints))
        }
        DataType::UInt64 => {
            let uints: Vec<Option<u64>> = values.iter().map(|v| v.map(|f| f as u64)).collect();
            Arc::new(UInt64Array::from(uints))
        }
        // Fallback
        _ => Arc::new(Float64Array::from(values.to_vec())),
    }
}

/// Extract a metric value from sub-aggregation results for a given column name.
fn extract_sub_agg_value(
    sub_agg_results: &AggregationResults,
    col_name: &str,
    sub_agg_defs: &Aggregations,
) -> Option<f64> {
    // Try direct match: col_name is a sub-agg key
    if let Some(result) = sub_agg_results.0.get(col_name) {
        if let AggregationResult::MetricResult(metric) = result {
            return extract_simple_metric_value(metric);
        }
    }

    // Try prefix match for stats-like aggs: col_name = "{sub_agg_name}_{suffix}"
    for (sub_name, _sub_def) in sub_agg_defs.iter() {
        if let Some(suffix) = col_name.strip_prefix(&format!("{sub_name}_")) {
            if let Some(AggregationResult::MetricResult(metric)) = sub_agg_results.0.get(sub_name) {
                return extract_stats_metric_value(metric, suffix);
            }
        }
    }

    None
}

fn extract_simple_metric_value(metric: &MetricResult) -> Option<f64> {
    match metric {
        MetricResult::Average(m)
        | MetricResult::Sum(m)
        | MetricResult::Min(m)
        | MetricResult::Max(m)
        | MetricResult::Count(m)
        | MetricResult::Cardinality(m) => m.value,
        MetricResult::Stats(s) => s.avg, // fallback for direct access
        _ => None,
    }
}

fn extract_stats_metric_value(metric: &MetricResult, suffix: &str) -> Option<f64> {
    match metric {
        MetricResult::Stats(s) => match suffix {
            "min" => s.min,
            "max" => s.max,
            "sum" => Some(s.sum),
            "count" => Some(s.count as f64),
            "avg" => s.avg,
            _ => None,
        },
        MetricResult::ExtendedStats(es) => match suffix {
            "min" => es.min,
            "max" => es.max,
            "sum" => Some(es.sum),
            "count" => Some(es.count as f64),
            "avg" => es.avg,
            "variance_population" => es.variance_population,
            "std_deviation_population" => es.std_deviation_population,
            _ => None,
        },
        _ => None,
    }
}
