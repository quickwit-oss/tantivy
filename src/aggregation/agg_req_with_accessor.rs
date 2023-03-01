//! This will enhance the request tree with access to the fastfield and metadata.

use std::rc::Rc;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use columnar::{Column, ColumnType, ColumnValues, StrColumn};

use super::agg_req::{Aggregation, Aggregations, BucketAggregationType, MetricAggregation};
use super::bucket::{
    DateHistogramAggregationReq, HistogramAggregation, RangeAggregation, TermsAggregation,
};
use super::metric::{
    AverageAggregation, CountAggregation, MaxAggregation, MinAggregation, StatsAggregation,
    SumAggregation,
};
use super::segment_agg_result::BucketCount;
use super::VecWithNames;
use crate::SegmentReader;

#[derive(Clone, Default)]
pub(crate) struct AggregationsWithAccessor {
    pub metrics: VecWithNames<MetricAggregationWithAccessor>,
    pub buckets: VecWithNames<BucketAggregationWithAccessor>,
}

impl AggregationsWithAccessor {
    fn from_data(
        metrics: VecWithNames<MetricAggregationWithAccessor>,
        buckets: VecWithNames<BucketAggregationWithAccessor>,
    ) -> Self {
        Self { metrics, buckets }
    }

    pub fn is_empty(&self) -> bool {
        self.metrics.is_empty() && self.buckets.is_empty()
    }
}

#[derive(Clone)]
pub struct BucketAggregationWithAccessor {
    /// In general there can be buckets without fast field access, e.g. buckets that are created
    /// based on search terms. So eventually this needs to be Option or moved.
    pub(crate) accessor: Column<u64>,
    pub(crate) str_dict_column: Option<StrColumn>,
    pub(crate) field_type: ColumnType,
    pub(crate) bucket_agg: BucketAggregationType,
    pub(crate) sub_aggregation: AggregationsWithAccessor,
    pub(crate) bucket_count: BucketCount,
}

impl BucketAggregationWithAccessor {
    fn try_from_bucket(
        bucket: &BucketAggregationType,
        sub_aggregation: &Aggregations,
        reader: &SegmentReader,
        bucket_count: Rc<AtomicU32>,
        max_bucket_count: u32,
    ) -> crate::Result<BucketAggregationWithAccessor> {
        let mut str_dict_column = None;
        let (accessor, field_type) = match &bucket {
            BucketAggregationType::Range(RangeAggregation {
                field: field_name, ..
            }) => get_ff_reader_and_validate(reader, field_name)?,
            BucketAggregationType::Histogram(HistogramAggregation {
                field: field_name, ..
            }) => get_ff_reader_and_validate(reader, field_name)?,
            BucketAggregationType::DateHistogram(DateHistogramAggregationReq {
                field: field_name,
                ..
            }) => get_ff_reader_and_validate(reader, field_name)?,
            BucketAggregationType::Terms(TermsAggregation {
                field: field_name, ..
            }) => {
                str_dict_column = reader.fast_fields().str(field_name)?;
                get_ff_reader_and_validate(reader, field_name)?
            }
        };
        let sub_aggregation = sub_aggregation.clone();
        Ok(BucketAggregationWithAccessor {
            accessor,
            field_type,
            sub_aggregation: get_aggs_with_accessor_and_validate(
                &sub_aggregation,
                reader,
                bucket_count.clone(),
                max_bucket_count,
            )?,
            bucket_agg: bucket.clone(),
            str_dict_column,
            bucket_count: BucketCount {
                bucket_count,
                max_bucket_count,
            },
        })
    }
}

/// Contains the metric request and the fast field accessor.
#[derive(Clone)]
pub struct MetricAggregationWithAccessor {
    pub metric: MetricAggregation,
    pub field_type: ColumnType,
    pub accessor: Column<u64>,
}

impl MetricAggregationWithAccessor {
    fn try_from_metric(
        metric: &MetricAggregation,
        reader: &SegmentReader,
    ) -> crate::Result<MetricAggregationWithAccessor> {
        match &metric {
            MetricAggregation::Average(AverageAggregation { field: field_name })
            | MetricAggregation::Count(CountAggregation { field: field_name })
            | MetricAggregation::Max(MaxAggregation { field: field_name })
            | MetricAggregation::Min(MinAggregation { field: field_name })
            | MetricAggregation::Stats(StatsAggregation { field: field_name })
            | MetricAggregation::Sum(SumAggregation { field: field_name }) => {
                let (accessor, field_type) = get_ff_reader_and_validate(reader, field_name)?;

                Ok(MetricAggregationWithAccessor {
                    accessor,
                    field_type,
                    metric: metric.clone(),
                })
            }
        }
    }
}

pub(crate) fn get_aggs_with_accessor_and_validate(
    aggs: &Aggregations,
    reader: &SegmentReader,
    bucket_count: Rc<AtomicU32>,
    max_bucket_count: u32,
) -> crate::Result<AggregationsWithAccessor> {
    let mut metrics = vec![];
    let mut buckets = vec![];
    for (key, agg) in aggs.iter() {
        match agg {
            Aggregation::Bucket(bucket) => buckets.push((
                key.to_string(),
                BucketAggregationWithAccessor::try_from_bucket(
                    &bucket.bucket_agg,
                    &bucket.sub_aggregation,
                    reader,
                    Rc::clone(&bucket_count),
                    max_bucket_count,
                )?,
            )),
            Aggregation::Metric(metric) => metrics.push((
                key.to_string(),
                MetricAggregationWithAccessor::try_from_metric(metric, reader)?,
            )),
        }
    }
    Ok(AggregationsWithAccessor::from_data(
        VecWithNames::from_entries(metrics),
        VecWithNames::from_entries(buckets),
    ))
}

/// Get fast field reader with given cardinatility.
fn get_ff_reader_and_validate(
    reader: &SegmentReader,
    field_name: &str,
) -> crate::Result<(columnar::Column<u64>, ColumnType)> {
    let ff_fields = reader.fast_fields();
    let ff_field_with_type = ff_fields
        .u64_lenient_with_type(field_name)?
        .unwrap_or_else(|| (build_empty_column(reader.num_docs()), ColumnType::U64));

    Ok(ff_field_with_type)
}

// Empty Column
fn build_empty_column(num_docs: u32) -> Column {
    struct EmptyValues;
    impl ColumnValues for EmptyValues {
        fn get_val(&self, _idx: u32) -> u64 {
            unimplemented!("Internal Error: Called get_val of empty column.")
        }
        fn min_value(&self) -> u64 {
            unimplemented!("Internal Error: Called min_value of empty column.")
        }
        fn max_value(&self) -> u64 {
            unimplemented!("Internal Error: Called max_value of empty column.")
        }
        fn num_vals(&self) -> u32 {
            0
        }
    }

    Column {
        idx: columnar::ColumnIndex::Empty { num_docs },
        values: Arc::new(EmptyValues),
    }
}
