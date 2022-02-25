//! This will enhance the request tree with access to the fastfield and metadata.

use super::agg_req::{Aggregation, Aggregations, BucketAggregationType, MetricAggregation};
use super::bucket::RangeAggregation;
use super::metric::{AverageAggregation, StatsAggregation};
use super::VecWithNames;
use crate::fastfield::{type_and_cardinality, DynamicFastFieldReader, FastType};
use crate::schema::{Cardinality, Type};
use crate::{SegmentReader, TantivyError};

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
    pub(crate) accessor: DynamicFastFieldReader<u64>,
    pub(crate) field_type: Type,
    pub(crate) bucket_agg: BucketAggregationType,
    pub(crate) sub_aggregation: AggregationsWithAccessor,
}

impl BucketAggregationWithAccessor {
    fn try_from_bucket(
        bucket: &BucketAggregationType,
        sub_aggregation: &Aggregations,
        reader: &SegmentReader,
    ) -> crate::Result<BucketAggregationWithAccessor> {
        let (accessor, field_type) = match &bucket {
            BucketAggregationType::Range(RangeAggregation {
                field: field_name,
                ranges: _,
            }) => get_ff_reader_and_validate(reader, field_name)?,
        };
        let sub_aggregation = sub_aggregation.clone();
        Ok(BucketAggregationWithAccessor {
            accessor,
            field_type,
            sub_aggregation: get_aggs_with_accessor_and_validate(&sub_aggregation, reader)?,
            bucket_agg: bucket.clone(),
        })
    }
}

/// Contains the metric request and the fast field accessor.
#[derive(Clone)]
pub struct MetricAggregationWithAccessor {
    pub metric: MetricAggregation,
    pub field_type: Type,
    pub accessor: DynamicFastFieldReader<u64>,
}

impl MetricAggregationWithAccessor {
    fn try_from_metric(
        metric: &MetricAggregation,
        reader: &SegmentReader,
    ) -> crate::Result<MetricAggregationWithAccessor> {
        match &metric {
            MetricAggregation::Average(AverageAggregation { field: field_name })
            | MetricAggregation::Stats(StatsAggregation { field: field_name }) => {
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

fn get_ff_reader_and_validate(
    reader: &SegmentReader,
    field_name: &str,
) -> crate::Result<(DynamicFastFieldReader<u64>, Type)> {
    let field = reader
        .schema()
        .get_field(field_name)
        .ok_or_else(|| TantivyError::FieldNotFound(field_name.to_string()))?;
    let field_type = reader.schema().get_field_entry(field).field_type();

    if let Some((ff_type, cardinality)) = type_and_cardinality(field_type) {
        if cardinality == Cardinality::MultiValues || ff_type == FastType::Date {
            return Err(TantivyError::InvalidArgument(format!(
                "Invalid field type in aggregation {:?}, only Cardinality::SingleValue supported",
                field_type.value_type()
            )));
        }
    } else {
        return Err(TantivyError::InvalidArgument(format!(
            "Only single value fast fields of type f64, u64, i64 are supported, but got {:?} ",
            field_type.value_type()
        )));
    };

    let ff_fields = reader.fast_fields();
    ff_fields
        .u64_lenient(field)
        .map(|field| (field, field_type.value_type()))
}
