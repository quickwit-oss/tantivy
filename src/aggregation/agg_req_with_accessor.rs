//! This will enhance the request tree with access to the fastfield and metadata.

use std::collections::HashMap;

use super::agg_req::{Aggregation, Aggregations, BucketAggregationType, MetricAggregation};
use super::bucket::RangeAggregation;
use super::VecWithNames;
use crate::fastfield::DynamicFastFieldReader;
use crate::schema::Type;
use crate::{SegmentReader, TantivyError};

#[derive(Clone)]
pub(crate) struct AggregationsWithAccessor(pub(crate) VecWithNames<AggregationWithAccessor>);

impl AggregationsWithAccessor {
    fn from_map(entries: HashMap<String, AggregationWithAccessor>) -> Self {
        AggregationsWithAccessor(VecWithNames::from_entries(entries.into_iter().collect()))
    }
}

/// Aggregation tree with fast field accessors.
#[derive(Clone)]
pub enum AggregationWithAccessor {
    Bucket(BucketAggregationWithAccessor),
    Metric(MetricAggregationWithAccessor),
}

impl AggregationWithAccessor {
    pub fn as_bucket(&self) -> Option<&BucketAggregationWithAccessor> {
        match self {
            AggregationWithAccessor::Bucket(bucket) => Some(bucket),
            AggregationWithAccessor::Metric(_) => None,
        }
    }
    pub fn as_metric(&self) -> Option<&MetricAggregationWithAccessor> {
        match self {
            AggregationWithAccessor::Bucket(_) => None,
            AggregationWithAccessor::Metric(metric) => Some(metric),
        }
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
    fn from_bucket(
        bucket: &BucketAggregationType,
        sub_aggregation: &Aggregations,
        reader: &SegmentReader,
    ) -> crate::Result<BucketAggregationWithAccessor> {
        let (accessor, field_type) = match &bucket {
            BucketAggregationType::RangeAggregation(RangeAggregation {
                field_name,
                buckets: _,
            }) => get_ff_reader(reader, field_name)?,
        };
        let sub_aggregation = sub_aggregation.clone();
        Ok(BucketAggregationWithAccessor {
            accessor,
            field_type,
            sub_aggregation: get_aggregations_with_accessor(&sub_aggregation, reader)?,
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
    fn from_metric(
        metric: &MetricAggregation,
        reader: &SegmentReader,
    ) -> crate::Result<MetricAggregationWithAccessor> {
        match &metric {
            MetricAggregation::Average { field_name } => {
                let (accessor, field_type) = get_ff_reader(reader, field_name)?;

                Ok(MetricAggregationWithAccessor {
                    accessor,
                    field_type,
                    metric: metric.clone(),
                })
            }
        }
    }
}

pub(crate) fn get_aggregations_with_accessor(
    aggs: &Aggregations,
    reader: &SegmentReader,
) -> crate::Result<AggregationsWithAccessor> {
    Ok(AggregationsWithAccessor::from_map(
        aggs.iter()
            .map(|(key, agg)| {
                get_aggregation_with_accessor(agg, reader).map(|el| (key.to_string(), el))
            })
            .collect::<crate::Result<HashMap<_, _>>>()?
            .into(),
    ))
}

fn get_aggregation_with_accessor(
    agg: &Aggregation,
    reader: &SegmentReader,
) -> crate::Result<AggregationWithAccessor> {
    match agg {
        Aggregation::Bucket(b) => {
            BucketAggregationWithAccessor::from_bucket(&b.bucket_agg, &b.sub_aggregation, reader)
                .map(AggregationWithAccessor::Bucket)
        }
        Aggregation::Metric(metric) => MetricAggregationWithAccessor::from_metric(metric, reader)
            .map(AggregationWithAccessor::Metric),
    }
}

// TODO validate field type
fn get_ff_reader(
    reader: &SegmentReader,
    field_name: &str,
) -> crate::Result<(DynamicFastFieldReader<u64>, Type)> {
    let field = reader
        .schema()
        .get_field(field_name)
        .ok_or_else(|| TantivyError::FieldNotFound(field_name.to_string()))?;
    let field_type = reader.schema().get_field_entry(field).field_type();
    let ff_fields = reader.fast_fields();
    ff_fields
        .u64_lenient(field)
        .map(|field| (field, field_type.value_type()))
}
