//! This will enhance the request tree with access to the fastfield and metadata.

use std::rc::Rc;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use fastfield_codecs::OptionalColumn;

use super::agg_req::{Aggregation, Aggregations, BucketAggregationType, MetricAggregation};
use super::bucket::{HistogramAggregation, RangeAggregation, TermsAggregation};
use super::metric::{AverageAggregation, StatsAggregation};
use super::segment_agg_result::BucketCount;
use super::VecWithNames;
use crate::fastfield::{type_and_cardinality, FastType, MultiValuedFastFieldReader};
use crate::schema::{Cardinality, Type};
use crate::{InvertedIndexReader, SegmentReader, TantivyError};

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
pub(crate) enum FastFieldAccessor {
    Multi(MultiValuedFastFieldReader<u64>),
    Single(Arc<dyn OptionalColumn<u64>>),
}
impl FastFieldAccessor {
    pub fn as_single(&self) -> Option<&dyn OptionalColumn<u64>> {
        match self {
            FastFieldAccessor::Multi(_) => None,
            FastFieldAccessor::Single(reader) => Some(&**reader),
        }
    }
    pub fn into_single(self) -> Option<Arc<dyn OptionalColumn<u64>>> {
        match self {
            FastFieldAccessor::Multi(_) => None,
            FastFieldAccessor::Single(reader) => Some(reader),
        }
    }
    pub fn as_multi(&self) -> Option<&MultiValuedFastFieldReader<u64>> {
        match self {
            FastFieldAccessor::Multi(reader) => Some(reader),
            FastFieldAccessor::Single(_) => None,
        }
    }
}

#[derive(Clone)]
pub struct BucketAggregationWithAccessor {
    /// In general there can be buckets without fast field access, e.g. buckets that are created
    /// based on search terms. So eventually this needs to be Option or moved.
    pub(crate) accessor: FastFieldAccessor,
    pub(crate) inverted_index: Option<Arc<InvertedIndexReader>>,
    pub(crate) field_type: Type,
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
        let mut inverted_index = None;
        let (accessor, field_type) = match &bucket {
            BucketAggregationType::Range(RangeAggregation {
                field: field_name, ..
            }) => get_ff_reader_and_validate(reader, field_name, Cardinality::SingleValue)?,
            BucketAggregationType::Histogram(HistogramAggregation {
                field: field_name, ..
            }) => get_ff_reader_and_validate(reader, field_name, Cardinality::SingleValue)?,
            BucketAggregationType::Terms(TermsAggregation {
                field: field_name, ..
            }) => {
                let field = reader
                    .schema()
                    .get_field(field_name)
                    .ok_or_else(|| TantivyError::FieldNotFound(field_name.to_string()))?;
                inverted_index = Some(reader.inverted_index(field)?);
                get_ff_reader_and_validate(reader, field_name, Cardinality::MultiValues)?
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
            inverted_index,
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
    pub field_type: Type,
    pub accessor: Arc<dyn OptionalColumn>,
}

impl MetricAggregationWithAccessor {
    fn try_from_metric(
        metric: &MetricAggregation,
        reader: &SegmentReader,
    ) -> crate::Result<MetricAggregationWithAccessor> {
        match &metric {
            MetricAggregation::Average(AverageAggregation { field: field_name })
            | MetricAggregation::Stats(StatsAggregation { field: field_name }) => {
                let (accessor, field_type) =
                    get_ff_reader_and_validate(reader, field_name, Cardinality::SingleValue)?;

                Ok(MetricAggregationWithAccessor {
                    accessor: accessor
                        .into_single()
                        .expect("unexpected fast field cardinality"),
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
    cardinality: Cardinality,
) -> crate::Result<(FastFieldAccessor, Type)> {
    let field = reader
        .schema()
        .get_field(field_name)
        .ok_or_else(|| TantivyError::FieldNotFound(field_name.to_string()))?;
    let field_type = reader.schema().get_field_entry(field).field_type();

    if let Some((ff_type, field_cardinality)) = type_and_cardinality(field_type) {
        if ff_type == FastType::Date {
            return Err(TantivyError::InvalidArgument(
                "Unsupported field type date in aggregation".to_string(),
            ));
        }

        if cardinality != field_cardinality {
            return Err(TantivyError::InvalidArgument(format!(
                "Invalid field cardinality on field {} expected {:?}, but got {:?}",
                field_name, cardinality, field_cardinality
            )));
        }
    } else {
        return Err(TantivyError::InvalidArgument(format!(
            "Only fast fields of type f64, u64, i64 are supported, but got {:?} ",
            field_type.value_type()
        )));
    };

    let ff_fields = reader.fast_fields();
    match cardinality {
        Cardinality::SingleValue => ff_fields
            .u64_lenient(field)
            .map(|field| (FastFieldAccessor::Single(field), field_type.value_type())),
        Cardinality::MultiValues => ff_fields
            .u64s_lenient(field)
            .map(|field| (FastFieldAccessor::Multi(field), field_type.value_type())),
    }
}
