use std::cmp::Ordering;
use std::fmt::Display;

use columnar::ColumnType;
use itertools::Itertools;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tantivy_bitpacker::minmax;

use crate::aggregation::agg_limits::MemoryConsumption;
use crate::aggregation::agg_req::Aggregations;
use crate::aggregation::agg_req_with_accessor::{
    AggregationWithAccessor, AggregationsWithAccessor,
};
use crate::aggregation::agg_result::BucketEntry;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
    IntermediateHistogramBucketEntry,
};
use crate::aggregation::segment_agg_result::{
    build_segment_agg_collector, AggregationLimits, SegmentAggregationCollector,
};
use crate::aggregation::{f64_from_fastfield_u64, format_date};
use crate::TantivyError;

/// Histogram is a bucket aggregation, where buckets are created dynamically for given `interval`.
/// Each document value is rounded down to its bucket.
///
/// E.g. if we have a price 18 and an interval of 5, the document will fall into the bucket with
/// the key 15. The formula used for this is:
/// `((val - offset) / interval).floor() * interval + offset`
///
/// For this calculation all fastfield values are converted to f64.
///
/// # Returned Buckets
/// By default buckets are returned between the min and max value of the documents, including empty
/// buckets.
/// Setting min_doc_count to != 0 will filter empty buckets.
///
/// The value range of the buckets can bet extended via
/// [extended_bounds](HistogramAggregation::extended_bounds) or limit the range via
/// [hard_bounds](HistogramAggregation::hard_bounds).
///
/// # Result
/// Result type is [`BucketResult`](crate::aggregation::agg_result::BucketResult) with
/// [`BucketEntry`](crate::aggregation::agg_result::BucketEntry) on the
/// `AggregationCollector`.
///
/// Result type is
/// [`IntermediateBucketResult`](crate::aggregation::intermediate_agg_result::IntermediateBucketResult) with
/// [`IntermediateHistogramBucketEntry`](crate::aggregation::intermediate_agg_result::IntermediateHistogramBucketEntry) on the
/// `DistributedAggregationCollector`.
///
/// # Limitations/Compatibility
///
/// # JSON Format
/// ```json
/// {
///     "prices": {
///         "histogram": {
///             "field": "price",
///             "interval": 10
///         }
///     }
/// }
/// ```
///
/// Response
/// See [`BucketEntry`](crate::aggregation::agg_result::BucketEntry)
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HistogramAggregation {
    /// The field to aggregate on.
    pub field: String,
    /// The interval to chunk your data range. Each bucket spans a value range of [0..interval).
    /// Must be a positive value.
    pub interval: f64,
    /// Intervals implicitly defines an absolute grid of buckets `[interval * k, interval * (k +
    /// 1))`.
    ///
    /// Offset makes it possible to shift this grid into
    /// `[offset + interval * k, offset + interval * (k + 1))`. Offset has to be in the range [0,
    /// interval).
    ///
    /// As an example, if there are two documents with value 9 and 12 and interval 10.0, they would
    /// fall into the buckets with the key 0 and 10.
    /// With offset 5 and interval 10, they would both fall into the bucket with they key 5 and the
    /// range [5..15)
    pub offset: Option<f64>,
    /// The minimum number of documents in a bucket to be returned. Defaults to 0.
    pub min_doc_count: Option<u64>,
    /// Limits the data range to `[min, max]` closed interval.
    ///
    /// This can be used to filter values if they are not in the data range.
    ///
    /// hard_bounds only limits the buckets, to force a range set both extended_bounds and
    /// hard_bounds to the same range.
    ///
    /// ## Example
    /// ```json
    /// {
    ///     "prices": {
    ///        "histogram": {
    ///            "field": "price",
    ///            "interval": 10,
    ///            "hard_bounds": {
    ///                "min": 0,
    ///                "max": 100
    ///            }
    ///        }
    ///    }
    /// }
    /// ```
    pub hard_bounds: Option<HistogramBounds>,
    /// Can be set to extend your bounds. The range of the buckets is by default defined by the
    /// data range of the values of the documents. As the name suggests, this can only be used to
    /// extend the value range. If the bounds for min or max are not extending the range, the value
    /// has no effect on the returned buckets.
    ///
    /// Cannot be set in conjunction with min_doc_count > 0, since the empty buckets from extended
    /// bounds would not be returned.
    pub extended_bounds: Option<HistogramBounds>,
    /// Whether to return the buckets as a hash map
    #[serde(default)]
    pub keyed: bool,
}

impl HistogramAggregation {
    pub(crate) fn normalize(&mut self, column_type: ColumnType) {
        if column_type.is_date_time() {
            // values are provided in ms, but the fastfield is in nano seconds
            self.interval *= 1_000_000.0;
            self.offset = self.offset.map(|off| off * 1_000_000.0);
            self.hard_bounds = self.hard_bounds.map(|bounds| HistogramBounds {
                min: bounds.min * 1_000_000.0,
                max: bounds.max * 1_000_000.0,
            });
            self.extended_bounds = self.extended_bounds.map(|bounds| HistogramBounds {
                min: bounds.min * 1_000_000.0,
                max: bounds.max * 1_000_000.0,
            });
        }
    }

    fn validate(&self) -> crate::Result<()> {
        if self.interval <= 0.0f64 {
            return Err(TantivyError::InvalidArgument(
                "interval must be a positive value".to_string(),
            ));
        }

        if self.min_doc_count.unwrap_or(0) > 0 && self.extended_bounds.is_some() {
            return Err(TantivyError::InvalidArgument(
                "Cannot set min_doc_count and extended_bounds at the same time".to_string(),
            ));
        }

        if let (Some(hard_bounds), Some(extended_bounds)) = (self.hard_bounds, self.extended_bounds)
        {
            if extended_bounds.min < hard_bounds.min || extended_bounds.max > hard_bounds.max {
                return Err(TantivyError::InvalidArgument(format!(
                    "extended_bounds have to be inside hard_bounds, extended_bounds: \
                     {extended_bounds}, hard_bounds {hard_bounds}"
                )));
            }
        }

        Ok(())
    }

    /// Returns the minimum number of documents required for a bucket to be returned.
    pub fn min_doc_count(&self) -> u64 {
        self.min_doc_count.unwrap_or(0)
    }
}

/// Used to set extended or hard bounds on the histogram.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct HistogramBounds {
    /// The lower bounds.
    #[serde(deserialize_with = "deserialize_date_or_num")]
    pub min: f64,
    /// The upper bounds.
    #[serde(deserialize_with = "deserialize_date_or_num")]
    pub max: f64,
}

fn deserialize_date_or_num<'de, D>(deserializer: D) -> Result<f64, D::Error>
where D: serde::Deserializer<'de> {
    let value: serde_json::Value = Deserialize::deserialize(deserializer)?;

    // Check if the value is a string representing an Rfc3339 formatted date
    if let serde_json::Value::String(date_str) = value {
        // Parse the Rfc3339 formatted date string into a DateTime<Utc>
        let date =
            time::OffsetDateTime::parse(&date_str, &time::format_description::well_known::Rfc3339)
                .map_err(|_| serde::de::Error::custom("Invalid Rfc3339 formatted date"))?;

        let milliseconds: i64 = (date.unix_timestamp_nanos() / 1_000_000)
            .try_into()
            .map_err(|_| serde::de::Error::custom("{date_str} out of allowed range"))?;

        // Return the milliseconds as f64
        Ok(milliseconds as f64)
    } else {
        // The value is not a string, so assume it's a regular f64 number
        value
            .as_f64()
            .ok_or_else(|| serde::de::Error::custom("Invalid number format"))
    }
}

impl Display for HistogramBounds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("[{},{}]", self.min, self.max))
    }
}

impl HistogramBounds {
    fn contains(&self, val: f64) -> bool {
        val >= self.min && val <= self.max
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub(crate) struct SegmentHistogramBucketEntry {
    pub key: f64,
    pub doc_count: u64,
}

impl SegmentHistogramBucketEntry {
    pub(crate) fn into_intermediate_bucket_entry(
        self,
        sub_aggregation: Option<Box<dyn SegmentAggregationCollector>>,
        agg_with_accessor: &AggregationsWithAccessor,
    ) -> crate::Result<IntermediateHistogramBucketEntry> {
        let mut sub_aggregation_res = IntermediateAggregationResults::default();
        if let Some(sub_aggregation) = sub_aggregation {
            sub_aggregation
                .add_intermediate_aggregation_result(agg_with_accessor, &mut sub_aggregation_res)?;
        }
        Ok(IntermediateHistogramBucketEntry {
            key: self.key,
            doc_count: self.doc_count,
            sub_aggregation: sub_aggregation_res,
        })
    }
}

/// The collector puts values from the fast field into the correct buckets and does a conversion to
/// the correct datatype.
#[derive(Clone, Debug)]
pub struct SegmentHistogramCollector {
    /// The buckets containing the aggregation data.
    buckets: FxHashMap<i64, SegmentHistogramBucketEntry>,
    sub_aggregations: FxHashMap<i64, Box<dyn SegmentAggregationCollector>>,
    sub_aggregation_blueprint: Option<Box<dyn SegmentAggregationCollector>>,
    column_type: ColumnType,
    interval: f64,
    offset: f64,
    bounds: HistogramBounds,
    accessor_idx: usize,
}

impl SegmentAggregationCollector for SegmentHistogramCollector {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let name = agg_with_accessor.aggs.keys[self.accessor_idx].to_string();
        let agg_with_accessor = &agg_with_accessor.aggs.values[self.accessor_idx];

        let bucket = self.into_intermediate_bucket_result(agg_with_accessor)?;
        results.push(name, IntermediateAggregationResult::Bucket(bucket))?;

        Ok(())
    }

    #[inline]
    fn collect(
        &mut self,
        doc: crate::DocId,
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        self.collect_block(&[doc], agg_with_accessor)
    }

    #[inline]
    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &mut AggregationsWithAccessor,
    ) -> crate::Result<()> {
        let bucket_agg_accessor = &mut agg_with_accessor.aggs.values[self.accessor_idx];

        let mem_pre = self.get_memory_consumption();

        let bounds = self.bounds;
        let interval = self.interval;
        let offset = self.offset;
        let get_bucket_pos = |val| (get_bucket_pos_f64(val, interval, offset) as i64);

        bucket_agg_accessor
            .column_block_accessor
            .fetch_block(docs, &bucket_agg_accessor.accessor);

        for (doc, val) in bucket_agg_accessor.column_block_accessor.iter_docid_vals() {
            let val = self.f64_from_fastfield_u64(val);

            let bucket_pos = get_bucket_pos(val);

            if bounds.contains(val) {
                let bucket = self.buckets.entry(bucket_pos).or_insert_with(|| {
                    let key = get_bucket_key_from_pos(bucket_pos as f64, interval, offset);
                    SegmentHistogramBucketEntry { key, doc_count: 0 }
                });
                bucket.doc_count += 1;
                if let Some(sub_aggregation_blueprint) = self.sub_aggregation_blueprint.as_mut() {
                    self.sub_aggregations
                        .entry(bucket_pos)
                        .or_insert_with(|| sub_aggregation_blueprint.clone())
                        .collect(doc, &mut bucket_agg_accessor.sub_aggregation)?;
                }
            }
        }

        let mem_delta = self.get_memory_consumption() - mem_pre;
        bucket_agg_accessor
            .limits
            .add_memory_consumed(mem_delta as u64)?;

        Ok(())
    }

    fn flush(&mut self, agg_with_accessor: &mut AggregationsWithAccessor) -> crate::Result<()> {
        let sub_aggregation_accessor =
            &mut agg_with_accessor.aggs.values[self.accessor_idx].sub_aggregation;

        for sub_aggregation in self.sub_aggregations.values_mut() {
            sub_aggregation.flush(sub_aggregation_accessor)?;
        }

        Ok(())
    }
}

impl SegmentHistogramCollector {
    fn get_memory_consumption(&self) -> usize {
        let self_mem = std::mem::size_of::<Self>();
        let sub_aggs_mem = self.sub_aggregations.memory_consumption();
        let buckets_mem = self.buckets.memory_consumption();
        self_mem + sub_aggs_mem + buckets_mem
    }
    /// Converts the collector result into a intermediate bucket result.
    pub fn into_intermediate_bucket_result(
        self,
        agg_with_accessor: &AggregationWithAccessor,
    ) -> crate::Result<IntermediateBucketResult> {
        let mut buckets = Vec::with_capacity(self.buckets.len());

        for (bucket_pos, bucket) in self.buckets {
            let bucket_res = bucket.into_intermediate_bucket_entry(
                self.sub_aggregations.get(&bucket_pos).cloned(),
                &agg_with_accessor.sub_aggregation,
            );

            buckets.push(bucket_res?);
        }
        buckets.sort_unstable_by(|b1, b2| b1.key.total_cmp(&b2.key));

        Ok(IntermediateBucketResult::Histogram {
            buckets,
            column_type: Some(self.column_type),
        })
    }

    pub(crate) fn from_req_and_validate(
        mut req: HistogramAggregation,
        sub_aggregation: &mut AggregationsWithAccessor,
        field_type: ColumnType,
        accessor_idx: usize,
    ) -> crate::Result<Self> {
        req.validate()?;
        req.normalize(field_type);

        let sub_aggregation_blueprint = if sub_aggregation.is_empty() {
            None
        } else {
            let sub_aggregation = build_segment_agg_collector(sub_aggregation)?;
            Some(sub_aggregation)
        };

        let bounds = req.hard_bounds.unwrap_or(HistogramBounds {
            min: f64::MIN,
            max: f64::MAX,
        });

        Ok(Self {
            buckets: Default::default(),
            column_type: field_type,
            interval: req.interval,
            offset: req.offset.unwrap_or(0.0),
            bounds,
            sub_aggregations: Default::default(),
            sub_aggregation_blueprint,
            accessor_idx,
        })
    }

    #[inline]
    fn f64_from_fastfield_u64(&self, val: u64) -> f64 {
        f64_from_fastfield_u64(val, &self.column_type)
    }
}

#[inline]
fn get_bucket_pos_f64(val: f64, interval: f64, offset: f64) -> f64 {
    ((val - offset) / interval).floor()
}

#[inline]
fn get_bucket_key_from_pos(bucket_pos: f64, interval: f64, offset: f64) -> f64 {
    bucket_pos * interval + offset
}

// Convert to BucketEntry and fill gaps
fn intermediate_buckets_to_final_buckets_fill_gaps(
    buckets: Vec<IntermediateHistogramBucketEntry>,
    histogram_req: &HistogramAggregation,
    sub_aggregation: &Aggregations,
    limits: &AggregationLimits,
) -> crate::Result<Vec<BucketEntry>> {
    // Generate the full list of buckets without gaps.
    //
    // The bounds are the min max from the current buckets, optionally extended by
    // extended_bounds from the request
    let min_max = minmax(buckets.iter().map(|bucket| bucket.key));

    // memory check upfront
    let (_, first_bucket_num, last_bucket_num) =
        generate_bucket_pos_with_opt_minmax(histogram_req, min_max);
    // It's based on user input, so we need to account for overflows
    let added_buckets = ((last_bucket_num.saturating_sub(first_bucket_num)).max(0) as u64)
        .saturating_sub(buckets.len() as u64);
    limits.add_memory_consumed(
        added_buckets * std::mem::size_of::<IntermediateHistogramBucketEntry>() as u64,
    )?;
    // create buckets
    let fill_gaps_buckets = generate_buckets_with_opt_minmax(histogram_req, min_max);

    let empty_sub_aggregation = IntermediateAggregationResults::empty_from_req(sub_aggregation);

    // Use merge_join_by to fill in gaps, since buckets are sorted

    let final_buckets: Vec<BucketEntry> = buckets
        .into_iter()
        .merge_join_by(fill_gaps_buckets, |existing_bucket, fill_gaps_bucket| {
            existing_bucket
                .key
                .partial_cmp(fill_gaps_bucket)
                .unwrap_or(Ordering::Equal)
        })
        .map(|either| match either {
            // Ignore the generated bucket
            itertools::EitherOrBoth::Both(existing, _) => existing,
            itertools::EitherOrBoth::Left(existing) => existing,
            // Add missing bucket
            itertools::EitherOrBoth::Right(missing_bucket) => IntermediateHistogramBucketEntry {
                key: missing_bucket,
                doc_count: 0,
                sub_aggregation: empty_sub_aggregation.clone(),
            },
        })
        .map(|intermediate_bucket| {
            intermediate_bucket.into_final_bucket_entry(sub_aggregation, limits)
        })
        .collect::<crate::Result<Vec<_>>>()?;

    Ok(final_buckets)
}

// Convert to BucketEntry
pub(crate) fn intermediate_histogram_buckets_to_final_buckets(
    buckets: Vec<IntermediateHistogramBucketEntry>,
    column_type: Option<ColumnType>,
    histogram_req: &HistogramAggregation,
    sub_aggregation: &Aggregations,
    limits: &AggregationLimits,
) -> crate::Result<Vec<BucketEntry>> {
    // Normalization is column type dependent.
    // The request used in the the call to final is not yet be normalized.
    // Normalization is changing the precision from milliseconds to nanoseconds.
    let mut histogram_req = histogram_req.clone();
    if let Some(column_type) = column_type {
        histogram_req.normalize(column_type);
    }
    let mut buckets = if histogram_req.min_doc_count() == 0 {
        // With min_doc_count != 0, we may need to add buckets, so that there are no
        // gaps, since intermediate result does not contain empty buckets (filtered to
        // reduce serialization size).
        intermediate_buckets_to_final_buckets_fill_gaps(
            buckets,
            &histogram_req,
            sub_aggregation,
            limits,
        )?
    } else {
        buckets
            .into_iter()
            .filter(|histogram_bucket| histogram_bucket.doc_count >= histogram_req.min_doc_count())
            .map(|histogram_bucket| {
                histogram_bucket.into_final_bucket_entry(sub_aggregation, limits)
            })
            .collect::<crate::Result<Vec<_>>>()?
    };

    // If we have a date type on the histogram buckets, we add the `key_as_string` field as rfc339
    // and normalize from nanoseconds to milliseconds
    if column_type == Some(ColumnType::DateTime) {
        for bucket in buckets.iter_mut() {
            if let crate::aggregation::Key::F64(ref mut val) = bucket.key {
                let key_as_string = format_date(*val as i64)?;
                *val /= 1_000_000.0;
                bucket.key_as_string = Some(key_as_string);
            }
        }
    }

    Ok(buckets)
}

/// Applies req extended_bounds/hard_bounds on the min_max value
///
/// May return `(f64::MAX, f64::MIN)`, if there is no range.
fn get_req_min_max(req: &HistogramAggregation, min_max: Option<(f64, f64)>) -> (f64, f64) {
    let (mut min, mut max) = min_max.unwrap_or((f64::MAX, f64::MIN));

    if let Some(extended_bounds) = &req.extended_bounds {
        min = min.min(extended_bounds.min);
        max = max.max(extended_bounds.max);
    }

    if let Some(hard_bounds) = &req.hard_bounds {
        min = min.max(hard_bounds.min);
        max = max.min(hard_bounds.max);
    }

    (min, max)
}

/// Generates buckets with req.interval
/// Range is computed for provided min_max and request extended_bounds/hard_bounds
/// returns empty vec when there is no range to span
pub(crate) fn generate_bucket_pos_with_opt_minmax(
    req: &HistogramAggregation,
    min_max: Option<(f64, f64)>,
) -> (f64, i64, i64) {
    let (min, max) = get_req_min_max(req, min_max);

    let offset = req.offset.unwrap_or(0.0);
    let first_bucket_num = get_bucket_pos_f64(min, req.interval, offset) as i64;
    let last_bucket_num = get_bucket_pos_f64(max, req.interval, offset) as i64;
    (offset, first_bucket_num, last_bucket_num)
}

/// Generates buckets with req.interval
/// Range is computed for provided min_max and request extended_bounds/hard_bounds
/// returns empty vec when there is no range to span
pub(crate) fn generate_buckets_with_opt_minmax(
    req: &HistogramAggregation,
    min_max: Option<(f64, f64)>,
) -> Vec<f64> {
    let (offset, first_bucket_num, last_bucket_num) =
        generate_bucket_pos_with_opt_minmax(req, min_max);
    let mut buckets = Vec::with_capacity((first_bucket_num..=last_bucket_num).count());
    for bucket_pos in first_bucket_num..=last_bucket_num {
        let bucket_key = bucket_pos as f64 * req.interval + offset;
        buckets.push(bucket_key);
    }

    buckets
}

#[cfg(test)]
mod tests {

    use pretty_assertions::assert_eq;
    use serde_json::Value;

    use super::*;
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::{
        exec_request, exec_request_with_query, exec_request_with_query_and_memory_limit,
        get_test_index_2_segments, get_test_index_from_values, get_test_index_with_num_docs,
    };

    #[test]
    fn histogram_test_crooked_values() -> crate::Result<()> {
        let values = vec![-12.0, 12.31, 14.33, 16.23];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_interval": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 3.5,
                    "offset": 0.0,
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["my_interval"]["buckets"][0]["key"], -14.0);
        assert_eq!(res["my_interval"]["buckets"][0]["doc_count"], 1);
        assert_eq!(res["my_interval"]["buckets"][7]["key"], 10.5);
        assert_eq!(res["my_interval"]["buckets"][7]["doc_count"], 1);
        assert_eq!(res["my_interval"]["buckets"][8]["key"], 14.0);
        assert_eq!(res["my_interval"]["buckets"][8]["doc_count"], 2);
        assert_eq!(res["my_interval"]["buckets"][9], Value::Null);

        // With offset
        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_interval": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 3.5,
                    "offset": 1.2,
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["my_interval"]["buckets"][0]["key"], -12.8);
        assert_eq!(res["my_interval"]["buckets"][0]["doc_count"], 1);
        assert_eq!(res["my_interval"]["buckets"][1]["key"], -9.3);
        assert_eq!(res["my_interval"]["buckets"][1]["doc_count"], 0);
        assert_eq!(res["my_interval"]["buckets"][2]["key"], -5.8);
        assert_eq!(res["my_interval"]["buckets"][2]["doc_count"], 0);
        assert_eq!(res["my_interval"]["buckets"][3]["key"], -2.3);
        assert_eq!(res["my_interval"]["buckets"][3]["doc_count"], 0);

        assert_eq!(res["my_interval"]["buckets"][7]["key"], 11.7);
        assert_eq!(res["my_interval"]["buckets"][7]["doc_count"], 2);
        assert_eq!(res["my_interval"]["buckets"][8]["key"], 15.2);
        assert_eq!(res["my_interval"]["buckets"][8]["doc_count"], 1);
        assert_eq!(res["my_interval"]["buckets"][9], Value::Null);

        Ok(())
    }

    #[test]
    fn histogram_test_min_value_positive_force_merge_segments() -> crate::Result<()> {
        histogram_test_min_value_positive_merge_segments(true)
    }

    #[test]
    fn histogram_test_min_value_positive() -> crate::Result<()> {
        histogram_test_min_value_positive_merge_segments(false)
    }
    fn histogram_test_min_value_positive_merge_segments(merge_segments: bool) -> crate::Result<()> {
        let values = vec![10.0, 12.0, 14.0, 16.23];

        let index = get_test_index_from_values(merge_segments, &values)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "my_interval": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 1.0,
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["my_interval"]["buckets"][0]["key"], 10.0);
        assert_eq!(res["my_interval"]["buckets"][0]["doc_count"], 1);
        assert_eq!(res["my_interval"]["buckets"][1]["key"], 11.0);
        assert_eq!(res["my_interval"]["buckets"][1]["doc_count"], 0);
        assert_eq!(res["my_interval"]["buckets"][2]["key"], 12.0);
        assert_eq!(res["my_interval"]["buckets"][2]["doc_count"], 1);
        assert_eq!(res["my_interval"]["buckets"][3]["key"], 13.0);
        assert_eq!(res["my_interval"]["buckets"][3]["doc_count"], 0);
        assert_eq!(res["my_interval"]["buckets"][6]["key"], 16.0);
        assert_eq!(res["my_interval"]["buckets"][6]["doc_count"], 1);
        assert_eq!(res["my_interval"]["buckets"][7], Value::Null);

        Ok(())
    }

    #[test]
    fn histogram_simple_test() -> crate::Result<()> {
        let index = get_test_index_with_num_docs(false, 100)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 1.0,
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 0.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 1);
        assert_eq!(res["histogram"]["buckets"][1]["key"], 1.0);
        assert_eq!(res["histogram"]["buckets"][1]["doc_count"], 1);
        assert_eq!(res["histogram"]["buckets"][99]["key"], 99.0);
        assert_eq!(res["histogram"]["buckets"][99]["doc_count"], 1);
        assert_eq!(res["histogram"]["buckets"][100], Value::Null);
        Ok(())
    }

    #[test]
    fn histogram_memory_limit() -> crate::Result<()> {
        let index = get_test_index_with_num_docs(true, 100)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 0.1,
                }
            }
        }))
        .unwrap();

        let res = exec_request_with_query_and_memory_limit(
            agg_req,
            &index,
            None,
            AggregationLimits::new(Some(5_000), None),
        )
        .unwrap_err();
        assert!(res.to_string().starts_with(
            "Aborting aggregation because memory limit was exceeded. Limit: 5.00 KB, Current"
        ));

        Ok(())
    }

    #[test]
    fn histogram_merge_test() -> crate::Result<()> {
        // Merge buckets counts from different segments
        let values = vec![10.0, 12.0, 14.0, 16.23, 10.0, 13.0, 10.0, 12.0];

        let index = get_test_index_from_values(false, &values)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 1.0,
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 10.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 3);
        assert_eq!(res["histogram"]["buckets"][1]["key"], 11.0);
        assert_eq!(res["histogram"]["buckets"][1]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][2]["key"], 12.0);
        assert_eq!(res["histogram"]["buckets"][2]["doc_count"], 2);
        assert_eq!(res["histogram"]["buckets"][3]["key"], 13.0);
        assert_eq!(res["histogram"]["buckets"][3]["doc_count"], 1);

        Ok(())
    }
    #[test]
    fn histogram_min_doc_test_multi_segments() -> crate::Result<()> {
        histogram_min_doc_test_with_opt(false)
    }
    #[test]
    fn histogram_min_doc_test_single_segments() -> crate::Result<()> {
        histogram_min_doc_test_with_opt(true)
    }
    fn histogram_min_doc_test_with_opt(merge_segments: bool) -> crate::Result<()> {
        let values = vec![10.0, 12.0, 14.0, 16.23, 10.0, 13.0, 10.0, 12.0];

        let index = get_test_index_from_values(merge_segments, &values)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 1.0,
                    "min_doc_count": 2,
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 10.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 3);
        assert_eq!(res["histogram"]["buckets"][1]["key"], 12.0);
        assert_eq!(res["histogram"]["buckets"][1]["doc_count"], 2);
        assert_eq!(res["histogram"]["buckets"][2], Value::Null);

        Ok(())
    }

    #[test]
    fn histogram_extended_bounds_test_multi_segment() -> crate::Result<()> {
        histogram_extended_bounds_test_with_opt(false)
    }
    #[test]
    fn histogram_extended_bounds_test_single_segment() -> crate::Result<()> {
        histogram_extended_bounds_test_with_opt(true)
    }
    fn histogram_extended_bounds_test_with_opt(merge_segments: bool) -> crate::Result<()> {
        let values = vec![5.0];
        let index = get_test_index_from_values(merge_segments, &values)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 1.0,
                    "extended_bounds": {
                        "min": 2.0,
                        "max": 12.0,
                    },
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 2.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][1]["key"], 3.0);
        assert_eq!(res["histogram"]["buckets"][1]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][2]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][10]["key"], 12.0);
        assert_eq!(res["histogram"]["buckets"][10]["doc_count"], 0);

        // 2 hits
        let values = vec![5.0, 5.5];
        let index = get_test_index_from_values(merge_segments, &values)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 1.0,
                    "extended_bounds": {
                        "min": 3.0,
                        "max": 6.0,
                    },
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 3.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][1]["key"], 4.0);
        assert_eq!(res["histogram"]["buckets"][1]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][2]["key"], 5.0);
        assert_eq!(res["histogram"]["buckets"][2]["doc_count"], 2);
        assert_eq!(res["histogram"]["buckets"][3]["key"], 6.0);
        assert_eq!(res["histogram"]["buckets"][3]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][4], Value::Null);

        // 1 hit outside bounds
        let values = vec![15.0];
        let index = get_test_index_from_values(merge_segments, &values)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 1.0,
                    "extended_bounds": {
                        "min": 3.0,
                        "max": 6.0,
                    },
                    "hard_bounds": {
                        "min": 3.0,
                        "max": 6.0,
                    },
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 3.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][1]["key"], 4.0);
        assert_eq!(res["histogram"]["buckets"][1]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][2]["key"], 5.0);
        assert_eq!(res["histogram"]["buckets"][2]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][3]["key"], 6.0);
        assert_eq!(res["histogram"]["buckets"][3]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][4], Value::Null);

        Ok(())
    }

    #[test]
    fn histogram_hard_bounds_test_multi_segment() -> crate::Result<()> {
        histogram_hard_bounds_test_with_opt(false)
    }
    #[test]
    fn histogram_hard_bounds_test_single_segment() -> crate::Result<()> {
        histogram_hard_bounds_test_with_opt(true)
    }
    fn histogram_hard_bounds_test_with_opt(merge_segments: bool) -> crate::Result<()> {
        let values = vec![10.0, 12.0, 14.0, 16.23, 10.0, 13.0, 10.0, 12.0];

        let index = get_test_index_from_values(merge_segments, &values)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 1.0,
                    "hard_bounds": {
                        "min": 2.0,
                        "max": 12.0,
                    },
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 10.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 3);
        assert_eq!(res["histogram"]["buckets"][1]["key"], 11.0);
        assert_eq!(res["histogram"]["buckets"][1]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][2]["key"], 12.0);
        assert_eq!(res["histogram"]["buckets"][2]["doc_count"], 2);

        assert_eq!(res["histogram"]["buckets"][3], Value::Null);

        // hard_bounds and extended_bounds will act like a force bounds
        //
        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 1.0,
                    "extended_bounds": {
                        "min": 2.0,
                        "max": 12.0,
                    },
                    "hard_bounds": {
                        "min": 2.0,
                        "max": 12.0,
                    },
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 2.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][1]["key"], 3.0);
        assert_eq!(res["histogram"]["buckets"][1]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][10]["key"], 12.0);
        assert_eq!(res["histogram"]["buckets"][10]["doc_count"], 2);

        assert_eq!(res["histogram"]["buckets"][11], Value::Null);

        // invalid request
        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 1.0,
                    "extended_bounds": {
                        "min": 1.0,
                        "max": 12.0,
                    },
                    "hard_bounds": {
                        "min": 2.0,
                        "max": 12.0,
                    },
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index).unwrap_err();
        assert_eq!(
            res.to_string(),
            "An invalid argument was passed: 'extended_bounds have to be inside hard_bounds, \
             extended_bounds: [1,12], hard_bounds [2,12]'"
        );

        Ok(())
    }

    #[test]
    fn histogram_empty_result_behaviour_test_single_segment() -> crate::Result<()> {
        histogram_empty_result_behaviour_test_with_opt(true)
    }

    #[test]
    fn histogram_empty_result_behaviour_test_multi_segment() -> crate::Result<()> {
        histogram_empty_result_behaviour_test_with_opt(false)
    }

    fn histogram_empty_result_behaviour_test_with_opt(merge_segments: bool) -> crate::Result<()> {
        let index = get_test_index_2_segments(merge_segments)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 1.0,
                }
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req.clone(), &index, Some(("text", "blubberasdf")))?;

        assert_eq!(
            res,
            json!({
                "histogram": {
                    "buckets": []
                }
            })
        );

        // test index without segments
        let values = vec![];

        // Don't merge empty segments
        let index = get_test_index_from_values(false, &values)?;

        let res = exec_request_with_query(agg_req, &index, Some(("text", "blubberasdf")))?;

        assert_eq!(
            res,
            json!({
                "histogram": {
                    "buckets": []
                }
            })
        );

        // test index without segments
        let values = vec![];

        // Don't merge empty segments
        let index = get_test_index_from_values(false, &values)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 1.0,
                    "extended_bounds": {
                        "min": 2.0,
                        "max": 12.0,
                    },
                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 2.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][1]["key"], 3.0);
        assert_eq!(res["histogram"]["buckets"][1]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][2]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][10]["key"], 12.0);
        assert_eq!(res["histogram"]["buckets"][10]["doc_count"], 0);

        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 1.0,
                    "extended_bounds": {
                        "min": 2.0,
                        "max": 5.0,
                    },
                    "hard_bounds": {
                        "min": 2.0,
                        "max": 12.0,
                    },

                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 2.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][1]["key"], 3.0);
        assert_eq!(res["histogram"]["buckets"][1]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][2]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][10], Value::Null);

        // hard_bounds will not extend the result
        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 1.0,
                    "hard_bounds": {
                        "min": 2.0,
                        "max": 12.0,
                    },

                }
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(
            res,
            json!({
                "histogram": {
                    "buckets": []
                }
            })
        );

        let sub_agg_req: Aggregations = serde_json::from_value(json!({
            "stats": { "stats": { "field": "score_f64", } },
            "avg": { "avg": { "field": "score_f64", } }

        }))
        .unwrap();

        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 1.0,
                    "extended_bounds": {
                        "min": 2.0,
                        "max": 12.0,
                    },
                },
                "aggs": sub_agg_req
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(
            res["histogram"]["buckets"][0],
            json!({
                "avg": {
                    "value": Value::Null
                },
                "doc_count": 0,
                "key": 2.0,
                "stats": {
                    "sum": 0.0,
                    "count": 0,
                    "min": Value::Null,
                    "max": Value::Null,
                    "avg": Value::Null,
                }
            })
        );
        assert_eq!(res["histogram"]["buckets"][0]["key"], 2.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][1]["key"], 3.0);
        assert_eq!(res["histogram"]["buckets"][1]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][2]["doc_count"], 0);
        assert_eq!(res["histogram"]["buckets"][10]["key"], 12.0);
        assert_eq!(res["histogram"]["buckets"][10]["doc_count"], 0);

        Ok(())
    }

    #[test]
    fn histogram_single_bucket_test_single_segment() -> crate::Result<()> {
        histogram_single_bucket_test_with_opt(true)
    }

    #[test]
    fn histogram_single_bucket_test_multi_segment() -> crate::Result<()> {
        histogram_single_bucket_test_with_opt(false)
    }

    fn histogram_single_bucket_test_with_opt(merge_segments: bool) -> crate::Result<()> {
        let index = get_test_index_2_segments(merge_segments)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 100000.0,
                },
            }
        }))
        .unwrap();

        let agg_res = exec_request(agg_req, &index)?;

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 0.0);
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 9);
        assert_eq!(res["histogram"]["buckets"][1], Value::Null);

        Ok(())
    }

    #[test]
    fn histogram_date_test_single_segment() -> crate::Result<()> {
        histogram_date_test_with_opt(true)
    }

    #[test]
    fn histogram_date_test_multi_segment() -> crate::Result<()> {
        histogram_date_test_with_opt(false)
    }

    fn histogram_date_test_with_opt(merge_segments: bool) -> crate::Result<()> {
        let index = get_test_index_2_segments(merge_segments)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "date",
                    "interval": 86400000.0, // one day in milliseconds seconds
                },
            }
        }))
        .unwrap();

        let agg_res = exec_request(agg_req, &index)?;

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

        assert_eq!(res["histogram"]["buckets"][0]["key"], 1546300800000.0);
        assert_eq!(
            res["histogram"]["buckets"][0]["key_as_string"],
            "2019-01-01T00:00:00Z"
        );
        assert_eq!(res["histogram"]["buckets"][0]["doc_count"], 1);

        assert_eq!(res["histogram"]["buckets"][1]["key"], 1546387200000.0);
        assert_eq!(
            res["histogram"]["buckets"][1]["key_as_string"],
            "2019-01-02T00:00:00Z"
        );

        assert_eq!(res["histogram"]["buckets"][1]["doc_count"], 5);

        assert_eq!(res["histogram"]["buckets"][2]["key"], 1546473600000.0);
        assert_eq!(
            res["histogram"]["buckets"][2]["key_as_string"],
            "2019-01-03T00:00:00Z"
        );

        assert_eq!(res["histogram"]["buckets"][3], Value::Null);

        Ok(())
    }

    #[test]
    fn histogram_invalid_request() -> crate::Result<()> {
        let index = get_test_index_2_segments(true)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 0.0,
                },
            }
        }))
        .unwrap();

        let agg_res = exec_request(agg_req, &index);

        assert!(agg_res.is_err());

        Ok(())
    }

    #[test]
    fn histogram_keyed_buckets_test() -> crate::Result<()> {
        let index = get_test_index_with_num_docs(false, 100)?;
        let agg_req: Aggregations = serde_json::from_value(json!({
            "histogram": {
                "histogram": {
                    "field": "score_f64",
                    "interval": 50.0,
                    "keyed": true
                },
            }
        }))
        .unwrap();

        let res = exec_request(agg_req, &index)?;

        assert_eq!(
            res,
            json!({
                "histogram": {
                    "buckets": {
                        "0": {
                            "key": 0.0,
                            "doc_count": 50
                        },
                        "50": {
                            "key": 50.0,
                            "doc_count": 50
                        }
                    }
                }
            })
        );

        Ok(())
    }
}
