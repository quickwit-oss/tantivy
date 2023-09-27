use std::fmt::Debug;
use std::ops::Range;

use columnar::{ColumnType, MonotonicallyMappableToU64};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::aggregation::agg_limits::ResourceLimitGuard;
use crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateAggregationResults, IntermediateBucketResult,
    IntermediateRangeBucketEntry, IntermediateRangeBucketResult,
};
use crate::aggregation::segment_agg_result::{
    build_segment_agg_collector, SegmentAggregationCollector,
};
use crate::aggregation::{
    f64_from_fastfield_u64, f64_to_fastfield_u64, format_date, Key, SerializedKey,
};
use crate::TantivyError;

/// Provide user-defined buckets to aggregate on.
/// Two special buckets will automatically be created to cover the whole range of values.
/// The provided buckets have to be continuous.
/// During the aggregation, the values extracted from the fast_field `field` will be checked
/// against each bucket range. Note that this aggregation includes the from value and excludes the
/// to value for each range.
///
/// Result type is [`BucketResult`](crate::aggregation::agg_result::BucketResult) with
/// [`RangeBucketEntry`](crate::aggregation::agg_result::RangeBucketEntry) on the
/// `AggregationCollector`.
///
/// Result type is
/// [`IntermediateBucketResult`](crate::aggregation::intermediate_agg_result::IntermediateBucketResult) with
/// [`IntermediateRangeBucketEntry`](crate::aggregation::intermediate_agg_result::IntermediateRangeBucketEntry) on the
/// `DistributedAggregationCollector`.
///
/// # Limitations/Compatibility
/// Overlapping ranges are not yet supported.
///
/// # Request JSON Format
/// ```json
/// {
///     "my_ranges": {
///         "field": "score",
///         "ranges": [
///             { "to": 3.0 },
///             { "from": 3.0, "to": 7.0 },
///             { "from": 7.0, "to": 20.0 },
///             { "from": 20.0 }
///         ]
///     }
/// }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RangeAggregation {
    /// The field to aggregate on.
    pub field: String,
    /// Note that this aggregation includes the from value and excludes the to value for each
    /// range. Extra buckets will be created until the first to, and last from, if necessary.
    pub ranges: Vec<RangeAggregationRange>,
    /// Whether to return the buckets as a hash map
    #[serde(default)]
    pub keyed: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// The range for one range bucket.
pub struct RangeAggregationRange {
    /// Custom key for the range bucket
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub key: Option<String>,
    /// The from range value, which is inclusive in the range.
    /// `None` equals to an open ended interval.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub from: Option<f64>,
    /// The to range value, which is not inclusive in the range.
    /// `None` equals to an open ended interval.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub to: Option<f64>,
}

impl From<Range<f64>> for RangeAggregationRange {
    fn from(range: Range<f64>) -> Self {
        let from = if range.start == f64::MIN {
            None
        } else {
            Some(range.start)
        };
        let to = if range.end == f64::MAX {
            None
        } else {
            Some(range.end)
        };
        RangeAggregationRange {
            key: None,
            from,
            to,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// Internally used u64 range for one range bucket.
pub(crate) struct InternalRangeAggregationRange {
    /// Custom key for the range bucket
    key: Option<String>,
    /// `u64` range value
    range: Range<u64>,
}

impl From<Range<u64>> for InternalRangeAggregationRange {
    fn from(range: Range<u64>) -> Self {
        InternalRangeAggregationRange { key: None, range }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SegmentRangeAndBucketEntry {
    range: Range<u64>,
    bucket: SegmentRangeBucketEntry,
}

/// The collector puts values from the fast field into the correct buckets and does a conversion to
/// the correct datatype.
#[derive(Clone, Debug)]
pub struct SegmentRangeCollector {
    /// The buckets containing the aggregation data.
    buckets: Vec<SegmentRangeAndBucketEntry>,
    column_type: ColumnType,
    pub(crate) accessor_idx: usize,
}

#[derive(Clone)]
pub(crate) struct SegmentRangeBucketEntry {
    pub key: Key,
    pub doc_count: u64,
    pub sub_aggregation: Option<Box<dyn SegmentAggregationCollector>>,
    /// The from range of the bucket. Equals `f64::MIN` when `None`.
    pub from: Option<f64>,
    /// The to range of the bucket. Equals `f64::MAX` when `None`. Open interval, `to` is not
    /// inclusive.
    pub to: Option<f64>,
}

impl Debug for SegmentRangeBucketEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentRangeBucketEntry")
            .field("key", &self.key)
            .field("doc_count", &self.doc_count)
            .field("from", &self.from)
            .field("to", &self.to)
            .finish()
    }
}
impl SegmentRangeBucketEntry {
    pub(crate) fn into_intermediate_bucket_entry(
        self,
        agg_with_accessor: &AggregationsWithAccessor,
    ) -> crate::Result<IntermediateRangeBucketEntry> {
        let mut sub_aggregation_res = IntermediateAggregationResults::default();
        if let Some(sub_aggregation) = self.sub_aggregation {
            sub_aggregation
                .add_intermediate_aggregation_result(agg_with_accessor, &mut sub_aggregation_res)?
        } else {
            Default::default()
        };

        Ok(IntermediateRangeBucketEntry {
            key: self.key.into(),
            doc_count: self.doc_count,
            sub_aggregation: sub_aggregation_res,
            from: self.from,
            to: self.to,
        })
    }
}

impl SegmentAggregationCollector for SegmentRangeCollector {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_with_accessor: &AggregationsWithAccessor,
        results: &mut IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let field_type = self.column_type;
        let name = agg_with_accessor.aggs.keys[self.accessor_idx].to_string();
        let sub_agg = &agg_with_accessor.aggs.values[self.accessor_idx].sub_aggregation;

        let buckets: FxHashMap<SerializedKey, IntermediateRangeBucketEntry> = self
            .buckets
            .into_iter()
            .map(move |range_bucket| {
                Ok((
                    range_to_string(&range_bucket.range, &field_type)?,
                    range_bucket
                        .bucket
                        .into_intermediate_bucket_entry(sub_agg)?,
                ))
            })
            .collect::<crate::Result<_>>()?;

        let bucket = IntermediateBucketResult::Range(IntermediateRangeBucketResult {
            buckets,
            column_type: Some(self.column_type),
        });

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

        bucket_agg_accessor
            .column_block_accessor
            .fetch_block(docs, &bucket_agg_accessor.accessor);

        for (doc, val) in bucket_agg_accessor.column_block_accessor.iter_docid_vals() {
            let bucket_pos = self.get_bucket_pos(val);

            let bucket = &mut self.buckets[bucket_pos];

            bucket.bucket.doc_count += 1;
            if let Some(sub_aggregation) = &mut bucket.bucket.sub_aggregation {
                sub_aggregation.collect(doc, &mut bucket_agg_accessor.sub_aggregation)?;
            }
        }

        Ok(())
    }

    fn flush(&mut self, agg_with_accessor: &mut AggregationsWithAccessor) -> crate::Result<()> {
        let sub_aggregation_accessor =
            &mut agg_with_accessor.aggs.values[self.accessor_idx].sub_aggregation;

        for bucket in self.buckets.iter_mut() {
            if let Some(sub_agg) = bucket.bucket.sub_aggregation.as_mut() {
                sub_agg.flush(sub_aggregation_accessor)?;
            }
        }

        Ok(())
    }
}

impl SegmentRangeCollector {
    pub(crate) fn from_req_and_validate(
        req: &RangeAggregation,
        sub_aggregation: &mut AggregationsWithAccessor,
        limits: &ResourceLimitGuard,
        field_type: ColumnType,
        accessor_idx: usize,
    ) -> crate::Result<Self> {
        // The range input on the request is f64.
        // We need to convert to u64 ranges, because we read the values as u64.
        // The mapping from the conversion is monotonic so ordering is preserved.
        let buckets: Vec<_> = extend_validate_ranges(&req.ranges, &field_type)?
            .iter()
            .map(|range| {
                let key = range
                    .key
                    .clone()
                    .map(|key| Ok(Key::Str(key)))
                    .unwrap_or_else(|| range_to_key(&range.range, &field_type))?;
                let to = if range.range.end == u64::MAX {
                    None
                } else {
                    Some(f64_from_fastfield_u64(range.range.end, &field_type))
                };
                let from = if range.range.start == u64::MIN {
                    None
                } else {
                    Some(f64_from_fastfield_u64(range.range.start, &field_type))
                };
                let sub_aggregation = if sub_aggregation.is_empty() {
                    None
                } else {
                    Some(build_segment_agg_collector(sub_aggregation)?)
                };

                Ok(SegmentRangeAndBucketEntry {
                    range: range.range.clone(),
                    bucket: SegmentRangeBucketEntry {
                        doc_count: 0,
                        sub_aggregation,
                        key,
                        from,
                        to,
                    },
                })
            })
            .collect::<crate::Result<_>>()?;

        limits.add_memory_consumed(
            buckets.len() as u64 * std::mem::size_of::<SegmentRangeAndBucketEntry>() as u64,
        )?;

        Ok(SegmentRangeCollector {
            buckets,
            column_type: field_type,
            accessor_idx,
        })
    }

    #[inline]
    fn get_bucket_pos(&self, val: u64) -> usize {
        let pos = self
            .buckets
            .binary_search_by_key(&val, |probe| probe.range.start)
            .unwrap_or_else(|pos| pos - 1);
        debug_assert!(self.buckets[pos].range.contains(&val));
        pos
    }
}

/// Converts the user provided f64 range value to fast field value space.
///
/// Internally fast field values are always stored as u64.
/// If the fast field has u64 `[1, 2, 5]`, these values are stored as is in the fast field.
/// A fast field with f64 `[1.0, 2.0, 5.0]` is converted to u64 space, using a
/// monotonic mapping function, so the order is preserved.
///
/// Consequently, a f64 user range 1.0..3.0 needs to be converted to fast field value space using
/// the same monotonic mapping function, so that the provided ranges contain the u64 values in the
/// fast field.
/// The alternative would be that every value read would be converted to the f64 range, but that is
/// more computational expensive when many documents are hit.
fn to_u64_range(
    range: &RangeAggregationRange,
    field_type: &ColumnType,
) -> crate::Result<InternalRangeAggregationRange> {
    let start = if let Some(from) = range.from {
        f64_to_fastfield_u64(from, field_type)
            .ok_or_else(|| TantivyError::InvalidArgument("invalid field type".to_string()))?
    } else {
        u64::MIN
    };

    let end = if let Some(to) = range.to {
        f64_to_fastfield_u64(to, field_type)
            .ok_or_else(|| TantivyError::InvalidArgument("invalid field type".to_string()))?
    } else {
        u64::MAX
    };

    Ok(InternalRangeAggregationRange {
        key: range.key.clone(),
        range: start..end,
    })
}

/// Extends the provided buckets to contain the whole value range, by inserting buckets at the
/// beginning and end and filling gaps.
fn extend_validate_ranges(
    buckets: &[RangeAggregationRange],
    field_type: &ColumnType,
) -> crate::Result<Vec<InternalRangeAggregationRange>> {
    let mut converted_buckets = buckets
        .iter()
        .map(|range| to_u64_range(range, field_type))
        .collect::<crate::Result<Vec<_>>>()?;

    converted_buckets.sort_by_key(|bucket| bucket.range.start);
    if converted_buckets[0].range.start != u64::MIN {
        converted_buckets.insert(0, (u64::MIN..converted_buckets[0].range.start).into());
    }

    if converted_buckets[converted_buckets.len() - 1].range.end != u64::MAX {
        converted_buckets
            .push((converted_buckets[converted_buckets.len() - 1].range.end..u64::MAX).into());
    }

    // fill up holes in the ranges
    let find_hole = |converted_buckets: &[InternalRangeAggregationRange]| {
        for (pos, ranges) in converted_buckets.windows(2).enumerate() {
            if ranges[0].range.end > ranges[1].range.start {
                return Err(TantivyError::InvalidArgument(format!(
                    "Overlapping ranges not supported range {:?}, range+1 {:?}",
                    ranges[0], ranges[1]
                )));
            }
            if ranges[0].range.end != ranges[1].range.start {
                return Ok(Some(pos));
            }
        }
        Ok(None)
    };

    while let Some(hole_pos) = find_hole(&converted_buckets)? {
        let new_range =
            converted_buckets[hole_pos].range.end..converted_buckets[hole_pos + 1].range.start;
        converted_buckets.insert(hole_pos + 1, new_range.into());
    }

    Ok(converted_buckets)
}

pub(crate) fn range_to_string(
    range: &Range<u64>,
    field_type: &ColumnType,
) -> crate::Result<String> {
    // is_start is there for malformed requests, e.g. ig the user passes the range u64::MIN..0.0,
    // it should be rendered as "*-0" and not "*-*"
    let to_str = |val: u64, is_start: bool| {
        if (is_start && val == u64::MIN) || (!is_start && val == u64::MAX) {
            Ok("*".to_string())
        } else if *field_type == ColumnType::DateTime {
            let val = i64::from_u64(val);
            format_date(val)
        } else {
            Ok(f64_from_fastfield_u64(val, field_type).to_string())
        }
    };

    Ok(format!(
        "{}-{}",
        to_str(range.start, true)?,
        to_str(range.end, false)?
    ))
}

pub(crate) fn range_to_key(range: &Range<u64>, field_type: &ColumnType) -> crate::Result<Key> {
    Ok(Key::Str(range_to_string(range, field_type)?))
}

#[cfg(test)]
mod tests {

    use columnar::MonotonicallyMappableToU64;
    use serde_json::Value;

    use super::*;
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::tests::{
        exec_request, exec_request_with_query, get_test_index_2_segments,
        get_test_index_with_num_docs,
    };
    use crate::aggregation::AggregationLimits;

    pub fn get_collector_from_ranges(
        ranges: Vec<RangeAggregationRange>,
        field_type: ColumnType,
    ) -> SegmentRangeCollector {
        let req = RangeAggregation {
            field: "dummy".to_string(),
            ranges,
            ..Default::default()
        };

        SegmentRangeCollector::from_req_and_validate(
            &req,
            &mut Default::default(),
            &AggregationLimits::default().new_guard(),
            field_type,
            0,
        )
        .expect("unexpected error")
    }

    #[test]
    fn range_fraction_test() -> crate::Result<()> {
        let index = get_test_index_with_num_docs(false, 100)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "range": {
                "range": {
                    "field": "fraction_f64",
                    "ranges": [
                        {"from": 0.0, "to": 0.1},
                        {"from": 0.1, "to": 0.2},
                    ]
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(res["range"]["buckets"][0]["key"], "*-0");
        assert_eq!(res["range"]["buckets"][0]["doc_count"], 0);
        assert_eq!(res["range"]["buckets"][1]["key"], "0-0.1");
        assert_eq!(res["range"]["buckets"][1]["doc_count"], 10);
        assert_eq!(res["range"]["buckets"][2]["key"], "0.1-0.2");
        assert_eq!(res["range"]["buckets"][2]["doc_count"], 10);
        assert_eq!(res["range"]["buckets"][3]["key"], "0.2-*");
        assert_eq!(res["range"]["buckets"][3]["doc_count"], 80);

        Ok(())
    }

    #[test]
    fn range_fraction_test_with_sub_agg() -> crate::Result<()> {
        let index = get_test_index_with_num_docs(false, 100)?;

        let sub_agg_req: Aggregations = serde_json::from_value(json!({
            "avg": { "avg": { "field": "score_f64", } }

        }))
        .unwrap();

        let agg_req: Aggregations = serde_json::from_value(json!({
            "range": {
                "range": {
                    "field": "fraction_f64",
                    "ranges": [
                        {"from": 0.0, "to": 0.1},
                        {"from": 0.1, "to": 0.2},
                    ]
                },
                "aggs": sub_agg_req
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(res["range"]["buckets"][0]["key"], "*-0");
        assert_eq!(res["range"]["buckets"][0]["doc_count"], 0);
        assert_eq!(res["range"]["buckets"][1]["key"], "0-0.1");
        assert_eq!(res["range"]["buckets"][1]["doc_count"], 10);
        assert_eq!(res["range"]["buckets"][2]["key"], "0.1-0.2");
        assert_eq!(res["range"]["buckets"][2]["doc_count"], 10);
        assert_eq!(res["range"]["buckets"][3]["key"], "0.2-*");
        assert_eq!(res["range"]["buckets"][3]["doc_count"], 80);

        Ok(())
    }

    #[test]
    fn range_keyed_buckets_test() -> crate::Result<()> {
        let index = get_test_index_with_num_docs(false, 100)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "range": {
                "range": {
                    "field": "fraction_f64",
                    "ranges": [
                        {"from": 0.0, "to": 0.1},
                        {"from": 0.1, "to": 0.2},
                    ],
                    "keyed": true
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res,
            json!({
                "range": {
                    "buckets": {
                        "*-0": { "key": "*-0", "doc_count": 0, "to": 0.0},
                        "0-0.1": {"key": "0-0.1", "doc_count": 10, "from": 0.0, "to": 0.1},
                        "0.1-0.2": {"key": "0.1-0.2", "doc_count": 10, "from": 0.1, "to": 0.2},
                        "0.2-*": {"key": "0.2-*", "doc_count": 80, "from": 0.2},
                    }
                }
            })
        );

        Ok(())
    }

    #[test]
    fn range_custom_key_test() -> crate::Result<()> {
        let index = get_test_index_with_num_docs(false, 100)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "range": {
                "range": {
                    "field": "fraction_f64",
                    "ranges": [
                        {"key": "custom-key-0-to-0.1", "from": 0.0, "to": 0.1},
                        {"from": 0.1, "to": 0.2},
                    ],
                    "keyed": false
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res,
            json!({
                "range": {
                    "buckets": [
                        {"key": "*-0", "doc_count": 0, "to": 0.0},
                        {"key": "custom-key-0-to-0.1", "doc_count": 10, "from": 0.0, "to": 0.1},
                        {"key": "0.1-0.2", "doc_count": 10, "from": 0.1, "to": 0.2},
                        {"key": "0.2-*", "doc_count": 80, "from": 0.2}
                    ]
                }
            })
        );

        Ok(())
    }

    #[test]
    fn range_date_test_single_segment() -> crate::Result<()> {
        range_date_test_with_opt(true)
    }

    #[test]
    fn range_date_test_multi_segment() -> crate::Result<()> {
        range_date_test_with_opt(false)
    }

    fn range_date_test_with_opt(merge_segments: bool) -> crate::Result<()> {
        let index = get_test_index_2_segments(merge_segments)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "date_ranges": {
                "range": {
                    "field": "date",
                    "ranges": [
                        {"to": 1546300800000000000i64},
                        {"from": 1546300800000000000i64, "to": 1546387200000000000i64},
                    ],
                    "keyed": false
                },
            }
        }))
        .unwrap();

        let agg_res = exec_request(agg_req, &index)?;

        let res: Value = serde_json::from_str(&serde_json::to_string(&agg_res)?)?;

        assert_eq!(
            res["date_ranges"]["buckets"][0]["from_as_string"],
            Value::Null
        );
        assert_eq!(
            res["date_ranges"]["buckets"][0]["key"],
            "*-2019-01-01T00:00:00Z"
        );
        assert_eq!(
            res["date_ranges"]["buckets"][1]["from_as_string"],
            "2019-01-01T00:00:00Z"
        );
        assert_eq!(
            res["date_ranges"]["buckets"][1]["to_as_string"],
            "2019-01-02T00:00:00Z"
        );

        assert_eq!(
            res["date_ranges"]["buckets"][2]["from_as_string"],
            "2019-01-02T00:00:00Z"
        );
        assert_eq!(
            res["date_ranges"]["buckets"][2]["to_as_string"],
            Value::Null
        );

        Ok(())
    }

    #[test]
    fn range_custom_key_keyed_buckets_test() -> crate::Result<()> {
        let index = get_test_index_with_num_docs(false, 100)?;

        let agg_req: Aggregations = serde_json::from_value(json!({
            "range": {
                "range": {
                    "field": "fraction_f64",
                    "ranges": [
                        {"key": "custom-key-0-to-0.1", "from": 0.0, "to": 0.1},
                    ],
                    "keyed": true
                },
            }
        }))
        .unwrap();

        let res = exec_request_with_query(agg_req, &index, None)?;

        assert_eq!(
            res,
            json!({
                "range": {
                    "buckets": {
                        "*-0": { "key": "*-0", "doc_count": 0, "to": 0.0},
                        "custom-key-0-to-0.1": {"key": "custom-key-0-to-0.1", "doc_count": 10, "from": 0.0, "to": 0.1},
                        "0.1-*": {"key": "0.1-*", "doc_count": 90, "from": 0.1},
                    }
                }
            })
        );

        Ok(())
    }

    #[test]
    fn bucket_test_extend_range_hole() {
        let buckets = vec![(10f64..20f64).into(), (30f64..40f64).into()];
        let collector = get_collector_from_ranges(buckets, ColumnType::F64);

        let buckets = collector.buckets;
        assert_eq!(buckets[0].range.start, u64::MIN);
        assert_eq!(buckets[0].range.end, 10f64.to_u64());
        assert_eq!(buckets[1].range.start, 10f64.to_u64());
        assert_eq!(buckets[1].range.end, 20f64.to_u64());
        // Added bucket to fill hole
        assert_eq!(buckets[2].range.start, 20f64.to_u64());
        assert_eq!(buckets[2].range.end, 30f64.to_u64());
        assert_eq!(buckets[3].range.start, 30f64.to_u64());
        assert_eq!(buckets[3].range.end, 40f64.to_u64());
    }

    #[test]
    fn bucket_test_range_conversion_special_case() {
        // the monotonic conversion between f64 and u64, does not map f64::MIN.to_u64() ==
        // u64::MIN, but the into trait converts f64::MIN/MAX to None
        let buckets = vec![
            (f64::MIN..10f64).into(),
            (10f64..20f64).into(),
            (20f64..f64::MAX).into(),
        ];
        let collector = get_collector_from_ranges(buckets, ColumnType::F64);

        let buckets = collector.buckets;
        assert_eq!(buckets[0].range.start, u64::MIN);
        assert_eq!(buckets[0].range.end, 10f64.to_u64());
        assert_eq!(buckets[1].range.start, 10f64.to_u64());
        assert_eq!(buckets[1].range.end, 20f64.to_u64());
        assert_eq!(buckets[2].range.start, 20f64.to_u64());
        assert_eq!(buckets[2].range.end, u64::MAX);
        assert_eq!(buckets.len(), 3);
    }

    #[test]
    fn bucket_range_test_negative_vals() {
        let buckets = vec![(-10f64..-1f64).into()];
        let collector = get_collector_from_ranges(buckets, ColumnType::F64);

        let buckets = collector.buckets;
        assert_eq!(&buckets[0].bucket.key.to_string(), "*--10");
        assert_eq!(&buckets[buckets.len() - 1].bucket.key.to_string(), "-1-*");
    }
    #[test]
    fn bucket_range_test_positive_vals() {
        let buckets = vec![(0f64..10f64).into()];
        let collector = get_collector_from_ranges(buckets, ColumnType::F64);

        let buckets = collector.buckets;
        assert_eq!(&buckets[0].bucket.key.to_string(), "*-0");
        assert_eq!(&buckets[buckets.len() - 1].bucket.key.to_string(), "10-*");
    }

    #[test]
    fn range_binary_search_test_u64() {
        let check_ranges = |ranges: Vec<RangeAggregationRange>| {
            let collector = get_collector_from_ranges(ranges, ColumnType::U64);
            let search = |val: u64| collector.get_bucket_pos(val);

            assert_eq!(search(u64::MIN), 0);
            assert_eq!(search(9), 0);
            assert_eq!(search(10), 1);
            assert_eq!(search(11), 1);
            assert_eq!(search(99), 1);
            assert_eq!(search(100), 2);
            assert_eq!(search(u64::MAX - 1), 2); // Since the end range is never included, the max
                                                 // value
        };

        let ranges = vec![(10.0..100.0).into()];
        check_ranges(ranges);

        let ranges = vec![
            RangeAggregationRange {
                key: None,
                to: Some(10.0),
                from: None,
            },
            (10.0..100.0).into(),
        ];
        check_ranges(ranges);

        let ranges = vec![
            RangeAggregationRange {
                key: None,
                to: Some(10.0),
                from: None,
            },
            (10.0..100.0).into(),
            RangeAggregationRange {
                key: None,
                to: None,
                from: Some(100.0),
            },
        ];
        check_ranges(ranges);
    }

    #[test]
    fn range_binary_search_test_f64() {
        let ranges = vec![(10.0..100.0).into()];

        let collector = get_collector_from_ranges(ranges, ColumnType::F64);
        let search = |val: u64| collector.get_bucket_pos(val);

        assert_eq!(search(u64::MIN), 0);
        assert_eq!(search(9f64.to_u64()), 0);
        assert_eq!(search(10f64.to_u64()), 1);
        assert_eq!(search(11f64.to_u64()), 1);
        assert_eq!(search(99f64.to_u64()), 1);
        assert_eq!(search(100f64.to_u64()), 2);
        assert_eq!(search(u64::MAX - 1), 2); // Since the end range is never included,
                                             // the max value
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    use super::*;
    use crate::aggregation::bucket::range::tests::get_collector_from_ranges;

    const TOTAL_DOCS: u64 = 1_000_000u64;
    const NUM_DOCS: u64 = 50_000u64;

    fn get_collector_with_buckets(num_buckets: u64, num_docs: u64) -> SegmentRangeCollector {
        let bucket_size = num_docs / num_buckets;
        let mut buckets: Vec<RangeAggregationRange> = vec![];
        for i in 0..num_buckets {
            let bucket_start = (i * bucket_size) as f64;
            buckets.push((bucket_start..bucket_start + bucket_size as f64).into())
        }

        get_collector_from_ranges(buckets, ColumnType::U64)
    }

    fn get_rand_docs(total_docs: u64, num_docs_returned: u64) -> Vec<u64> {
        let mut rng = thread_rng();

        let all_docs = (0..total_docs - 1).collect_vec();
        let mut vals = all_docs
            .as_slice()
            .choose_multiple(&mut rng, num_docs_returned as usize)
            .cloned()
            .collect_vec();
        vals.sort();
        vals
    }

    fn bench_range_binary_search(b: &mut test::Bencher, num_buckets: u64) {
        let collector = get_collector_with_buckets(num_buckets, TOTAL_DOCS);
        let vals = get_rand_docs(TOTAL_DOCS, NUM_DOCS);
        b.iter(|| {
            let mut bucket_pos = 0;
            for val in &vals {
                bucket_pos = collector.get_bucket_pos(*val);
            }
            bucket_pos
        })
    }

    #[bench]
    fn bench_range_100_buckets(b: &mut test::Bencher) {
        bench_range_binary_search(b, 100)
    }

    #[bench]
    fn bench_range_10_buckets(b: &mut test::Bencher) {
        bench_range_binary_search(b, 10)
    }
}
