use std::collections::HashMap;
use std::fmt::Formatter;

use columnar::{ColumnarReader, DynamicColumn};
use regex::Regex;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{TopHitsMetricResult, TopHitsVecEntry};
use crate::aggregation::bucket::Order;
use crate::aggregation::intermediate_agg_result::{
    IntermediateAggregationResult, IntermediateMetricResult,
};
use crate::aggregation::segment_agg_result::SegmentAggregationCollector;
use crate::collector::TopNComputer;
use crate::schema::OwnedValue;
use crate::{DocAddress, DocId, SegmentOrdinal};

/// # Top Hits
///
/// The top hits aggregation is a useful tool to answer questions like:
/// - "What are the most recent posts by each author?"
/// - "What are the most popular items in each category?"
///
/// It does so by keeping track of the most relevant document being aggregated,
/// in terms of a sort criterion that can consist of multiple fields and their
/// sort-orders (ascending or descending).
///
/// `top_hits` should not be used as a top-level aggregation. It is intended to be
/// used as a sub-aggregation, inside a `terms` aggregation or a `filters` aggregation,
/// for example.
///
/// The following example demonstrates a request for the top_hits aggregation:
/// ```JSON
/// {
///     "aggs": {
///         "top_authors": {
///             "terms": {
///                 "field": "author",
///                 "size": 5
///             }
///         },
///         "aggs": {
///             "top_hits": {
///                 "size": 2,
///                 "from": 0
///                 "sort": [
///                     { "date": "desc" }
///                 ]
///             }
///         }
/// }
/// ```
///
/// This request will return an object containing the top two documents, sorted
/// by the `date` field in descending order. You can also sort by multiple fields, which
/// helps to resolve ties. The aggregation object for each bucket will look like:
/// ```JSON
/// {
///     "hits": [
///         {
///           "id": <doc_address>,
///           score: [<time_u64>]
///         },
///         {
///             "id": <doc_address>,
///             score: [<time_u64>]
///         }
///     ]
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TopHitsAggregation {
    sort: Vec<KeyOrder>,
    size: usize,
    from: Option<usize>,

    #[serde(flatten)]
    retrieval: RetrievalFields,
}

const fn default_doc_value_fields() -> Vec<String> {
    Vec::new()
}

/// Search query spec for each matched document
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct RetrievalFields {
    /// The fast fields to return for each hit.
    /// This is the only variant supported for now.
    /// TODO: support the {field, format} variant for custom formatting.
    #[serde(rename = "docvalue_fields")]
    #[serde(default = "default_doc_value_fields")]
    pub doc_value_fields: Vec<String>,
}

/// Search query result for each matched document
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FieldRetrivalResult {
    /// The fast fields returned for each hit.
    #[serde(rename = "docvalue_fields")]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub doc_value_fields: HashMap<String, OwnedValue>,
}

impl RetrievalFields {
    fn get_field_names(&self) -> Vec<&str> {
        self.doc_value_fields.iter().map(|s| s.as_str()).collect()
    }

    fn resolve_field_names(self, reader: &ColumnarReader) -> crate::Result<Self> {
        let globbed_string_to_regex = |globbed: &str| {
            let sanitized = format!("^{}$", regex::escape(globbed).replace(r"\*", ".*"));
            Regex::new(&sanitized.replace('*', ".*")).map_err(|e| {
                crate::TantivyError::SchemaError(format!(
                    "Invalid regex '{}' in docvalue_fields: {}",
                    globbed, e
                ))
            })
        };
        let resolved_fields = self
            .doc_value_fields
            .into_iter()
            .map(|field| {
                if !field.contains('*') && reader.iter_columns()?.any(|(name, _)| name == field) {
                    return Ok(vec![field]);
                }

                let pattern = globbed_string_to_regex(&field)?;
                Ok(reader
                    .iter_columns()?
                    .filter(|(name, _)| pattern.is_match(name))
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>())
            })
            .collect::<crate::Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(Self {
            doc_value_fields: resolved_fields,
        })
    }

    fn get_fields(
        &self,
        accessors: &HashMap<String, DynamicColumn>,
        doc_id: DocId,
    ) -> FieldRetrivalResult {
        let dvf = self
            .doc_value_fields
            .iter()
            .map(|field| {
                let accessor = accessors.get(field).expect("field accessor must exist");

                let value: OwnedValue = match accessor {
                    DynamicColumn::U64(accessor) => accessor
                        .values_for_doc(doc_id)
                        .next()
                        .map_or(OwnedValue::Null, OwnedValue::U64),
                    DynamicColumn::I64(accessor) => accessor
                        .values_for_doc(doc_id)
                        .next()
                        .map_or(OwnedValue::Null, OwnedValue::I64),
                    DynamicColumn::F64(accessor) => accessor
                        .values_for_doc(doc_id)
                        .next()
                        .map_or(OwnedValue::Null, OwnedValue::F64),
                    DynamicColumn::Bytes(accessor) => 'l: {
                        let Some(term_ord) = accessor.term_ords(doc_id).next() else {
                            break 'l OwnedValue::Null;
                        };
                        let mut buffer = vec![];
                        match accessor
                            .ord_to_bytes(term_ord, &mut buffer)
                            .expect("must succeed")
                        {
                            false => OwnedValue::Null,
                            true => OwnedValue::Bytes(buffer),
                        }
                    }
                    DynamicColumn::Str(accessor) => 'l: {
                        let Some(term_ord) = accessor.term_ords(doc_id).next() else {
                            break 'l OwnedValue::Null;
                        };
                        let mut buffer = vec![];
                        match accessor
                            .ord_to_bytes(term_ord, &mut buffer)
                            .expect("must succeed")
                        {
                            false => OwnedValue::Null,
                            true => OwnedValue::Str(
                                String::from_utf8(buffer).expect("string must be valid utf8"),
                            ),
                        }
                    }
                    DynamicColumn::Bool(accessor) => accessor
                        .values_for_doc(doc_id)
                        .next()
                        .map_or(OwnedValue::Null, OwnedValue::Bool),
                    DynamicColumn::IpAddr(accessor) => accessor
                        .values_for_doc(doc_id)
                        .next()
                        .map_or(OwnedValue::Null, OwnedValue::IpAddr),
                    DynamicColumn::DateTime(accessor) => accessor
                        .values_for_doc(doc_id)
                        .next()
                        .map_or(OwnedValue::Null, OwnedValue::Date),
                };
                (field.to_owned(), value)
            })
            .collect();
        FieldRetrivalResult {
            doc_value_fields: dvf,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
struct KeyOrder {
    field: String,
    order: Order,
}

impl Serialize for KeyOrder {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let KeyOrder { field, order } = self;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(field, order)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for KeyOrder {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let mut k_o = <HashMap<String, Order>>::deserialize(deserializer)?.into_iter();
        let (k, v) = k_o.next().ok_or(serde::de::Error::custom(
            "Expected exactly one key-value pair in KeyOrder, found none",
        ))?;
        if k_o.next().is_some() {
            return Err(serde::de::Error::custom(
                "Expected exactly one key-value pair in KeyOrder, found more",
            ));
        }
        Ok(Self { field: k, order: v })
    }
}

impl TopHitsAggregation {
    /// Validate and resolve field retrieval parameters
    pub fn validate_and_resolve(&self, reader: &ColumnarReader) -> crate::Result<Self> {
        let cf = self.clone(); // :(
        let retrieval = cf.retrieval.resolve_field_names(reader)?;
        Ok(Self { retrieval, ..cf })
    }

    /// Return fields accessed by the aggregator, in order.
    pub fn field_names(&self) -> Vec<&str> {
        self.sort
            .iter()
            .map(|KeyOrder { field, .. }| field.as_str())
            .collect()
    }

    /// Return fields accessed by the aggregator's value retrieval.
    pub fn value_field_names(&self) -> Vec<&str> {
        self.retrieval.get_field_names()
    }
}

/// Holds a single comparable doc feature, and the order in which it should be sorted.
#[derive(Clone, Serialize, Deserialize, Debug)]
struct ComparableDocFeature {
    /// Stores any u64-mappable feature.
    value: Option<u64>,
    /// Sort order for the doc feature
    order: Order,
}

impl Ord for ComparableDocFeature {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let invert = |cmp: std::cmp::Ordering| match self.order {
            Order::Asc => cmp,
            Order::Desc => cmp.reverse(),
        };

        match (self.value, other.value) {
            (Some(self_value), Some(other_value)) => invert(self_value.cmp(&other_value)),
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
        }
    }
}

impl PartialOrd for ComparableDocFeature {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ComparableDocFeature {
    fn eq(&self, other: &Self) -> bool {
        self.value.cmp(&other.value) == std::cmp::Ordering::Equal
    }
}

impl Eq for ComparableDocFeature {}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct ComparableDocFeatures(Vec<ComparableDocFeature>, FieldRetrivalResult);

impl Ord for ComparableDocFeatures {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for (self_feature, other_feature) in self.0.iter().zip(other.0.iter()) {
            let cmp = self_feature.cmp(other_feature);
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    }
}

impl PartialOrd for ComparableDocFeatures {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ComparableDocFeatures {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for ComparableDocFeatures {}

/// The TopHitsCollector used for collecting over segments and merging results.
#[derive(Clone, Serialize, Deserialize)]
pub struct TopHitsCollector {
    req: TopHitsAggregation,
    top_n: TopNComputer<ComparableDocFeatures, DocAddress, false>,
}

impl Default for TopHitsCollector {
    fn default() -> Self {
        Self {
            req: TopHitsAggregation::default(),
            top_n: TopNComputer::new(1),
        }
    }
}

impl std::fmt::Debug for TopHitsCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopHitsCollector")
            .field("req", &self.req)
            .field("top_n_threshold", &self.top_n.threshold)
            .finish()
    }
}

impl std::cmp::PartialEq for TopHitsCollector {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl TopHitsCollector {
    fn collect(&mut self, features: ComparableDocFeatures, doc: DocAddress) {
        self.top_n.push(features, doc);
    }

    pub(crate) fn merge_fruits(&mut self, other_fruit: Self) -> crate::Result<()> {
        for doc in other_fruit.top_n.into_vec() {
            self.collect(doc.feature, doc.doc);
        }
        Ok(())
    }

    /// Finalize by converting self into the final result form
    pub fn finalize(self) -> TopHitsMetricResult {
        let mut hits: Vec<TopHitsVecEntry> = self
            .top_n
            .into_sorted_vec()
            .into_iter()
            .map(|doc| TopHitsVecEntry {
                id: doc.doc,
                sort: doc.feature.0.iter().map(|f| f.value).collect(),
                search_results: doc.feature.1,
            })
            .collect();

        // Remove the first `from` elements
        // Truncating from end would be more efficient, but we need to truncate from the front
        // because `into_sorted_vec` gives us a descending order because of the inverted
        // `Ord` semantics of the heap elements.
        hits.drain(..self.req.from.unwrap_or(0));
        TopHitsMetricResult { hits }
    }
}

#[derive(Clone)]
pub(crate) struct SegmentTopHitsCollector {
    segment_id: SegmentOrdinal,
    accessor_idx: usize,
    inner_collector: TopHitsCollector,
}

impl SegmentTopHitsCollector {
    pub fn from_req(
        req: &TopHitsAggregation,
        accessor_idx: usize,
        segment_id: SegmentOrdinal,
    ) -> Self {
        Self {
            inner_collector: TopHitsCollector {
                req: req.clone(),
                top_n: TopNComputer::new(req.size + req.from.unwrap_or(0)),
            },
            segment_id,
            accessor_idx,
        }
    }
}

impl std::fmt::Debug for SegmentTopHitsCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentTopHitsCollector")
            .field("segment_id", &self.segment_id)
            .field("accessor_idx", &self.accessor_idx)
            .field("inner_collector", &self.inner_collector)
            .finish()
    }
}

impl SegmentAggregationCollector for SegmentTopHitsCollector {
    fn add_intermediate_aggregation_result(
        self: Box<Self>,
        agg_with_accessor: &crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor,
        results: &mut crate::aggregation::intermediate_agg_result::IntermediateAggregationResults,
    ) -> crate::Result<()> {
        let name = agg_with_accessor.aggs.keys[self.accessor_idx].to_string();
        let intermediate_result = IntermediateMetricResult::TopHits(self.inner_collector);
        results.push(
            name,
            IntermediateAggregationResult::Metric(intermediate_result),
        )
    }

    fn collect(
        &mut self,
        doc_id: crate::DocId,
        agg_with_accessor: &mut crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor,
    ) -> crate::Result<()> {
        let accessors = &agg_with_accessor.aggs.values[self.accessor_idx].accessors;
        let value_accessors = &agg_with_accessor.aggs.values[self.accessor_idx].value_accessors;
        let features: Vec<ComparableDocFeature> = self
            .inner_collector
            .req
            .sort
            .iter()
            .enumerate()
            .map(|(idx, KeyOrder { order, .. })| {
                let order = *order;
                let value = accessors
                    .get(idx)
                    .expect("field accessor must exist")
                    .0
                    .values_for_doc(doc_id)
                    .next();
                ComparableDocFeature { value, order }
            })
            .collect();

        let retrieval_result = self
            .inner_collector
            .req
            .retrieval
            .get_fields(value_accessors, doc_id);

        self.inner_collector.collect(
            ComparableDocFeatures(features, retrieval_result),
            DocAddress {
                segment_ord: self.segment_id,
                doc_id,
            },
        );
        Ok(())
    }

    fn collect_block(
        &mut self,
        docs: &[crate::DocId],
        agg_with_accessor: &mut crate::aggregation::agg_req_with_accessor::AggregationsWithAccessor,
    ) -> crate::Result<()> {
        // TODO: Consider getting fields with the column block accessor and refactor this.
        // ---
        // Would the additional complexity of getting fields with the column_block_accessor
        // make sense here? Probably yes, but I want to get a first-pass review first
        // before proceeding.
        for doc in docs {
            self.collect(*doc, agg_with_accessor)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common::DateTime;
    use pretty_assertions::assert_eq;
    use serde_json::Value;
    use time::macros::datetime;

    use super::{ComparableDocFeature, ComparableDocFeatures, Order};
    use crate::aggregation::agg_req::Aggregations;
    use crate::aggregation::agg_result::{AggregationResult, AggregationResults, MetricResult};
    use crate::aggregation::bucket::tests::get_test_index_from_docs;
    use crate::aggregation::metric::{FieldRetrivalResult, TopHitsMetricResult, TopHitsVecEntry};
    use crate::aggregation::tests::get_test_index_from_values;
    use crate::aggregation::AggregationCollector;
    use crate::collector::ComparableDoc;
    use crate::query::AllQuery;
    use crate::schema::OwnedValue as SchemaValue;

    fn invert_order(cmp_feature: ComparableDocFeature) -> ComparableDocFeature {
        let ComparableDocFeature { value, order } = cmp_feature;
        let order = match order {
            Order::Asc => Order::Desc,
            Order::Desc => Order::Asc,
        };
        ComparableDocFeature { value, order }
    }

    fn collector_with_capacity(capacity: usize) -> super::TopHitsCollector {
        super::TopHitsCollector {
            top_n: super::TopNComputer::new(capacity),
            ..Default::default()
        }
    }

    fn invert_order_features(cmp_features: ComparableDocFeatures) -> ComparableDocFeatures {
        let ComparableDocFeatures(cmp_features, search_results) = cmp_features;
        let cmp_features = cmp_features
            .into_iter()
            .map(invert_order)
            .collect::<Vec<_>>();
        ComparableDocFeatures(cmp_features, search_results)
    }

    #[test]
    fn test_comparable_doc_feature() -> crate::Result<()> {
        let small = ComparableDocFeature {
            value: Some(1),
            order: Order::Asc,
        };
        let big = ComparableDocFeature {
            value: Some(2),
            order: Order::Asc,
        };
        let none = ComparableDocFeature {
            value: None,
            order: Order::Asc,
        };

        assert!(small < big);
        assert!(none < small);
        assert!(none < big);

        let small = invert_order(small);
        let big = invert_order(big);
        let none = invert_order(none);

        assert!(small > big);
        assert!(none < small);
        assert!(none < big);

        Ok(())
    }

    #[test]
    fn test_comparable_doc_features() -> crate::Result<()> {
        let features_1 = ComparableDocFeatures(
            vec![ComparableDocFeature {
                value: Some(1),
                order: Order::Asc,
            }],
            Default::default(),
        );

        let features_2 = ComparableDocFeatures(
            vec![ComparableDocFeature {
                value: Some(2),
                order: Order::Asc,
            }],
            Default::default(),
        );

        assert!(features_1 < features_2);

        assert!(invert_order_features(features_1.clone()) > invert_order_features(features_2));

        Ok(())
    }

    #[test]
    fn test_aggregation_top_hits_empty_index() -> crate::Result<()> {
        let values = vec![];

        let index = get_test_index_from_values(false, &values)?;

        let d: Aggregations = serde_json::from_value(json!({
            "top_hits_req": {
                "top_hits": {
                    "size": 2,
                    "sort": [
                        { "date": "desc" }
                    ],
                    "from": 0,
                }
        }
        }))
        .unwrap();

        let collector = AggregationCollector::from_aggs(d, Default::default());

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let agg_res: AggregationResults = searcher.search(&AllQuery, &collector).unwrap();

        let res: Value = serde_json::from_str(
            &serde_json::to_string(&agg_res).expect("JSON serialization failed"),
        )
        .expect("JSON parsing failed");

        assert_eq!(
            res,
            json!({
                "top_hits_req": {
                    "hits": []
                }
            })
        );

        Ok(())
    }

    #[test]
    fn test_top_hits_collector_single_feature() -> crate::Result<()> {
        let docs = vec![
            ComparableDoc::<_, _, false> {
                doc: crate::DocAddress {
                    segment_ord: 0,
                    doc_id: 0,
                },
                feature: ComparableDocFeatures(
                    vec![ComparableDocFeature {
                        value: Some(1),
                        order: Order::Asc,
                    }],
                    Default::default(),
                ),
            },
            ComparableDoc {
                doc: crate::DocAddress {
                    segment_ord: 0,
                    doc_id: 2,
                },
                feature: ComparableDocFeatures(
                    vec![ComparableDocFeature {
                        value: Some(3),
                        order: Order::Asc,
                    }],
                    Default::default(),
                ),
            },
            ComparableDoc {
                doc: crate::DocAddress {
                    segment_ord: 0,
                    doc_id: 1,
                },
                feature: ComparableDocFeatures(
                    vec![ComparableDocFeature {
                        value: Some(5),
                        order: Order::Asc,
                    }],
                    Default::default(),
                ),
            },
        ];

        let mut collector = collector_with_capacity(3);
        for doc in docs.clone() {
            collector.collect(doc.feature, doc.doc);
        }

        let res = collector.finalize();

        assert_eq!(
            res,
            super::TopHitsMetricResult {
                hits: vec![
                    super::TopHitsVecEntry {
                        id: docs[0].doc,
                        sort: vec![docs[0].feature.0[0].value],
                        search_results: Default::default(),
                    },
                    super::TopHitsVecEntry {
                        id: docs[1].doc,
                        sort: vec![docs[1].feature.0[0].value],
                        search_results: Default::default(),
                    },
                    super::TopHitsVecEntry {
                        id: docs[2].doc,
                        sort: vec![docs[2].feature.0[0].value],
                        search_results: Default::default(),
                    },
                ]
            }
        );

        Ok(())
    }

    fn test_aggregation_top_hits(merge_segments: bool) -> crate::Result<()> {
        let docs = vec![
            vec![
                r#"{ "date": "2015-01-02T00:00:00Z", "text": "bbb", "text2": "bbb" }"#,
                r#"{ "date": "2017-06-15T00:00:00Z", "text": "ccc", "text2": "ddd" }"#,
            ],
            vec![
                r#"{ "text": "aaa", "text2": "bbb", "date": "2018-01-02T00:00:00Z" }"#,
                r#"{ "text": "aaa", "text2": "bbb", "date": "2016-01-02T00:00:00Z" }"#,
            ],
        ];

        let index = get_test_index_from_docs(merge_segments, &docs)?;

        let d: Aggregations = serde_json::from_value(json!({
            "top_hits_req": {
                "top_hits": {
                    "size": 2,
                    "sort": [
                        { "date": "desc" }
                    ],
                    "from": 1,
                    "docvalue_fields": [
                        "date",
                        "tex*",
                    ],
                }
        }
        }))?;

        let collector = AggregationCollector::from_aggs(d, Default::default());
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let agg_res =
            serde_json::to_value(searcher.search(&AllQuery, &collector).unwrap()).unwrap();

        let date_2017 = datetime!(2017-06-15 00:00:00 UTC);
        let date_2016 = datetime!(2016-01-02 00:00:00 UTC);

        assert_eq!(
            agg_res["top_hits_req"],
            json!({
                "hits": [
                    {
                        "id": {
                            "segment_ord": 0,
                            "doc_id": 1,
                        },
                        "sort": [common::i64_to_u64(date_2017.unix_timestamp_nanos() as i64)],
                        "docvalue_fields": {
                            "date": SchemaValue::Date(DateTime::from_utc(date_2017)),
                            "text": "ccc",
                            "text2": "ddd",
                        }
                    },
                    {
                        "id": {
                            "segment_ord": if merge_segments { 0 } else { 1 },
                            "doc_id": if merge_segments { 2 + 1 } else { 1 },
                        },
                        "sort": [common::i64_to_u64(date_2016.unix_timestamp_nanos() as i64)],
                        "docvalue_fields": {
                            "date": SchemaValue::Date(DateTime::from_utc(date_2016)),
                            "text": "aaa",
                            "text2": "bbb",
                        }
                    }
                ]
            }),
        );

        Ok(())
    }

    #[test]
    fn test_aggregation_top_hits_single_segment() -> crate::Result<()> {
        test_aggregation_top_hits(true)
    }

    #[test]
    fn test_aggregation_top_hits_multi_segment() -> crate::Result<()> {
        test_aggregation_top_hits(false)
    }
}
