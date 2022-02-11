//! Contains Intermediate aggregation trees, that can be merged.
//! Intermediate aggregation results can be used to merge results between segments or between
//! indices.

use std::collections::HashMap;

use super::metric::AverageData;
use super::segment_agg_result::{
    SegmentAggregationResultCollector, SegmentAggregationResultsCollector, SegmentBucketDataEntry,
    SegmentBucketDataEntryKeyCount, SegmentBucketResultCollector, SegmentMetricResultCollector,
};
use super::{Key, VecWithNames};

#[derive(Default, Debug, Clone, PartialEq)]
/// Contains the intermediate aggregation result, which is optimized to be merged with other
/// intermediate results.
pub struct IntermediateAggregationResults(pub(crate) VecWithNames<IntermediateAggregationResult>);

impl From<SegmentAggregationResultsCollector> for IntermediateAggregationResults {
    fn from(tree: SegmentAggregationResultsCollector) -> Self {
        Self(VecWithNames::from_entries(
            tree.collectors
                .into_iter()
                .map(|(key, agg)| (key, agg.into()))
                .collect(),
        ))
    }
}

impl IntermediateAggregationResults {
    /// Merge an other intermediate aggregation result into this result.
    pub fn merge_fruits(&mut self, other: &IntermediateAggregationResults) {
        for (tree_left, tree_right) in self.0.values_mut().zip(other.0.values()) {
            tree_left.merge_fruits(tree_right);
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// An aggregation is either a bucket or a metric.
pub enum IntermediateAggregationResult {
    /// Bucket variant
    Bucket(IntermediateBucketResult),
    /// Metric variant
    Metric(IntermediateMetricResult),
}

impl From<SegmentAggregationResultCollector> for IntermediateAggregationResult {
    fn from(tree: SegmentAggregationResultCollector) -> Self {
        match tree {
            SegmentAggregationResultCollector::Bucket(bucket) => {
                IntermediateAggregationResult::Bucket(bucket.into())
            }
            SegmentAggregationResultCollector::Metric(metric) => {
                IntermediateAggregationResult::Metric(metric.into())
            }
        }
    }
}

impl IntermediateAggregationResult {
    fn merge_fruits(&mut self, other: &IntermediateAggregationResult) {
        match (self, other) {
            (
                IntermediateAggregationResult::Bucket(res_left),
                IntermediateAggregationResult::Bucket(res_right),
            ) => {
                res_left.merge_fruits(res_right);
            }
            (
                IntermediateAggregationResult::Metric(res_left),
                IntermediateAggregationResult::Metric(res_right),
            ) => {
                res_left.merge_fruits(res_right);
            }
            _ => {
                panic!("incompatible types in aggregation tree on merge fruits");
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// Holds the intermediate data for metric resuls
pub enum IntermediateMetricResult {
    /// AverageData variant
    Average(AverageData),
}

impl From<SegmentMetricResultCollector> for IntermediateMetricResult {
    fn from(tree: SegmentMetricResultCollector) -> Self {
        match tree {
            SegmentMetricResultCollector::Average(collector) => {
                IntermediateMetricResult::Average(AverageData::from_collector(collector))
            }
        }
    }
}

impl IntermediateMetricResult {
    fn merge_fruits(&mut self, other: &IntermediateMetricResult) {
        match (self, other) {
            (
                IntermediateMetricResult::Average(avg_data_left),
                IntermediateMetricResult::Average(avg_data_right),
            ) => {
                avg_data_left.merge_fruits(avg_data_right);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// The intermediate bucket results. Internally they can be easily merged via the keys of the
/// buckets.
pub struct IntermediateBucketResult {
    pub(crate) buckets: HashMap<Key, IntermediateBucketDataEntry>,
}

impl From<SegmentBucketResultCollector> for IntermediateBucketResult {
    fn from(collector: SegmentBucketResultCollector) -> Self {
        match collector {
            SegmentBucketResultCollector::Range(range) => range.into_intermediate_agg_result(),
        }
    }
}

impl IntermediateBucketResult {
    fn merge_fruits(&mut self, other: &IntermediateBucketResult) {
        for (name, entry_left) in self.buckets.iter_mut() {
            if let Some(entry_right) = other.buckets.get(name) {
                match (entry_left, entry_right) {
                    (
                        IntermediateBucketDataEntry::KeyCount(key_count_left),
                        IntermediateBucketDataEntry::KeyCount(key_count_right),
                    ) => key_count_left.merge_fruits(key_count_right),
                }
            }
        }

        for (key, res) in other.buckets.iter() {
            if !self.buckets.contains_key(key) {
                self.buckets.insert(key.clone(), res.clone());
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// The intermediate buckets
pub enum IntermediateBucketDataEntry {
    /// This is the default entry for a bucket, which contains a key, count, and optionally
    /// sub_aggregations.
    KeyCount(IntermediateBucketDataEntryKeyCount),
}

impl From<SegmentBucketDataEntry> for IntermediateBucketDataEntry {
    fn from(entry: SegmentBucketDataEntry) -> Self {
        match entry {
            SegmentBucketDataEntry::KeyCount(key_count) => {
                IntermediateBucketDataEntry::KeyCount(key_count.into())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// This is the default entry for a bucket, which contains a key, count, and optionally
/// sub_aggregations.
pub struct IntermediateBucketDataEntryKeyCount {
    /// The unique the bucket is identified.
    pub key: Key,
    /// The number of documents in the bucket.
    pub doc_count: u64,
    pub(crate) values: Option<Vec<u64>>,
    /// The sub_aggregation in this bucket.
    pub sub_aggregation: IntermediateAggregationResults,
}

impl From<SegmentBucketDataEntryKeyCount> for IntermediateBucketDataEntryKeyCount {
    fn from(entry: SegmentBucketDataEntryKeyCount) -> Self {
        let sub_aggregation = entry.sub_aggregation.into();

        IntermediateBucketDataEntryKeyCount {
            key: entry.key,
            doc_count: entry.doc_count,
            values: entry.values,
            sub_aggregation,
        }
    }
}

impl IntermediateBucketDataEntryKeyCount {
    fn merge_fruits(&mut self, other: &IntermediateBucketDataEntryKeyCount) {
        self.doc_count += other.doc_count;
        self.sub_aggregation.merge_fruits(&other.sub_aggregation);
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn get_sub_test_tree(data: &[(String, u64)]) -> IntermediateAggregationResults {
        let mut map = HashMap::new();
        let mut buckets = HashMap::new();
        for (key, doc_count) in data {
            buckets.insert(
                Key::Str(key.to_string()),
                IntermediateBucketDataEntry::KeyCount(IntermediateBucketDataEntryKeyCount {
                    key: Key::Str(key.to_string()),
                    doc_count: *doc_count,
                    values: None,
                    sub_aggregation: Default::default(),
                }),
            );
        }
        map.insert(
            "my_agg_level2".to_string(),
            IntermediateAggregationResult::Bucket(IntermediateBucketResult { buckets }),
        );
        IntermediateAggregationResults(VecWithNames::from_entries(map.into_iter().collect()))
    }

    fn get_test_tree(data: &[(String, u64, String, u64)]) -> IntermediateAggregationResults {
        let mut map = HashMap::new();
        let mut buckets = HashMap::new();
        for (key, doc_count, sub_aggregation_key, sub_aggregation_count) in data {
            buckets.insert(
                Key::Str(key.to_string()),
                IntermediateBucketDataEntry::KeyCount(IntermediateBucketDataEntryKeyCount {
                    key: Key::Str(key.to_string()),
                    doc_count: *doc_count,
                    values: None,
                    sub_aggregation: get_sub_test_tree(&[(
                        sub_aggregation_key.to_string(),
                        *sub_aggregation_count,
                    )]),
                }),
            );
        }
        map.insert(
            "my_agg_level1".to_string(),
            IntermediateAggregationResult::Bucket(IntermediateBucketResult { buckets }),
        );
        IntermediateAggregationResults(VecWithNames::from_entries(map.into_iter().collect()))
    }

    #[test]
    fn test_merge_fruits_tree_1() {
        let mut tree_left = get_test_tree(&[
            ("red".to_string(), 50, "1900".to_string(), 25),
            ("blue".to_string(), 30, "1900".to_string(), 30),
        ]);
        let tree_right = get_test_tree(&[
            ("red".to_string(), 60, "1900".to_string(), 30),
            ("blue".to_string(), 25, "1900".to_string(), 50),
        ]);

        tree_left.merge_fruits(&tree_right);

        let tree_expected = get_test_tree(&[
            ("red".to_string(), 110, "1900".to_string(), 55),
            ("blue".to_string(), 55, "1900".to_string(), 80),
        ]);

        assert_eq!(tree_left, tree_expected);
    }

    #[test]
    fn test_merge_fruits_tree_2() {
        let mut tree_left = get_test_tree(&[
            ("red".to_string(), 50, "1900".to_string(), 25),
            ("blue".to_string(), 30, "1900".to_string(), 30),
        ]);
        let tree_right = get_test_tree(&[
            ("red".to_string(), 60, "1900".to_string(), 30),
            ("green".to_string(), 25, "1900".to_string(), 50),
        ]);

        tree_left.merge_fruits(&tree_right);

        let tree_expected = get_test_tree(&[
            ("red".to_string(), 110, "1900".to_string(), 55),
            ("blue".to_string(), 30, "1900".to_string(), 30),
            ("green".to_string(), 25, "1900".to_string(), 50),
        ]);

        assert_eq!(tree_left, tree_expected);
    }
}
