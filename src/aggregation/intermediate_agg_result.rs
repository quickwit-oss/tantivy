//! Contains Intermediate aggregation trees, that can be merged.
//! This tree will be used to merge results between segments and between indices.

use super::{
    metric::AverageData,
    segment_agg_result::{
        SegmentAggregationResultCollector, SegmentAggregationResults, SegmentBucketDataEntry,
        SegmentBucketDataEntryKeyCount, SegmentBucketResultCollector, SegmentMetricResultCollector,
    },
    Key,
};
use crate::collector::MergeableFruit;
use std::collections::HashMap;

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct IntermediateAggregationResults(pub HashMap<String, IntermediateAggregationResult>);

impl From<SegmentAggregationResults> for IntermediateAggregationResults {
    fn from(tree: SegmentAggregationResults) -> Self {
        Self(
            tree.0
                .into_iter()
                .map(|(key, agg)| (key, agg.into()))
                .collect(),
        )
    }
}

impl MergeableFruit for IntermediateAggregationResults {
    fn merge_fruit(&mut self, other: &Self) {
        self.merge_fruits(other);
    }
}

impl IntermediateAggregationResults {
    pub fn merge_fruits(&mut self, other: &IntermediateAggregationResults) {
        for (name, tree_left) in self.0.iter_mut() {
            if let Some(tree_right) = other.0.get(name) {
                tree_left.merge_fruits(tree_right);
            }
        }

        for (key, res) in other.0.iter() {
            if !other.0.contains_key(key) {
                self.0.insert(key.to_string(), res.clone());
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IntermediateAggregationResult {
    Bucket(IntermediateBucketAggregationResult),
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IntermediateMetricResult {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntermediateBucketAggregationResult {
    pub buckets: HashMap<Key, IntermediateBucketDataEntry>,
}

impl From<SegmentBucketResultCollector> for IntermediateBucketAggregationResult {
    fn from(collector: SegmentBucketResultCollector) -> Self {
        match collector {
            SegmentBucketResultCollector::Range(range) => range.into_bucket_agg_result(),
        }
    }
}

impl IntermediateBucketAggregationResult {
    fn merge_fruits(&mut self, other: &IntermediateBucketAggregationResult) {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IntermediateBucketDataEntry {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntermediateBucketDataEntryKeyCount {
    key: Key,
    doc_count: u64,
    values: Option<Vec<u64>>,
    sub_aggregation: IntermediateAggregationResults,
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
    use super::*;
    use pretty_assertions::assert_eq;

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
            IntermediateAggregationResult::Bucket(IntermediateBucketAggregationResult { buckets }),
        );
        IntermediateAggregationResults(map)
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
            IntermediateAggregationResult::Bucket(IntermediateBucketAggregationResult { buckets }),
        );
        IntermediateAggregationResults(map)
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
