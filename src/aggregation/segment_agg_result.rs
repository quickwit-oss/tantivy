use super::metric::AverageData;
use crate::{collector::MergeableFruit, DocId};
use std::collections::HashMap;

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct AggregationResultTree(HashMap<String, AggregationResult>);

impl MergeableFruit for AggregationResultTree {
    fn merge_fruit(&mut self, other: &Self) {
        self.merge_fruits(other);
    }
}

impl AggregationResultTree {
    fn merge_fruits(&mut self, other: &AggregationResultTree) {
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
pub enum AggregationResult {
    BucketResult(BucketAggregationResult),
    MetricResult(MetricResult),
}

impl AggregationResult {
    fn merge_fruits(&mut self, other: &AggregationResult) {
        match (self, other) {
            (
                AggregationResult::BucketResult(res_left),
                AggregationResult::BucketResult(res_right),
            ) => {
                res_left.merge_fruits(res_right);
            }
            (
                AggregationResult::MetricResult(res_left),
                AggregationResult::MetricResult(res_right),
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
pub enum MetricResult {
    Average(AverageData),
}

impl MetricResult {
    fn merge_fruits(&mut self, other: &MetricResult) {
        match (self, other) {
            (MetricResult::Average(avg_data_left), MetricResult::Average(avg_data_right)) => {
                avg_data_left.merge_fruits(avg_data_right);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BucketAggregationResult {
    buckets: HashMap<Key, BucketDataEntry>,
}

impl BucketAggregationResult {
    fn merge_fruits(&mut self, other: &BucketAggregationResult) {
        for (name, entry_left) in self.buckets.iter_mut() {
            if let Some(entry_right) = other.buckets.get(name) {
                match (entry_left, entry_right) {
                    (
                        BucketDataEntry::KeyCount(key_count_left),
                        BucketDataEntry::KeyCount(key_count_right),
                    ) => key_count_left.merge_fruits(&key_count_right),
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

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum Key {
    Str(String),
    U64(u64),
    I64(i64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BucketDataEntry {
    KeyCount(BucketDataEntryKeyCount),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BucketDataEntryKeyCount {
    key: Key,
    doc_count: u64,
    docs: Option<Vec<DocId>>,
    sub_aggregation: Option<Box<AggregationResultTree>>,
}

impl BucketDataEntryKeyCount {
    fn merge_fruits(&mut self, other: &BucketDataEntryKeyCount) {
        self.doc_count += other.doc_count;
        if let (Some(sub_aggregation_left), Some(sub_aggregation_right)) = (
            self.sub_aggregation.as_mut(),
            other.sub_aggregation.as_ref(),
        ) {
            sub_aggregation_left.merge_fruits(&sub_aggregation_right);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::{assert_eq, assert_ne};

    fn get_sub_test_tree(data: &[(String, u64)]) -> AggregationResultTree {
        let mut map = HashMap::new();
        let mut buckets = HashMap::new();
        for (key, doc_count) in data {
            buckets.insert(
                Key::Str(key.to_string()),
                BucketDataEntry::KeyCount(BucketDataEntryKeyCount {
                    key: Key::Str(key.to_string()),
                    doc_count: *doc_count,
                    docs: None,
                    sub_aggregation: None,
                }),
            );
        }
        map.insert(
            "my_agg_level2".to_string(),
            AggregationResult::BucketResult(BucketAggregationResult { buckets }),
        );
        AggregationResultTree(map)
    }

    fn get_test_tree(data: &[(String, u64, String, u64)]) -> AggregationResultTree {
        let mut map = HashMap::new();
        let mut buckets = HashMap::new();
        for (key, doc_count, sub_aggregation_key, sub_aggregation_count) in data {
            buckets.insert(
                Key::Str(key.to_string()),
                BucketDataEntry::KeyCount(BucketDataEntryKeyCount {
                    key: Key::Str(key.to_string()),
                    doc_count: *doc_count,
                    docs: None,
                    sub_aggregation: Some(Box::new(get_sub_test_tree(&[(
                        sub_aggregation_key.to_string(),
                        *sub_aggregation_count,
                    )]))),
                }),
            );
        }
        map.insert(
            "my_agg_level1".to_string(),
            AggregationResult::BucketResult(BucketAggregationResult { buckets }),
        );
        AggregationResultTree(map)
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
