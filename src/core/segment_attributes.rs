use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::Serialize;

/// SegmentAttributes implementation owns custom segment attributes and its merging behavior
pub trait SegmentAttributes: Default + Serialize + DeserializeOwned + Send + Sync + Clone {
    /// Must be implemented for defining how to merge `SegmentAttributes` from
    /// different segments
    fn merge(segments_attributes: Vec<Self>) -> Self;
}

pub trait SegmentAttributesMerger: Send + Sync {
    fn merge_json(&self, segment_attributes_json: Vec<&serde_json::Value>) -> serde_json::Value;
    fn default(&self) -> serde_json::Value;
    fn box_clone(&self) -> Box<dyn SegmentAttributesMerger>;
}

#[derive(Clone)]
pub struct SegmentAttributesMergerImpl<S: SegmentAttributes> {
    _phantom: PhantomData<S>,
}

impl<S: SegmentAttributes> SegmentAttributesMergerImpl<S> {
    pub fn new() -> SegmentAttributesMergerImpl<S> {
        SegmentAttributesMergerImpl {
            _phantom: PhantomData,
        }
    }
}

impl<S: SegmentAttributes + 'static> SegmentAttributesMerger for SegmentAttributesMergerImpl<S> {
    fn merge_json(&self, segment_attributes_json: Vec<&serde_json::Value>) -> serde_json::Value {
        let segment_attributes: Vec<_> = segment_attributes_json
            .into_iter()
            .flat_map(|v| serde_json::from_value(v.clone()))
            .collect();
        serde_json::to_value(S::merge(segment_attributes)).unwrap()
    }

    fn default(&self) -> serde_json::Value {
        serde_json::to_value(S::default()).unwrap()
    }

    fn box_clone(&self) -> Box<dyn SegmentAttributesMerger> {
        Box::new((*self).clone())
    }
}

impl Clone for Box<dyn SegmentAttributesMerger> {
    fn clone(&self) -> Box<dyn SegmentAttributesMerger> {
        self.box_clone()
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Clone, Default, Serialize, Deserialize)]
    struct TestSegmentAttributes {
        is_frozen: bool,
    }

    impl TestSegmentAttributes {
        pub fn new(is_frozen: bool) -> TestSegmentAttributes {
            TestSegmentAttributes { is_frozen }
        }
    }

    impl SegmentAttributes for TestSegmentAttributes {
        fn merge(segments_attributes: Vec<Self>) -> Self {
            TestSegmentAttributes {
                is_frozen: segments_attributes
                    .iter()
                    .map(|v| v.is_frozen)
                    .reduce(|a, b| a && b)
                    .unwrap(),
            }
        }
    }

    #[test]
    fn test_merge() {
        let merged = TestSegmentAttributes::merge(vec![
            TestSegmentAttributes::default(),
            TestSegmentAttributes::new(true),
        ]);
        assert!(!merged.is_frozen)
    }
}
