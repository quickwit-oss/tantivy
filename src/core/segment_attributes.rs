use std::collections::HashMap;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

/// SegmentAttribute is a single attribute related to a segment
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SegmentAttribute {
    ConjunctiveBool(bool),
    DisjunctiveBool(bool),
    StringList(Vec<String>),
}

impl SegmentAttribute {
    pub fn merge(&mut self, other: &SegmentAttribute) {
        match (self, other) {
            (SegmentAttribute::ConjunctiveBool(a), SegmentAttribute::ConjunctiveBool(b)) => {
                *a = *a && *b
            }
            (SegmentAttribute::DisjunctiveBool(a), SegmentAttribute::DisjunctiveBool(b)) => {
                *a = *a || *b
            }
            (SegmentAttribute::StringList(a), SegmentAttribute::StringList(b)) => {
                a.extend(b.iter().cloned())
            }
            (a, b) => warn!(
                "SegmentAttribute {other:?} cannot be merged to {self:?}, defaulted to {self:?}",
                other = b,
                self = a
            ),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SegmentAttributesConfig(HashMap<String, SegmentAttribute>);

impl SegmentAttributesConfig {
    pub fn new(segment_attributes: HashMap<String, SegmentAttribute>) -> SegmentAttributesConfig {
        SegmentAttributesConfig(segment_attributes)
    }
    pub fn insert(
        &mut self,
        name: &str,
        segment_attribute: SegmentAttribute,
    ) -> Option<SegmentAttribute> {
        self.0.insert(name.to_string(), segment_attribute)
    }
    pub fn segment_attributes(&self) -> SegmentAttributes {
        SegmentAttributes::new(self.0.clone())
    }
    pub fn get(&self, name: &str) -> Option<&SegmentAttribute> {
        self.0.get(name)
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct SegmentAttributes(HashMap<String, SegmentAttribute>);

impl SegmentAttributes {
    pub fn new(segment_attributes: HashMap<String, SegmentAttribute>) -> SegmentAttributes {
        SegmentAttributes(segment_attributes)
    }

    pub fn get(&self, name: &str) -> Option<&SegmentAttribute> {
        self.0.get(name)
    }

    pub fn merge<'a, I: Iterator<Item = &'a SegmentAttributes>>(
        segments_attributes: I,
        segment_attributes_config: &SegmentAttributesConfig,
    ) -> Self {
        let mut new_attributes = segment_attributes_config.segment_attributes();
        for segment_attributes in segments_attributes {
            for (name, new_segment_attribute) in new_attributes.0.iter_mut() {
                if let Some(segment_attribute) = segment_attributes.get(name) {
                    new_segment_attribute.merge(segment_attribute);
                }
            }
        }
        new_attributes
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<S: ToString> FromIterator<(S, SegmentAttribute)> for SegmentAttributes {
    fn from_iter<T: IntoIterator<Item = (S, SegmentAttribute)>>(iter: T) -> Self {
        SegmentAttributes(HashMap::from_iter(
            iter.into_iter().map(|(k, v)| (k.to_string(), v)),
        ))
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_merge() {
        let segment_attributes_config = SegmentAttributesConfig(
            vec![
                (
                    "is_frozen".to_string(),
                    SegmentAttribute::ConjunctiveBool(false),
                ),
                (
                    "contain_memes".to_string(),
                    SegmentAttribute::DisjunctiveBool(false),
                ),
                (
                    "ancestrals".to_string(),
                    SegmentAttribute::StringList(vec![]),
                ),
            ]
            .into_iter()
            .collect(),
        );
        let segment_attributes_1 = SegmentAttributes(
            vec![
                (
                    "is_frozen".to_string(),
                    SegmentAttribute::ConjunctiveBool(true),
                ),
                (
                    "contain_memes".to_string(),
                    SegmentAttribute::DisjunctiveBool(false),
                ),
                (
                    "ancestrals".to_string(),
                    SegmentAttribute::StringList(vec![
                        "segment_12".to_string(),
                        "segment_16".to_string(),
                    ]),
                ),
            ]
            .into_iter()
            .collect(),
        );
        let segment_attributes_2 = SegmentAttributes(
            vec![
                (
                    "is_frozen".to_string(),
                    SegmentAttribute::ConjunctiveBool(false),
                ),
                (
                    "contain_memes".to_string(),
                    SegmentAttribute::DisjunctiveBool(true),
                ),
                (
                    "ancestrals".to_string(),
                    SegmentAttribute::StringList(vec![
                        "segment_2".to_string(),
                        "segment_18".to_string(),
                    ]),
                ),
            ]
            .into_iter()
            .collect(),
        );
        let segment_attributes_result = SegmentAttributes::merge(
            vec![segment_attributes_1, segment_attributes_2].iter(),
            &segment_attributes_config,
        );
        let parsed: SegmentAttributes = serde_json::from_str(
            "{\"is_frozen\":{\"conjunctive_bool\":false},\"contain_memes\":{\"disjunctive_bool\":\
             true},\"ancestrals\":{\"string_list\":[\"segment_12\",\"segment_16\",\"segment_2\",\"\
             segment_18\"]}}",
        )
        .unwrap();
        assert_eq!(parsed, segment_attributes_result);
        assert_eq!(
            segment_attributes_result,
            SegmentAttributes(
                vec![
                    (
                        "is_frozen".to_string(),
                        SegmentAttribute::ConjunctiveBool(false)
                    ),
                    (
                        "contain_memes".to_string(),
                        SegmentAttribute::DisjunctiveBool(true)
                    ),
                    (
                        "ancestrals".to_string(),
                        SegmentAttribute::StringList(vec![
                            "segment_12".to_string(),
                            "segment_16".to_string(),
                            "segment_2".to_string(),
                            "segment_18".to_string()
                        ])
                    )
                ]
                .into_iter()
                .collect()
            )
        )
    }
}
