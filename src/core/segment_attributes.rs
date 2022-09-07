use std::collections::HashMap;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum SegmentAttribute {
    ConjunctiveBool(bool),
    DisjunctiveBool(bool),
    StringList(Vec<String>),
}

impl SegmentAttribute {
    pub fn merge(&mut self, other: &SegmentAttribute) {
        match self {
            SegmentAttribute::ConjunctiveBool(a) => match other {
                SegmentAttribute::ConjunctiveBool(b) => *a = *a && *b,
                _ => unreachable!(),
            },
            SegmentAttribute::DisjunctiveBool(a) => match other {
                SegmentAttribute::DisjunctiveBool(b) => *a = *a || *b,
                _ => unreachable!(),
            },
            SegmentAttribute::StringList(a) => match other {
                SegmentAttribute::StringList(b) => a.extend(b.iter().cloned()),
                _ => unreachable!(),
            },
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SegmentAttributesConfig(HashMap<String, SegmentAttribute>);

impl SegmentAttributesConfig {
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
