use std::collections::btree_map::BTreeMap;
use {DocId, TantivyError};

pub fn does_not_match(doc: DocId) -> TantivyError {
    TantivyError::InvalidArgument(format!("Document #({}) does not match", doc))
}

#[derive(Clone, Serialize)]
#[serde(untagged)]
pub enum Explanation {
    Value(f32),
    Formula {
        msg: String,
        val: f32,
        children: BTreeMap<String, Explanation>,
    },
}

impl Explanation {
    pub fn new<T: ToString>(
        msg: T,
        val: f32,
        children: BTreeMap<String, Explanation>,
    ) -> Explanation {
        Explanation::Formula {
            msg: msg.to_string(),
            val,
            children,
        }
    }

    pub fn value(val: f32) -> Explanation {
        Explanation::Value(val)
    }

    pub fn to_string(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }
}
