use crate::{DocId, Score, TantivyError};
use serde::Serialize;
use std::fmt;

pub(crate) fn does_not_match(doc: DocId) -> TantivyError {
    TantivyError::InvalidArgument(format!("Document #({}) does not match", doc))
}

/// Object describing the score of a given document.
/// It is organized in trees.
///
/// `.to_pretty_json()` can be useful to print out a human readable
/// representation of this tree when debugging a given score.
#[derive(Clone, Serialize)]
pub struct Explanation {
    value: Score,
    description: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    details: Vec<Explanation>,
    context: Vec<String>,
}

impl fmt::Debug for Explanation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Explanation({})", self.to_pretty_json())
    }
}

impl Explanation {
    /// Creates a new explanation object.
    pub fn new<T: ToString>(description: T, value: Score) -> Explanation {
        Explanation {
            value,
            description: description.to_string(),
            details: vec![],
            context: vec![],
        }
    }

    /// Returns the value associated to the current node.
    pub fn value(&self) -> Score {
        self.value
    }

    /// Add some detail, explaining some part of the current node formula.
    ///
    /// Details are treated as child of the current node.
    pub fn add_detail(&mut self, child_explanation: Explanation) {
        self.details.push(child_explanation);
    }

    /// Adds some extra context to the explanation.
    pub fn add_context(&mut self, context: String) {
        self.context.push(context);
    }

    /// Shortcut for `self.details.push(Explanation::new(name, value));`
    pub fn add_const<T: ToString>(&mut self, name: T, value: Score) {
        self.details.push(Explanation::new(name, value));
    }

    /// Returns an indented json representation of the explanation tree for debug usage.
    pub fn to_pretty_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }
}
