use serde::{Deserialize, Serialize};

/// Contains a feature (field, score, etc.) of a document along with the document address.
///
/// Used only by TopNComputer, which implements the actual comparison via a `Comparator`.
#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComparableDoc<T, D> {
    /// The feature of the document. In practice, this is
    /// is any type that implements `PartialOrd`.
    pub sort_key: T,
    /// The document address. In practice, this is any
    /// type that implements `PartialOrd`, and is guaranteed
    /// to be unique for each document.
    pub doc: D,
}

impl<T: std::fmt::Debug, D: std::fmt::Debug> std::fmt::Debug for ComparableDoc<T, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct(format!("ComparableDoc").as_str())
            .field("feature", &self.sort_key)
            .field("doc", &self.doc)
            .finish()
    }
}
