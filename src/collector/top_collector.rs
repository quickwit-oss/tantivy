use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

/// Contains a feature (field, score, etc.) of a document along with the document address.
///
/// It guarantees stable sorting: in case of a tie on the feature, the document
/// address is used.
///
/// The REVERSE_ORDER generic parameter controls whether the by-feature order
/// should be reversed, which is useful for achieving for example largest-first
/// semantics without having to wrap the feature in a `Reverse`.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct ComparableDoc<T, D, const REVERSE_ORDER: bool = false> {
    /// The feature of the document. In practice, this is
    /// is any type that implements `PartialOrd`.
    pub sort_key: T,
    /// The document address. In practice, this is any
    /// type that implements `PartialOrd`, and is guaranteed
    /// to be unique for each document.
    pub doc: D,
}
impl<T: std::fmt::Debug, D: std::fmt::Debug, const R: bool> std::fmt::Debug
    for ComparableDoc<T, D, R>
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct(format!("ComparableDoc<_, _ {R}").as_str())
            .field("feature", &self.sort_key)
            .field("doc", &self.doc)
            .finish()
    }
}

impl<T: PartialOrd, D: PartialOrd, const R: bool> PartialOrd for ComparableDoc<T, D, R> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: PartialOrd, D: PartialOrd, const R: bool> Ord for ComparableDoc<T, D, R> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        let by_feature = self
            .sort_key
            .partial_cmp(&other.sort_key)
            .map(|ord| if R { ord.reverse() } else { ord })
            .unwrap_or(Ordering::Equal);

        let lazy_by_doc_address = || self.doc.partial_cmp(&other.doc).unwrap_or(Ordering::Equal);

        // In case of a tie on the feature, we sort by ascending
        // `DocAddress` in order to ensure a stable sorting of the
        // documents.
        by_feature.then_with(lazy_by_doc_address)
    }
}

impl<T: PartialOrd, D: PartialOrd, const R: bool> PartialEq for ComparableDoc<T, D, R> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T: PartialOrd, D: PartialOrd, const R: bool> Eq for ComparableDoc<T, D, R> {}
