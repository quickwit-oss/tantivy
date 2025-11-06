use std::{cmp::Ordering, fmt};

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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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


/// Fast TopN Computation
///
/// Capacity of the vec is 2 * top_n.
/// The buffer is truncated to the top_n elements when it reaches the capacity of the Vec.
/// That means capacity has special meaning and should be carried over when cloning or serializing.
///
/// For TopN == 0, it will be relative expensive.
#[derive(Serialize, Deserialize)]
#[serde(from = "TopNComputerDeser<Score, D, REVERSE_ORDER>")]
pub struct TopNComputer<Score, D, const REVERSE_ORDER: bool = false> {
    /// The buffer reverses sort order to get top-semantics instead of bottom-semantics
    buffer: Vec<ComparableDoc<Score, D, REVERSE_ORDER>>,
    top_n: usize,
    pub(crate) threshold: Option<Score>,
}

impl<Score: std::fmt::Debug, D, const REVERSE_ORDER: bool> std::fmt::Debug
    for TopNComputer<Score, D, REVERSE_ORDER>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopNComputer")
            .field("buffer_len", &self.buffer.len())
            .field("top_n", &self.top_n)
            .field("current_threshold", &self.threshold)
            .finish()
    }
}

// Intermediate struct for TopNComputer for deserialization, to keep vec capacity
#[derive(Deserialize)]
struct TopNComputerDeser<Score, D, const REVERSE_ORDER: bool> {
    buffer: Vec<ComparableDoc<Score, D, REVERSE_ORDER>>,
    top_n: usize,
    threshold: Option<Score>,
}

// Custom clone to keep capacity
impl<Score: Clone, D: Clone, const REVERSE_ORDER: bool> Clone
    for TopNComputer<Score, D, REVERSE_ORDER>
{
    fn clone(&self) -> Self {
        let mut buffer_clone = Vec::with_capacity(self.buffer.capacity());
        buffer_clone.extend(self.buffer.iter().cloned());

        TopNComputer {
            buffer: buffer_clone,
            top_n: self.top_n,
            threshold: self.threshold.clone(),
        }
    }
}

impl<Score, D, const R: bool> From<TopNComputerDeser<Score, D, R>> for TopNComputer<Score, D, R> {
    fn from(mut value: TopNComputerDeser<Score, D, R>) -> Self {
        let expected_cap = value.top_n.max(1) * 2;
        let current_cap = value.buffer.capacity();
        if current_cap < expected_cap {
            value.buffer.reserve_exact(expected_cap - current_cap);
        } else {
            value.buffer.shrink_to(expected_cap);
        }

        TopNComputer {
            buffer: value.buffer,
            top_n: value.top_n,
            threshold: value.threshold,
        }
    }
}

impl<Score, D, const REVERSE_ORDER: bool> TopNComputer<Score, D, REVERSE_ORDER>
where
    Score: PartialOrd + Clone,
    D: Ord,
{
    /// Create a new `TopNComputer`.
    /// Internally it will allocate a buffer of size `2 * top_n`.
    pub fn new(top_n: usize) -> Self {
        let vec_cap = top_n.max(1) * 2;
        TopNComputer {
            buffer: Vec::with_capacity(vec_cap),
            top_n,
            threshold: None,
        }
    }

    /// Push a new document to the top n.
    /// If the document is below the current threshold, it will be ignored.
    #[inline]
    pub fn push(&mut self, sort_key: Score, doc: D) {
        if let Some(last_median) = self.threshold.clone() {
            if !REVERSE_ORDER && sort_key > last_median {
                return;
            }
            if REVERSE_ORDER && sort_key < last_median {
                return;
            }
        }
        if self.buffer.len() == self.buffer.capacity() {
            let median = self.truncate_top_n();
            self.threshold = Some(median);
        }

        // This is faster since it avoids the buffer resizing to be inlined from vec.push()
        // (this is in the hot path)
        // TODO: Replace with `push_within_capacity` when it's stabilized
        let uninit = self.buffer.spare_capacity_mut();
        // This cannot panic, because we truncate_median will at least remove one element, since
        // the min capacity is 2.
        uninit[0].write(ComparableDoc { doc, sort_key });
        // This is safe because it would panic in the line above
        unsafe {
            self.buffer.set_len(self.buffer.len() + 1);
        }
    }

    #[inline(never)]
    fn truncate_top_n(&mut self) -> Score {
        // Use select_nth_unstable to find the top nth score
        let (_, median_el, _) = self.buffer.select_nth_unstable(self.top_n);

        let median_score = median_el.sort_key.clone();
        // Remove all elements below the top_n
        self.buffer.truncate(self.top_n);

        median_score
    }

    /// Returns the top n elements in sorted order.
    pub fn into_sorted_vec(mut self) -> Vec<ComparableDoc<Score, D, REVERSE_ORDER>> {
        if self.buffer.len() > self.top_n {
            self.truncate_top_n();
        }
        self.buffer.sort_unstable();
        self.buffer
    }

    /// Returns the top n elements in stored order.
    /// Useful if you do not need the elements in sorted order,
    /// for example when merging the results of multiple segments.
    pub fn into_vec(mut self) -> Vec<ComparableDoc<Score, D, REVERSE_ORDER>> {
        if self.buffer.len() > self.top_n {
            self.truncate_top_n();
        }
        self.buffer
    }
}
