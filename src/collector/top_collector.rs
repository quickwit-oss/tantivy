use std::cmp::Ordering;
use std::collections::BinaryHeap;
use DocAddress;
use DocId;
use SegmentLocalId;

/// Contains a feature (field, score, etc.) of a document along with the document address.
///
/// It has a custom implementation of `PartialOrd` that reverses the order. This is because the
/// default Rust heap is a max heap, whereas a min heap is needed.
#[derive(Clone, Copy)]
pub struct ComparableDoc<T> {
    feature: T,
    doc_address: DocAddress,
}

impl<T: PartialOrd> PartialOrd for ComparableDoc<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: PartialOrd> Ord for ComparableDoc<T> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .feature
            .partial_cmp(&self.feature)
            .unwrap_or_else(|| other.doc_address.cmp(&self.doc_address))
    }
}

impl<T: PartialOrd> PartialEq for ComparableDoc<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T: PartialOrd> Eq for ComparableDoc<T> {}

/// The Top Collector keeps track of the K documents
/// sorted by type `T`.
///
/// The implementation is based on a `BinaryHeap`.
/// The theorical complexity for collecting the top `K` out of `n` documents
/// is `O(n log K)`.
pub struct TopCollector<T> {
    limit: usize,
    heap: BinaryHeap<ComparableDoc<T>>,
    segment_id: u32,
}

impl<T: PartialOrd + Clone> TopCollector<T> {
    /// Creates a top collector, with a number of documents equal to "limit".
    ///
    /// # Panics
    /// The method panics if limit is 0
    pub fn with_limit(limit: usize) -> TopCollector<T> {
        if limit < 1 {
            panic!("Limit must be strictly greater than 0.");
        }
        TopCollector {
            limit,
            heap: BinaryHeap::with_capacity(limit),
            segment_id: 0,
        }
    }

    /// Returns K best documents sorted in decreasing order.
    ///
    /// Calling this method triggers the sort.
    /// The result of the sort is not cached.
    pub fn docs(&self) -> Vec<DocAddress> {
        self.top_docs()
            .into_iter()
            .map(|(_feature, doc)| doc)
            .collect()
    }

    /// Returns K best FeatureDocuments sorted in decreasing order.
    ///
    /// Calling this method triggers the sort.
    /// The result of the sort is not cached.
    pub fn top_docs(&self) -> Vec<(T, DocAddress)> {
        let mut feature_docs: Vec<ComparableDoc<T>> = self.heap.iter().cloned().collect();
        feature_docs.sort();
        feature_docs
            .into_iter()
            .map(
                |ComparableDoc {
                     feature,
                     doc_address,
                 }| (feature, doc_address),
            ).collect()
    }

    /// Return true iff at least K documents have gone through
    /// the collector.
    #[inline]
    pub fn at_capacity(&self) -> bool {
        self.heap.len() >= self.limit
    }

    /// Sets the segment local ID for the collector
    pub fn set_segment_id(&mut self, segment_id: SegmentLocalId) {
        self.segment_id = segment_id;
    }

    /// Collects a document scored by the given feature
    ///
    /// It collects documents until it has reached the max capacity. Once it reaches capacity, it
    /// will compare the lowest scoring item with the given one and keep whichever is greater.
    pub fn collect(&mut self, doc: DocId, feature: T) {
        if self.at_capacity() {
            // It's ok to unwrap as long as a limit of 0 is forbidden.
            let limit_doc: ComparableDoc<T> = self
                .heap
                .peek()
                .expect("Top collector with size 0 is forbidden")
                .clone();
            if limit_doc.feature < feature {
                let mut mut_head = self
                    .heap
                    .peek_mut()
                    .expect("Top collector with size 0 is forbidden");
                mut_head.feature = feature;
                mut_head.doc_address = DocAddress(self.segment_id, doc);
            }
        } else {
            let wrapped_doc = ComparableDoc {
                feature,
                doc_address: DocAddress(self.segment_id, doc),
            };
            self.heap.push(wrapped_doc);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use DocId;
    use Score;

    #[test]
    fn test_top_collector_not_at_capacity() {
        let mut top_collector = TopCollector::with_limit(4);
        top_collector.collect(1, 0.8);
        top_collector.collect(3, 0.2);
        top_collector.collect(5, 0.3);
        assert!(!top_collector.at_capacity());
        let score_docs: Vec<(Score, DocId)> = top_collector
            .top_docs()
            .into_iter()
            .map(|(score, doc_address)| (score, doc_address.doc()))
            .collect();
        assert_eq!(score_docs, vec![(0.8, 1), (0.3, 5), (0.2, 3)]);
    }

    #[test]
    fn test_top_collector_at_capacity() {
        let mut top_collector = TopCollector::with_limit(4);
        top_collector.collect(1, 0.8);
        top_collector.collect(3, 0.2);
        top_collector.collect(5, 0.3);
        top_collector.collect(7, 0.9);
        top_collector.collect(9, -0.2);
        assert!(top_collector.at_capacity());
        {
            let score_docs: Vec<(Score, DocId)> = top_collector
                .top_docs()
                .into_iter()
                .map(|(score, doc_address)| (score, doc_address.doc()))
                .collect();
            assert_eq!(score_docs, vec![(0.9, 7), (0.8, 1), (0.3, 5), (0.2, 3)]);
        }
        {
            let docs: Vec<DocId> = top_collector
                .docs()
                .into_iter()
                .map(|doc_address| doc_address.doc())
                .collect();
            assert_eq!(docs, vec![7, 1, 5, 3]);
        }
    }

    #[test]
    #[should_panic]
    fn test_top_0() {
        let _collector: TopCollector<Score> = TopCollector::with_limit(0);
    }

}
