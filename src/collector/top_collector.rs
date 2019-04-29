use crate::DocAddress;
use crate::DocId;
use crate::Result;
use crate::SegmentLocalId;
use crate::SegmentReader;
use serde::export::PhantomData;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Contains a feature (field, score, etc.) of a document along with the document address.
///
/// It has a custom implementation of `PartialOrd` that reverses the order. This is because the
/// default Rust heap is a max heap, whereas a min heap is needed.
///
/// WARNING: equality is not what you would expect here.
/// Two elements are equal if their feature is equal, and regardless of whether `doc`
/// is equal. This should be perfectly fine for this usage, but let's make sure this
/// struct is never public.
struct ComparableDoc<T, D> {
    feature: T,
    doc: D,
}

impl<T: PartialOrd, D> PartialOrd for ComparableDoc<T, D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: PartialOrd, D> Ord for ComparableDoc<T, D> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .feature
            .partial_cmp(&self.feature)
            .unwrap_or_else(|| Ordering::Equal)
    }
}

impl<T: PartialOrd, D> PartialEq for ComparableDoc<T, D> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T: PartialOrd, D> Eq for ComparableDoc<T, D> {}

pub(crate) struct TopCollector<T> {
    limit: usize,
    _marker: PhantomData<T>,
}

impl<T> TopCollector<T>
where
    T: PartialOrd + Clone,
{
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
            _marker: PhantomData,
        }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn merge_fruits(
        &self,
        children: Vec<Vec<(T, DocAddress)>>,
    ) -> Result<Vec<(T, DocAddress)>> {
        if self.limit == 0 {
            return Ok(Vec::new());
        }
        let mut top_collector = BinaryHeap::new();
        for child_fruit in children {
            for (feature, doc) in child_fruit {
                if top_collector.len() < self.limit {
                    top_collector.push(ComparableDoc { feature, doc });
                } else if let Some(mut head) = top_collector.peek_mut() {
                    if head.feature < feature {
                        *head = ComparableDoc { feature, doc };
                    }
                }
            }
        }
        Ok(top_collector
            .into_sorted_vec()
            .into_iter()
            .map(|cdoc| (cdoc.feature, cdoc.doc))
            .collect())
    }

    pub(crate) fn for_segment<F: PartialOrd>(
        &self,
        segment_id: SegmentLocalId,
        _: &SegmentReader,
    ) -> Result<TopSegmentCollector<F>> {
        Ok(TopSegmentCollector::new(segment_id, self.limit))
    }
}

/// The Top Collector keeps track of the K documents
/// sorted by type `T`.
///
/// The implementation is based on a `BinaryHeap`.
/// The theorical complexity for collecting the top `K` out of `n` documents
/// is `O(n log K)`.
pub(crate) struct TopSegmentCollector<T> {
    limit: usize,
    heap: BinaryHeap<ComparableDoc<T, DocId>>,
    segment_id: u32,
}

impl<T: PartialOrd> TopSegmentCollector<T> {
    fn new(segment_id: SegmentLocalId, limit: usize) -> TopSegmentCollector<T> {
        TopSegmentCollector {
            limit,
            heap: BinaryHeap::with_capacity(limit),
            segment_id,
        }
    }
}

impl<T: PartialOrd + Clone> TopSegmentCollector<T> {
    pub fn harvest(self) -> Vec<(T, DocAddress)> {
        let segment_id = self.segment_id;
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|comparable_doc| {
                (
                    comparable_doc.feature,
                    DocAddress(segment_id, comparable_doc.doc),
                )
            })
            .collect()
    }

    /// Return true iff at least K documents have gone through
    /// the collector.
    #[inline(always)]
    pub(crate) fn at_capacity(&self) -> bool {
        self.heap.len() >= self.limit
    }

    /// Collects a document scored by the given feature
    ///
    /// It collects documents until it has reached the max capacity. Once it reaches capacity, it
    /// will compare the lowest scoring item with the given one and keep whichever is greater.
    #[inline(always)]
    pub fn collect(&mut self, doc: DocId, feature: T) {
        if self.at_capacity() {
            // It's ok to unwrap as long as a limit of 0 is forbidden.
            if let Some(limit_feature) = self.heap.peek().map(|head| head.feature.clone()) {
                if limit_feature < feature {
                    if let Some(mut head) = self.heap.peek_mut() {
                        head.feature = feature;
                        head.doc = doc;
                    }
                }
            }
        } else {
            // we have not reached capacity yet, so we can just push the
            // element.
            self.heap.push(ComparableDoc { feature, doc });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TopSegmentCollector;
    use super::{TopCollector, TopSegmentCollector};
    use crate::DocAddress;
    use crate::Score;
    use DocAddress;

    #[test]
    fn test_top_collector_not_at_capacity() {
        let mut top_collector = TopSegmentCollector::new(0, 4);
        top_collector.collect(1, 0.8);
        top_collector.collect(3, 0.2);
        top_collector.collect(5, 0.3);
        assert_eq!(
            top_collector.harvest(),
            vec![
                (0.8, DocAddress(0, 1)),
                (0.3, DocAddress(0, 5)),
                (0.2, DocAddress(0, 3))
            ]
        );
    }

    #[test]
    fn test_top_collector_at_capacity() {
        let mut top_collector = TopSegmentCollector::new(0, 4);
        top_collector.collect(1, 0.8);
        top_collector.collect(3, 0.2);
        top_collector.collect(5, 0.3);
        top_collector.collect(7, 0.9);
        top_collector.collect(9, -0.2);
        assert_eq!(
            top_collector.harvest(),
            vec![
                (0.9, DocAddress(0, 7)),
                (0.8, DocAddress(0, 1)),
                (0.3, DocAddress(0, 5)),
                (0.2, DocAddress(0, 3))
            ]
        );
    }
}
