use crate::DocAddress;
use crate::DocId;
use crate::SegmentOrdinal;
use crate::SegmentReader;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::marker::PhantomData;

/// Contains a feature (field, score, etc.) of a document along with the document address.
///
/// It has a custom implementation of `PartialOrd` that reverses the order. This is because the
/// default Rust heap is a max heap, whereas a min heap is needed.
///
/// Additionally, it guarantees stable sorting: in case of a tie on the feature, the document
/// address is used.
///
/// WARNING: equality is not what you would expect here.
/// Two elements are equal if their feature is equal, and regardless of whether `doc`
/// is equal. This should be perfectly fine for this usage, but let's make sure this
/// struct is never public.
pub(crate) struct ComparableDoc<T, D> {
    pub feature: T,
    pub doc: D,
}

impl<T: PartialOrd, D: PartialOrd> PartialOrd for ComparableDoc<T, D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: PartialOrd, D: PartialOrd> Ord for ComparableDoc<T, D> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed to make BinaryHeap work as a min-heap
        let by_feature = other
            .feature
            .partial_cmp(&self.feature)
            .unwrap_or(Ordering::Equal);

        let lazy_by_doc_address = || self.doc.partial_cmp(&other.doc).unwrap_or(Ordering::Equal);

        // In case of a tie on the feature, we sort by ascending
        // `DocAddress` in order to ensure a stable sorting of the
        // documents.
        by_feature.then_with(lazy_by_doc_address)
    }
}

impl<T: PartialOrd, D: PartialOrd> PartialEq for ComparableDoc<T, D> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T: PartialOrd, D: PartialOrd> Eq for ComparableDoc<T, D> {}

pub(crate) struct TopCollector<T> {
    pub limit: usize,
    pub offset: usize,
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
        Self {
            limit,
            offset: 0,
            _marker: PhantomData,
        }
    }

    /// Skip the first "offset" documents when collecting.
    ///
    /// This is equivalent to `OFFSET` in MySQL or PostgreSQL and `start` in
    /// Lucene's TopDocsCollector.
    pub fn and_offset(mut self, offset: usize) -> TopCollector<T> {
        self.offset = offset;
        self
    }

    pub fn merge_fruits(
        &self,
        children: Vec<Vec<(T, DocAddress)>>,
    ) -> crate::Result<Vec<(T, DocAddress)>> {
        if self.limit == 0 {
            return Ok(Vec::new());
        }
        let mut top_collector = BinaryHeap::new();
        for child_fruit in children {
            for (feature, doc) in child_fruit {
                if top_collector.len() < (self.limit + self.offset) {
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
            .skip(self.offset)
            .map(|cdoc| (cdoc.feature, cdoc.doc))
            .collect())
    }

    pub(crate) fn for_segment<F: PartialOrd>(
        &self,
        segment_id: SegmentOrdinal,
        _: &SegmentReader,
    ) -> TopSegmentCollector<F> {
        TopSegmentCollector::new(segment_id, self.limit + self.offset)
    }

    /// Create a new TopCollector with the same limit and offset.
    ///
    /// Ideally we would use Into but the blanket implementation seems to cause the Scorer traits
    /// to fail.
    #[doc(hidden)]
    pub(crate) fn into_tscore<TScore: PartialOrd + Clone>(self) -> TopCollector<TScore> {
        TopCollector {
            limit: self.limit,
            offset: self.offset,
            _marker: PhantomData,
        }
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
    segment_ord: u32,
}

impl<T: PartialOrd> TopSegmentCollector<T> {
    fn new(segment_ord: SegmentOrdinal, limit: usize) -> TopSegmentCollector<T> {
        TopSegmentCollector {
            limit,
            heap: BinaryHeap::with_capacity(limit),
            segment_ord,
        }
    }
}

impl<T: PartialOrd + Clone> TopSegmentCollector<T> {
    pub fn harvest(self) -> Vec<(T, DocAddress)> {
        let segment_ord = self.segment_ord;
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|comparable_doc| {
                (
                    comparable_doc.feature,
                    DocAddress {
                        segment_ord,
                        doc_id: comparable_doc.doc,
                    },
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
    use super::{TopCollector, TopSegmentCollector};
    use crate::DocAddress;

    #[test]
    fn test_top_collector_not_at_capacity() {
        let mut top_collector = TopSegmentCollector::new(0, 4);
        top_collector.collect(1, 0.8);
        top_collector.collect(3, 0.2);
        top_collector.collect(5, 0.3);
        assert_eq!(
            top_collector.harvest(),
            vec![
                (0.8, DocAddress::new(0, 1)),
                (0.3, DocAddress::new(0, 5)),
                (0.2, DocAddress::new(0, 3))
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
                (0.9, DocAddress::new(0, 7)),
                (0.8, DocAddress::new(0, 1)),
                (0.3, DocAddress::new(0, 5)),
                (0.2, DocAddress::new(0, 3))
            ]
        );
    }

    #[test]
    fn test_top_segment_collector_stable_ordering_for_equal_feature() {
        // given that the documents are collected in ascending doc id order,
        // when harvesting we have to guarantee stable sorting in case of a tie
        // on the score
        let doc_ids_collection = [4, 5, 6];
        let score = 3.14;

        let mut top_collector_limit_2 = TopSegmentCollector::new(0, 2);
        for id in &doc_ids_collection {
            top_collector_limit_2.collect(*id, score);
        }

        let mut top_collector_limit_3 = TopSegmentCollector::new(0, 3);
        for id in &doc_ids_collection {
            top_collector_limit_3.collect(*id, score);
        }

        assert_eq!(
            top_collector_limit_2.harvest(),
            top_collector_limit_3.harvest()[..2].to_vec(),
        );
    }

    #[test]
    fn test_top_collector_with_limit_and_offset() {
        let collector = TopCollector::with_limit(2).and_offset(1);

        let results = collector
            .merge_fruits(vec![vec![
                (0.9, DocAddress::new(0, 1)),
                (0.8, DocAddress::new(0, 2)),
                (0.7, DocAddress::new(0, 3)),
                (0.6, DocAddress::new(0, 4)),
                (0.5, DocAddress::new(0, 5)),
            ]])
            .unwrap();

        assert_eq!(
            results,
            vec![(0.8, DocAddress::new(0, 2)), (0.7, DocAddress::new(0, 3)),]
        );
    }

    #[test]
    fn test_top_collector_with_limit_larger_than_set_and_offset() {
        let collector = TopCollector::with_limit(2).and_offset(1);

        let results = collector
            .merge_fruits(vec![vec![
                (0.9, DocAddress::new(0, 1)),
                (0.8, DocAddress::new(0, 2)),
            ]])
            .unwrap();

        assert_eq!(results, vec![(0.8, DocAddress::new(0, 2)),]);
    }

    #[test]
    fn test_top_collector_with_limit_and_offset_larger_than_set() {
        let collector = TopCollector::with_limit(2).and_offset(20);

        let results = collector
            .merge_fruits(vec![vec![
                (0.9, DocAddress::new(0, 1)),
                (0.8, DocAddress::new(0, 2)),
            ]])
            .unwrap();

        assert_eq!(results, vec![]);
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use super::TopSegmentCollector;
    use test::Bencher;

    #[bench]
    fn bench_top_segment_collector_collect_not_at_capacity(b: &mut Bencher) {
        let mut top_collector = TopSegmentCollector::new(0, 400);

        b.iter(|| {
            for i in 0..100 {
                top_collector.collect(i, 0.8);
            }
        });
    }

    #[bench]
    fn bench_top_segment_collector_collect_at_capacity(b: &mut Bencher) {
        let mut top_collector = TopSegmentCollector::new(0, 100);

        for i in 0..100 {
            top_collector.collect(i, 0.8);
        }

        b.iter(|| {
            for i in 0..100 {
                top_collector.collect(i, 0.8);
            }
        });
    }

    #[bench]
    fn bench_top_segment_collector_collect_and_harvest_many_ties(b: &mut Bencher) {
        b.iter(|| {
            let mut top_collector = TopSegmentCollector::new(0, 100);

            for i in 0..100 {
                top_collector.collect(i, 0.8);
            }

            // it would be nice to be able to do the setup N times but still
            // measure only harvest(). We can't since harvest() consumes
            // the top_collector.
            top_collector.harvest()
        });
    }

    #[bench]
    fn bench_top_segment_collector_collect_and_harvest_no_tie(b: &mut Bencher) {
        b.iter(|| {
            let mut top_collector = TopSegmentCollector::new(0, 100);
            let mut score = 1.0;

            for i in 0..100 {
                score += 1.0;
                top_collector.collect(i, score);
            }

            // it would be nice to be able to do the setup N times but still
            // measure only harvest(). We can't since harvest() consumes
            // the top_collector.
            top_collector.harvest()
        });
    }
}
