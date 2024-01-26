use std::cmp::Ordering;
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use super::top_score_collector::TopNComputer;
use crate::{DocAddress, DocId, SegmentOrdinal, SegmentReader};

/// Contains a feature (field, score, etc.) of a document along with the document address.
///
/// It guarantees stable sorting: in case of a tie on the feature, the document
/// address is used.
///
/// The REVERSE_ORDER generic parameter controls whether the by-feature order
/// should be reversed, which is useful for achieving for example largest-first
/// semantics without having to wrap the feature in a `Reverse`.
///
/// WARNING: equality is not what you would expect here.
/// Two elements are equal if their feature is equal, and regardless of whether `doc`
/// is equal. This should be perfectly fine for this usage, but let's make sure this
/// struct is never public.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct ComparableDoc<T, D, const REVERSE_ORDER: bool = false> {
    /// The feature of the document. In practice, this is
    /// is any type that implements `PartialOrd`.
    pub feature: T,
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
            .field("feature", &self.feature)
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
            .feature
            .partial_cmp(&other.feature)
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

pub(crate) struct TopCollector<T> {
    pub limit: usize,
    pub offset: usize,
    _marker: PhantomData<T>,
}

impl<T> TopCollector<T>
where T: PartialOrd + Clone
{
    /// Creates a top collector, with a number of documents equal to "limit".
    ///
    /// # Panics
    /// The method panics if limit is 0
    pub fn with_limit(limit: usize) -> TopCollector<T> {
        assert!(limit >= 1, "Limit must be strictly greater than 0.");
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
        let mut top_collector: TopNComputer<_, _> = TopNComputer::new(self.limit + self.offset);
        for child_fruit in children {
            for (feature, doc) in child_fruit {
                top_collector.push(feature, doc);
            }
        }

        Ok(top_collector
            .into_sorted_vec()
            .into_iter()
            .skip(self.offset)
            .map(|cdoc| (cdoc.feature, cdoc.doc))
            .collect())
    }

    pub(crate) fn for_segment<F: PartialOrd + Clone>(
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
/// The implementation is based on a repeatedly truncating on the median after K * 2 documents
/// The theoretical complexity for collecting the top `K` out of `n` documents
/// is `O(n + K)`.
pub(crate) struct TopSegmentCollector<T> {
    /// We reverse the order of the feature in order to
    /// have top-semantics instead of bottom semantics.
    topn_computer: TopNComputer<T, DocId>,
    segment_ord: u32,
}

impl<T: PartialOrd + Clone> TopSegmentCollector<T> {
    fn new(segment_ord: SegmentOrdinal, limit: usize) -> TopSegmentCollector<T> {
        TopSegmentCollector {
            topn_computer: TopNComputer::new(limit),
            segment_ord,
        }
    }
}

impl<T: PartialOrd + Clone> TopSegmentCollector<T> {
    pub fn harvest(self) -> Vec<(T, DocAddress)> {
        let segment_ord = self.segment_ord;
        self.topn_computer
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

    /// Collects a document scored by the given feature
    ///
    /// It collects documents until it has reached the max capacity. Once it reaches capacity, it
    /// will compare the lowest scoring item with the given one and keep whichever is greater.
    #[inline]
    pub fn collect(&mut self, doc: DocId, feature: T) {
        self.topn_computer.push(feature, doc);
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
        let score = 3.3f32;

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
    use test::Bencher;

    use super::TopSegmentCollector;

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
