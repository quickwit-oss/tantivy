use super::Collector;
use SegmentReader;
use SegmentLocalId;
use DocAddress;
use Result;
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use DocId;
use Score;

// Rust heap is a max-heap and we need a min heap.
#[derive(Clone, Copy)]
struct GlobalScoredDoc {
    score: Score,
    doc_address: DocAddress,
}

impl PartialOrd for GlobalScoredDoc {
    fn partial_cmp(&self, other: &GlobalScoredDoc) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GlobalScoredDoc {
    #[inline]
    fn cmp(&self, other: &GlobalScoredDoc) -> Ordering {
        other
            .score
            .partial_cmp(&self.score)
            .unwrap_or_else(|| other.doc_address.cmp(&self.doc_address))
    }
}

impl PartialEq for GlobalScoredDoc {
    fn eq(&self, other: &GlobalScoredDoc) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for GlobalScoredDoc {}

/// The Top Collector keeps track of the K documents
/// with the best scores.
///
/// The implementation is based on a `BinaryHeap`.
/// The theorical complexity is `O(n log K)`.
pub struct TopCollector {
    limit: usize,
    heap: BinaryHeap<GlobalScoredDoc>,
    segment_id: u32,
}

impl TopCollector {
    /// Creates a top collector, with a number of documents equal to "limit".
    ///
    /// # Panics
    /// The method panics if limit is 0
    pub fn with_limit(limit: usize) -> TopCollector {
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
        self.score_docs()
            .into_iter()
            .map(|score_doc| score_doc.1)
            .collect()
    }

    /// Returns K best ScoredDocument sorted in decreasing order.
    ///
    /// Calling this method triggers the sort.
    /// The result of the sort is not cached.
    pub fn score_docs(&self) -> Vec<(Score, DocAddress)> {
        let mut scored_docs: Vec<GlobalScoredDoc> = self.heap.iter().cloned().collect();
        scored_docs.sort();
        scored_docs
            .into_iter()
            .map(|GlobalScoredDoc { score, doc_address }| (score, doc_address))
            .collect()
    }

    /// Return true iff at least K documents have gone through
    /// the collector.
    #[inline]
    pub fn at_capacity(&self) -> bool {
        self.heap.len() >= self.limit
    }
}

impl Collector for TopCollector {
    fn set_segment(&mut self, segment_id: SegmentLocalId, _: &SegmentReader) -> Result<()> {
        self.segment_id = segment_id;
        Ok(())
    }

    fn collect(&mut self, doc: DocId, score: Score) {
        if self.at_capacity() {
            // It's ok to unwrap as long as a limit of 0 is forbidden.
            let limit_doc: GlobalScoredDoc = *self.heap
                .peek()
                .expect("Top collector with size 0 is forbidden");
            if limit_doc.score < score {
                let mut mut_head = self.heap
                    .peek_mut()
                    .expect("Top collector with size 0 is forbidden");
                mut_head.score = score;
                mut_head.doc_address = DocAddress(self.segment_id, doc);
            }
        } else {
            let wrapped_doc = GlobalScoredDoc {
                score,
                doc_address: DocAddress(self.segment_id, doc),
            };
            self.heap.push(wrapped_doc);
        }
    }

    fn requires_scoring(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use DocId;
    use Score;
    use collector::Collector;

    #[test]
    fn test_top_collector_not_at_capacity() {
        let mut top_collector = TopCollector::with_limit(4);
        top_collector.collect(1, 0.8);
        top_collector.collect(3, 0.2);
        top_collector.collect(5, 0.3);
        assert!(!top_collector.at_capacity());
        let score_docs: Vec<(Score, DocId)> = top_collector
            .score_docs()
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
                .score_docs()
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
        TopCollector::with_limit(0);
    }
}
