use std::io;
use super::Collector;
use ScoredDoc;
use SegmentReader;
use SegmentLocalId;
use DocAddress;
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use Score;

// Rust heap is a max-heap and we need a min heap.
#[derive(Clone, Copy)]
struct GlobalScoredDoc(Score, DocAddress);

impl PartialOrd for GlobalScoredDoc {
    fn partial_cmp(&self, other: &GlobalScoredDoc) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GlobalScoredDoc {
    #[inline(always)]
    fn cmp(&self, other: &GlobalScoredDoc) -> Ordering {
        other.0.partial_cmp(&self.0)
        .unwrap_or(
            other.1.cmp(&self.1)
        )
    }
}

impl PartialEq for GlobalScoredDoc {
    fn eq(&self, other: &GlobalScoredDoc) -> bool {
        self.cmp(&other) == Ordering::Equal
    }
}

impl Eq for GlobalScoredDoc {}

pub struct TopCollector {
    limit: usize,
    heap: BinaryHeap<GlobalScoredDoc>,
    segment_id: u32,
}

impl TopCollector {

    /// Creates a top collector, with a number of document of "limit"
    ///
    /// # Panics
    /// The method panics if limit is 0
    pub fn with_limit(limit: usize) -> TopCollector {
        if limit < 1 {
            panic!("Limit must be strictly greater than 0.");
        }
        TopCollector {
            limit: limit,
            heap: BinaryHeap::with_capacity(limit),
            segment_id: 0,
        }
    }

    pub fn docs(&self) -> Vec<DocAddress> {
        self.score_docs()
            .into_iter()
            .map(|score_doc| score_doc.1)
            .collect()
    }

    pub fn score_docs(&self) -> Vec<(Score, DocAddress)> {
        let mut scored_docs: Vec<GlobalScoredDoc> = self.heap
            .iter()
            .cloned()
            .collect();
        scored_docs.sort();
        scored_docs.into_iter()
            .map(|GlobalScoredDoc(score, doc_address)| (score, doc_address))
            .collect()
    }

    #[inline(always)]
    pub fn at_capacity(&self, ) -> bool {
        self.heap.len() >= self.limit
    }
}

impl Collector for TopCollector {

    fn set_segment(&mut self, segment_id: SegmentLocalId, _: &SegmentReader) -> io::Result<()> {
        self.segment_id = segment_id;
        Ok(())
    }

    fn collect(&mut self, scored_doc: ScoredDoc) {
        if self.at_capacity() {
            // It's ok to unwrap as long as a limit of 0 is forbidden.
            let limit_doc: GlobalScoredDoc = *self.heap.peek().unwrap();
            if limit_doc.0 < scored_doc.score() {
                let wrapped_doc = GlobalScoredDoc(scored_doc.score(), DocAddress(self.segment_id, scored_doc.doc()));
                self.heap.replace(wrapped_doc);
            }
        }
        else {
            let wrapped_doc = GlobalScoredDoc(scored_doc.score(), DocAddress(self.segment_id, scored_doc.doc()));
            self.heap.push(wrapped_doc);
        }

    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use ScoredDoc;
    use DocId;
    use Score;
    use collector::Collector;

    #[test]
    fn test_top_collector_not_at_capacity() {
        let mut top_collector = TopCollector::with_limit(4);
        top_collector.collect(ScoredDoc(0.8, 1));
        top_collector.collect(ScoredDoc(0.2, 3));
        top_collector.collect(ScoredDoc(0.3, 5));
        assert!(!top_collector.at_capacity());
        let score_docs: Vec<(Score, DocId)> = top_collector.score_docs()
            .into_iter()
            .map(|(score, doc_address)| (score, doc_address.doc()))
            .collect();
        assert_eq!(score_docs, vec!(
            (0.8, 1), (0.3, 5), (0.2, 3),
        ));
    }

    #[test]
    fn test_top_collector_at_capacity() {
        let mut top_collector = TopCollector::with_limit(4);
        top_collector.collect(ScoredDoc(0.8, 1));
        top_collector.collect(ScoredDoc(0.2, 3));
        top_collector.collect(ScoredDoc(0.3, 5));
        top_collector.collect(ScoredDoc(0.9, 7));
        top_collector.collect(ScoredDoc(-0.2, 9));
        assert!(top_collector.at_capacity());
        {
            let score_docs: Vec<(Score, DocId)> = top_collector
                .score_docs()
                .into_iter()
                .map(|(score, doc_address)| (score, doc_address.doc()))
                .collect();
            assert_eq!(score_docs, vec!(
                (0.9, 7), (0.8, 1), (0.3, 5), (0.2, 3)
            ));
        }
        {
            let docs: Vec<DocId> = top_collector
                .docs()
                .into_iter()
                .map(|doc_address| doc_address.doc())
                .collect();
            assert_eq!(docs, vec!(7, 1, 5, 3));
        }
        
        
    }

    #[test]
    #[should_panic]
    fn test_top_0() {
        TopCollector::with_limit(0);
    }
}