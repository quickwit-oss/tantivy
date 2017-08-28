use query::Scorer;
use DocId;
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use postings::DocSet;
use query::OccurFilter;
use query::boolean_query::ScoreCombiner;


/// Each `HeapItem` represents the head of
/// one of scorer being merged.
///
/// * `doc` - is the current doc id for the given segment postings
/// * `ord` - is the ordinal used to identify to which segment postings
/// this heap item belong to.
#[derive(Eq, PartialEq)]
struct HeapItem {
    doc: DocId,
    ord: u32,
}

/// `HeapItem` are ordered by the document
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        (other.doc).cmp(&self.doc)
    }
}

pub struct BooleanScorer<TScorer: Scorer> {
    scorers: Vec<TScorer>,
    queue: BinaryHeap<HeapItem>,
    doc: DocId,
    score_combiner: ScoreCombiner,
    occur_filter: OccurFilter,
}

impl<TScorer: Scorer> BooleanScorer<TScorer> {
    pub fn new(scorers: Vec<TScorer>, occur_filter: OccurFilter) -> BooleanScorer<TScorer> {
        let score_combiner = ScoreCombiner::default_for_num_scorers(scorers.len());
        let mut non_empty_scorers: Vec<TScorer> = Vec::new();
        for mut posting in scorers {
            let non_empty = posting.advance();
            if non_empty {
                non_empty_scorers.push(posting);
            }
        }
        let heap_items: Vec<HeapItem> = non_empty_scorers
            .iter()
            .map(|posting| posting.doc())
            .enumerate()
            .map(|(ord, doc)| {
                HeapItem {
                    doc: doc,
                    ord: ord as u32,
                }
            })
            .collect();
        BooleanScorer {
            scorers: non_empty_scorers,
            queue: BinaryHeap::from(heap_items),
            doc: 0u32,
            score_combiner: score_combiner,
            occur_filter: occur_filter,
        }
    }

    /// Advances the head of our heap (the segment posting with the lowest doc)
    /// It will also update the new current `DocId` as well as the term frequency
    /// associated with the segment postings.
    ///
    /// After advancing the `SegmentPosting`, the postings is removed from the heap
    /// if it has been entirely consumed, or pushed back into the heap.
    ///
    /// # Panics
    /// This method will panic if the head `SegmentPostings` is not empty.
    fn advance_head(&mut self) {
        {
            let mut mutable_head = self.queue.peek_mut().unwrap();
            let cur_scorers = &mut self.scorers[mutable_head.ord as usize];
            if cur_scorers.advance() {
                mutable_head.doc = cur_scorers.doc();
                return;
            }
        }
        self.queue.pop();
    }
}

impl<TScorer: Scorer> DocSet for BooleanScorer<TScorer> {
    fn size_hint(&self) -> usize {
        // TODO fix this. it should be the min
        // of the MUST scorer
        // and the max of the SHOULD scorers.
        self.scorers
            .iter()
            .map(|scorer| scorer.size_hint())
            .max()
            .unwrap()
    }


    fn advance(&mut self) -> bool {
        loop {
            self.score_combiner.clear();
            let mut ord_bitset = 0u64;
            match self.queue.peek() {
                Some(heap_item) => {
                    let ord = heap_item.ord as usize;
                    self.doc = heap_item.doc;
                    let score = self.scorers[ord].score();
                    self.score_combiner.update(score);
                    ord_bitset |= 1 << ord;
                }
                None => {
                    return false;
                }
            }
            self.advance_head();
            while let Some(&HeapItem { doc, ord }) = self.queue.peek() {
                if doc == self.doc {
                    let ord = ord as usize;
                    let score = self.scorers[ord].score();
                    self.score_combiner.update(score);
                    ord_bitset |= 1 << ord;
                } else {
                    break;
                }
                self.advance_head();
            }
            if self.occur_filter.accept(ord_bitset) {
                return true;
            }
        }
    }

    fn doc(&self) -> DocId {
        self.doc
    }
}

impl<TScorer: Scorer> Scorer for BooleanScorer<TScorer> {
    fn score(&self) -> f32 {
        self.score_combiner.score()
    }
}
