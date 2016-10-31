use query::Scorer;
use DocId;
use Score;
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use postings::DocSet;
use query::OccurFilter;


struct ScoreCombiner {
    coords: Vec<Score>,
    num_fields: usize,
    score: Score,
}

impl ScoreCombiner {
    
    fn update(&mut self, score: Score) {
        self.score += score;
        self.num_fields += 1;
    }

    fn clear(&mut self,) {
        self.score = 0f32;
        self.num_fields = 0;
    }
        
    /// Compute the coord term
    fn coord(&self,) -> f32 {
        self.coords[self.num_fields]
    }
    
    #[inline]
    fn score(&self, ) -> Score {
        self.score * self.coord()
    }
}

impl From<Vec<Score>> for ScoreCombiner {
    fn from(coords: Vec<Score>) -> ScoreCombiner {
        ScoreCombiner {
            coords: coords,
            num_fields: 0,
            score: 0f32,
        }
    }
}


/// Each `HeapItem` represents the head of
/// a segment postings being merged.
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
    fn cmp(&self, other:&Self) -> Ordering {
         (other.doc).cmp(&self.doc)
    }
}

pub struct BooleanScorer<TScorer: Scorer> {
    postings: Vec<TScorer>,
    queue: BinaryHeap<HeapItem>,
    doc: DocId,
    score_combiner: ScoreCombiner,
    filter: OccurFilter,
}

impl<TScorer: Scorer> BooleanScorer<TScorer> {
    
    pub fn new(postings: Vec<TScorer>, filter: OccurFilter) -> BooleanScorer<TScorer> {
        let num_postings = postings.len();
        let query_coords: Vec<Score> = (0..num_postings + 1)
            .map(|i| (i as Score) / (num_postings as Score))
            .collect();
        let score_combiner = ScoreCombiner::from(query_coords);
        let mut non_empty_postings: Vec<TScorer> = Vec::new();
        for mut posting in postings {
            let non_empty = posting.advance();
            if non_empty {
                non_empty_postings.push(posting);
            }
        }
        let heap_items: Vec<HeapItem> = non_empty_postings
            .iter()
            .map(|posting| posting.doc())
            .enumerate()
            .map(|(ord, doc)| {
                HeapItem {
                    doc: doc,
                    ord: ord as u32
                }
            })
            .collect();
        BooleanScorer {
            postings: non_empty_postings,
            queue: BinaryHeap::from(heap_items),
            doc: 0u32,
            score_combiner: score_combiner,
            filter: filter,
            
        }
    }
    
    
    /// Advances the head of our heap (the segment postings with the lowest doc)
    /// It will also update the new current `DocId` as well as the term frequency
    /// associated with the segment postings.
    /// 
    /// After advancing the `SegmentPosting`, the postings is removed from the heap
    /// if it has been entirely consumed, or pushed back into the heap.
    /// 
    /// # Panics
    /// This method will panic if the head `SegmentPostings` is not empty.
    fn advance_head(&mut self,) {
        {
            let mut mutable_head = self.queue.peek_mut().unwrap();
            let cur_postings = &mut self.postings[mutable_head.ord as usize];
            if cur_postings.advance() {
                mutable_head.doc = cur_postings.doc(); 
                return;
            }
            
        }
        self.queue.pop();
    }
}

impl<TScorer: Scorer> DocSet for BooleanScorer<TScorer> {
    fn advance(&mut self,) -> bool {
        loop {
            self.score_combiner.clear();
            let mut ord_bitset = 0u64;
            match self.queue.peek() {
                Some(heap_item) => {
                    let ord = heap_item.ord as usize;
                    self.doc = heap_item.doc;
                    let score = self.postings[ord].score();
                    self.score_combiner.update(score);
                    ord_bitset |= 1 << ord;  
                }
                None => {
                    return false;
                }
            }
            self.advance_head();
            while let Some(&HeapItem {doc, ord}) = self.queue.peek() {
                if doc == self.doc {
                    let ord = ord as usize;
                    let score = self.postings[ord].score();
                    self.score_combiner.update(score);
                    ord_bitset |= 1 << ord;
                }
                else  {
                    break;
                }
                self.advance_head();
            } 
            if self.filter.accept(ord_bitset) {
                return true;
            }
        }
    }   
            
    fn doc(&self,) -> DocId {
        self.doc
    }
}

impl<TScorer: Scorer> Scorer for BooleanScorer<TScorer> {
    
    fn score(&self,) -> f32 {
        self.score_combiner.score()
    }
}

