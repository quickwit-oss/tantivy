use DocId;
use postings::{Postings, DocSet};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use query::MultiTermAccumulator;
use query::Similarity; 
use fastfield::U32FastFieldReader;
use query::Occur;
use std::iter;
use super::Scorer;
use Score;

/// Each `HeapItem` represents the head of
/// a segment postings being merged.
///
/// Heap(doc_id, segment_ordinal)
/// * doc_id - is the current doc id for the given segment postings 
/// * segment_ordinal - is the ordinal used to identify to which segment postings
/// this heap item belong to.
#[derive(Eq, PartialEq)]
struct HeapItem(DocId, u32);

/// HeapItem are ordered by the document
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other:&Self) -> Ordering {
         (other.0).cmp(&self.0)
    }
}

struct Filter {
    and_mask: u64,
    result: u64,    
}

impl Filter {
    fn accept(&self, ord_set: u64) -> bool {
        (self.and_mask & ord_set) == self.result
    }
    
    fn new(occurs: &[Occur]) -> Filter {
        let mut and_mask = 0u64;
        let mut result = 0u64;
        for (i, occur) in occurs.iter().enumerate() {
            let shift = 1 << i;
            match *occur {
                Occur::Must => {
                    and_mask |= shift;
                    result |= shift;
                },
                Occur::MustNot => {
                    and_mask |= shift;
                },
                Occur::Should => {},
            }
        }
        Filter {
            and_mask: and_mask,
            result: result
        }
    }
}

/// Document-At-A-Time multi term scorer.
///
/// The scorer merges multiple segment postings and pushes
/// term information to the score accumulator. 
pub struct DAATMultiTermScorer<TPostings: Postings, TAccumulator: MultiTermAccumulator> {
    fieldnorm_readers: Vec<U32FastFieldReader>,
    postings: Vec<TPostings>,
    term_frequencies: Vec<u32>,
    queue: BinaryHeap<HeapItem>,
    doc: DocId,
    similarity: TAccumulator,
    filter: Filter,
}

impl<TPostings: Postings, TAccumulator: MultiTermAccumulator> DAATMultiTermScorer<TPostings, TAccumulator> {
    
    fn new_non_empty(
        
        fieldnorm_readers: Vec<U32FastFieldReader>,
        postings: Vec<TPostings>,
        similarity: TAccumulator,
        filter: Filter
    ) -> DAATMultiTermScorer<TPostings, TAccumulator> {
        let mut term_frequencies: Vec<u32> = iter::repeat(0u32).take(postings.len()).collect();
        let heap_items: Vec<HeapItem> = postings
            .iter()
            .map(|posting| {
                (posting.doc(), posting.term_freq())
            })
            .enumerate()
            .map(|(ord, (doc, tf))| {
                term_frequencies[ord] = tf;
                HeapItem(doc, ord as u32)
            })
            .collect();
        DAATMultiTermScorer {
            fieldnorm_readers: fieldnorm_readers,
            postings: postings,
            term_frequencies: term_frequencies,
            queue: BinaryHeap::from(heap_items),
            doc: 0,
            similarity: similarity,
            filter: filter
        }
    }
    
    /// Constructor
    pub fn new(postings_and_fieldnorms: Vec<(Occur, TPostings, U32FastFieldReader)>, similarity: TAccumulator) -> DAATMultiTermScorer<TPostings, TAccumulator> {      
        let mut postings = Vec::new();
        let mut fieldnorm_readers = Vec::new();
        let mut occurs = Vec::new();
        for (occur, mut posting, fieldnorm_reader) in postings_and_fieldnorms {
            if posting.advance() {
                postings.push(posting);
                fieldnorm_readers.push(fieldnorm_reader);
                occurs.push(occur);
            }
        }
        let filter = Filter::new(&occurs);
        DAATMultiTermScorer::new_non_empty(fieldnorm_readers, postings, similarity, filter)
    }

    /// Returns the scorer
    pub fn scorer(&self,) -> &TAccumulator {
        &self.similarity
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
        let ord = self.queue.peek().unwrap().1 as usize;
        let cur_postings = &mut self.postings[ord];
        if cur_postings.advance() {
            let doc = cur_postings.doc();
            self.term_frequencies[ord] = cur_postings.term_freq();
            // peek & replace is incredibly 
            self.queue.replace(HeapItem(doc, ord as u32));
        }
        else {
            self.queue.pop();
        }
    }
    
    /// Returns the field norm for the segment postings with the given ordinal,
    /// and the given document. 
    fn get_field_norm(&self, ord:usize, doc:DocId) -> u32 {
        self.fieldnorm_readers[ord].get(doc)
    }

}

impl<TPostings: Postings, TSimilarity: Similarity> Scorer for DAATMultiTermScorer<TPostings, TSimilarity> {
    fn score(&self,) -> Score {
        self.similarity.score()
    }
} 

impl<TPostings: Postings, TAccumulator: MultiTermAccumulator> DocSet for DAATMultiTermScorer<TPostings, TAccumulator> {
    
    fn advance(&mut self,) -> bool {
        loop {
            self.similarity.clear();
            let mut ord_bitset = 0u64;
            match self.queue.peek() {
                Some(&HeapItem(doc, ord)) => {
                    self.doc = doc;
                    let ord: usize = ord as usize;
                    let fieldnorm = self.get_field_norm(ord, doc);
                    let tf = self.term_frequencies[ord];
                    self.similarity.update(ord, tf, fieldnorm);
                    ord_bitset |= 1 << ord;  
                }
                None => {
                    return false;
                }
            }
            self.advance_head();
            while let Some(&HeapItem(peek_doc, peek_ord)) = self.queue.peek() {
                if peek_doc == self.doc {
                    let peek_ord: usize = peek_ord as usize;
                    let peek_tf = self.term_frequencies[peek_ord];
                    let peek_fieldnorm = self.get_field_norm(peek_ord, peek_doc);
                    self.similarity.update(peek_ord, peek_tf, peek_fieldnorm);
                    ord_bitset |= 1 << peek_ord;
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

#[cfg(test)]
mod tests {
    
    use super::*;
    use postings::{DocSet, VecPostings};
    use query::TfIdf;
    use query::Scorer;
    use directory::Directory;
    use directory::RAMDirectory;
    use schema::Field;
    use std::path::Path;
    use query::Occur;
    use fastfield::{U32FastFieldReader, U32FastFieldWriter, FastFieldSerializer};

    
    pub fn create_u32_fastfieldreader(field: Field, vals: Vec<u32>) -> U32FastFieldReader {
        let mut u32_field_writer = U32FastFieldWriter::new(field);
        for val in vals {
            u32_field_writer.add_val(val);
        }
        let path = Path::new("some_path");
        let mut directory = RAMDirectory::create();
        {
            let write = directory.open_write(&path).unwrap();
            let mut serializer = FastFieldSerializer::new(write).unwrap();
            u32_field_writer.serialize(&mut serializer).unwrap();
            serializer.close().unwrap();
        }
        let read = directory.open_read(&path).unwrap();
        U32FastFieldReader::open(read).unwrap()
    }
    
    fn abs_diff(left: f32, right: f32) -> f32 {
        (right - left).abs()
    }   
       
    #[test]
    pub fn test_daat_scorer() {
        let left_fieldnorms = create_u32_fastfieldreader(Field(1), vec!(100,200,300));
        let right_fieldnorms = create_u32_fastfieldreader(Field(2), vec!(15,25,35));   
        let left = VecPostings::from(vec!(1, 2, 3));
        let right = VecPostings::from(vec!(1, 3, 8));
        let tfidf = TfIdf::new(vec!(0f32, 1f32, 2f32), vec!(1f32, 4f32));
        let mut daat_scorer = DAATMultiTermScorer::new(
            vec!(
                (Occur::Should, left, left_fieldnorms),
                (Occur::Should, right, right_fieldnorms),
            ),
            tfidf
        );
        assert_eq!(daat_scorer.next(), Some(1u32));
        assert!(abs_diff(daat_scorer.score(), 2.182179f32) < 0.001);
        assert_eq!(daat_scorer.next(), Some(2u32));
        assert!(abs_diff(daat_scorer.score(), 0.2236068) < 0.001f32);
        assert_eq!(daat_scorer.next(), Some(3u32));
        assert_eq!(daat_scorer.next(), Some(8u32));
        assert!(abs_diff(daat_scorer.score(), 0.8944272f32) < 0.001f32);
        assert!(!daat_scorer.advance());
    }

}

