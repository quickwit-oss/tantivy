use DocId;
use postings::{Postings, DocSet};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use query::MultiTermScorer;
use postings::ScoredDocSet;
use query::Scorer;
use fastfield::U32FastFieldReader;


#[derive(Eq, PartialEq)]
struct HeapItem(DocId, usize, u32);

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other:&Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other:&Self) -> Ordering {
         (self.0, self.1).cmp(&(other.0, other.1)).reverse()
    }
}

pub struct UnionPostings<TPostings: Postings> {
    fieldnorms_readers: Vec<U32FastFieldReader>,
    postings: Vec<TPostings>,
    queue: BinaryHeap<HeapItem>,
    doc: DocId,
    scorer: MultiTermScorer
}

impl<TPostings: Postings> UnionPostings<TPostings> {
    
    
    pub fn new(fieldnorms_reader: Vec<U32FastFieldReader>, postings: Vec<TPostings>, multi_term_scorer: MultiTermScorer) -> UnionPostings<TPostings> {
        let num_postings = postings.len();
        assert_eq!(fieldnorms_reader.len(), num_postings);
        let mut union_postings = UnionPostings {
            fieldnorms_readers: fieldnorms_reader,
            postings: postings,
            queue: BinaryHeap::new(),
            doc: 0,
            scorer: multi_term_scorer
        };
        for ord in 0..num_postings {
            union_postings.enqueue(ord);
        }
        union_postings
    }

    fn enqueue(&mut self, ord: usize) {
        let cur_postings = &mut self.postings[ord];
        if cur_postings.next() {
            let doc = cur_postings.doc();
            let tf = cur_postings.term_freq();
            self.queue.push(HeapItem(doc, ord, tf));
        }
    }
    
    fn get_field_norm(&self, ord:usize, doc:DocId) -> u32 {
        self.fieldnorms_readers[ord].get(doc)
    }

}

impl<TPostings: Postings> DocSet for UnionPostings<TPostings> {
    

    
    fn next(&mut self,) -> bool {
        self.scorer.clear();
        let head = self.queue.pop(); 
        match head {
            Some(HeapItem(doc, ord, tf)) => {
                // let fieldnorm = self.get_field_norm(ord, doc);
                let fieldnorm: u32 = 1u32;
                self.scorer.update(ord, tf, fieldnorm);
                self.enqueue(ord);
                self.doc = doc;
                loop {
                    match self.queue.peek() {
                        Some(&HeapItem(peek_doc, _, _))  => {
                            if peek_doc != doc {
                                break;
                            }
                        }
                        None => { break; }   
                    }
                    let HeapItem(_, peek_ord, peek_tf) = self.queue.pop().unwrap();
                    let fieldnorm = self.get_field_norm(peek_ord, doc);
                    self.scorer.update(peek_ord, peek_tf, fieldnorm);
                    self.enqueue(peek_ord);
                }
                return true;
            }
            None => {
                return false;
            }
        }
    }


    // TODO implement a faster skip_next
        
    fn doc(&self,) -> DocId {
        self.doc
    }
        
    fn doc_freq(&self,) -> usize {
        panic!("Doc freq");
    }
}

impl<TPostings: Postings> ScoredDocSet for UnionPostings<TPostings> {
    fn score(&self,) -> f32 {
        self.scorer.score()
    }
}

#[cfg(test)]
mod tests {
    
    use super::*;
    use postings::{DocSet, VecPostings, ScoredDocSet};
    use query::MultiTermScorer;
    use directory::ReadOnlySource;
    use directory::SharedVec;
    use schema::Field;
    use fastfield::{U32FastFieldReader, U32FastFieldWriter, FastFieldSerializer};

    
    pub fn create_u32_fastfieldreader(field: Field, vals: Vec<u32>) -> U32FastFieldReader {
        let mut u32_field_writer = U32FastFieldWriter::new(field);
        for val in vals {
            u32_field_writer.add_val(val);
        }
        let data = SharedVec::new();
        let write: Box<SharedVec> = Box::new(data.clone());
        let mut serializer = FastFieldSerializer::new(write).unwrap();
        u32_field_writer.serialize(&mut serializer).unwrap();
        serializer.close().unwrap();
        U32FastFieldReader::open(ReadOnlySource::Anonymous(data.copy_vec())).unwrap()
    }
    
    fn abs_diff(left: f32, right: f32) -> f32 {
        (right - left).abs()
    }   
       
    #[test]
    pub fn test_union_postings() {
        let left_fieldnorms = create_u32_fastfieldreader(Field(1), vec!(100,200,300));
        let right_fieldnorms = create_u32_fastfieldreader(Field(2), vec!(15,25,35));   
        let left = VecPostings::new(vec!(1, 2, 3));
        let right = VecPostings::new(vec!(1, 3, 8));
        let multi_term_scorer = MultiTermScorer::new(vec!(1f32, 2f32), vec!(1f32, 4f32));
        let mut union = UnionPostings::new(
            vec!(left_fieldnorms, right_fieldnorms),
            vec!(left, right),
            multi_term_scorer
        );
        assert!(union.next());
        assert_eq!(union.doc(), 1);
        assert!(abs_diff(union.score(), 2.182179f32) < 0.001);
        assert!(union.next());
        assert_eq!(union.doc(), 2);
        assert!(abs_diff(union.score(), 0.2236068) < 0.001f32);
        assert!(union.next());
        assert_eq!(union.doc(), 3);
        assert!(union.next());
        assert!(abs_diff(union.score(), 0.8944272f32) < 0.001f32);
        assert_eq!(union.doc(), 8);
        assert!(!union.next());
    }

}

