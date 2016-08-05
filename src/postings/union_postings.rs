use DocId;
use postings::{Postings, DocSet};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use query::MultiTermAccumulator; 
use fastfield::U32FastFieldReader;
use std::iter;

#[derive(Eq, PartialEq)]
struct HeapItem(DocId, u32);

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other:&Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other:&Self) -> Ordering {
         (other.0).cmp(&self.0)
    }
}

pub struct UnionPostings<TPostings: Postings, TAccumulator: MultiTermAccumulator> {
    fieldnorms_readers: Vec<U32FastFieldReader>,
    postings: Vec<TPostings>,
    term_frequencies: Vec<u32>,
    queue: BinaryHeap<HeapItem>,
    doc: DocId,
    scorer: TAccumulator
}

impl<TPostings: Postings, TAccumulator: MultiTermAccumulator> UnionPostings<TPostings, TAccumulator> {
        
    pub fn new(fieldnorms_reader: Vec<U32FastFieldReader>, mut postings: Vec<TPostings>, scorer: TAccumulator) -> UnionPostings<TPostings, TAccumulator> {
        let num_postings = postings.len();
        assert_eq!(fieldnorms_reader.len(), num_postings);
        for posting in &mut postings {
            assert!(posting.next());
        }
        let mut term_frequencies: Vec<u32> = iter::repeat(0u32).take(num_postings).collect();
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
        
        UnionPostings {
            fieldnorms_readers: fieldnorms_reader,
            postings: postings,
            term_frequencies: term_frequencies,
            queue: BinaryHeap::from(heap_items),
            doc: 0,
            scorer: scorer
        }
    }


    pub fn scorer(&self,) -> &TAccumulator {
        &self.scorer
    }

    fn advance_head(&mut self,) {
        let ord = self.queue.peek().unwrap().1 as usize;
        let cur_postings = &mut self.postings[ord];
        if cur_postings.next() {
            let doc = cur_postings.doc();
            self.term_frequencies[ord] = cur_postings.term_freq();  
            self.queue.replace(HeapItem(doc, ord as u32));
        }
        else {
            self.queue.pop();
        }
    }
    
    fn get_field_norm(&self, ord:usize, doc:DocId) -> u32 {
        self.fieldnorms_readers[ord].get(doc)
    }

}

impl<TPostings: Postings, TAccumulator: MultiTermAccumulator> DocSet for UnionPostings<TPostings, TAccumulator> {
    
    fn next(&mut self,) -> bool {
        self.scorer.clear();
        match self.queue.peek() {
            Some(&HeapItem(doc, ord)) => {
                self.doc = doc;
                let ord: usize = ord as usize;
                let fieldnorm = self.get_field_norm(ord, doc);
                let tf = self.term_frequencies[ord];
                self.scorer.update(ord, tf, fieldnorm);   
            }
            None => {
                return false;
            }
        }
        self.advance_head();
        loop {
            match self.queue.peek() {
                Some(&HeapItem(peek_doc, peek_ord))  => {
                    if peek_doc != self.doc {
                        break;
                    }
                    else {
                        let peek_ord: usize = peek_ord as usize;
                        let peek_tf = self.term_frequencies[peek_ord];
                        let peek_fieldnorm = self.get_field_norm(peek_ord, peek_doc);
                        self.scorer.update(peek_ord, peek_tf, peek_fieldnorm);
                    }
                }
                None => { break; }   
            }
            self.advance_head();
        }
        return true;
    }

    // TODO implement a faster skip_next
        
    fn doc(&self,) -> DocId {
        self.doc
    }
        
    fn doc_freq(&self,) -> usize {
        panic!("Doc freq");
    }
}

#[cfg(test)]
mod tests {
    
    use super::*;
    use postings::{DocSet, VecPostings};
    use query::TfIdfScorer;
    use query::Scorer;
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
        let left = VecPostings::from(vec!(1, 2, 3));
        let right = VecPostings::from(vec!(1, 3, 8));
        let multi_term_scorer = TfIdfScorer::new(vec!(0f32, 1f32, 2f32), vec!(1f32, 4f32));
        let mut union = UnionPostings::new(
            vec!(left_fieldnorms, right_fieldnorms),
            vec!(left, right),
            multi_term_scorer
        );
        assert!(union.next());
        assert_eq!(union.doc(), 1);
        assert!(abs_diff(union.scorer().score(), 2.182179f32) < 0.001);
        assert!(union.next());
        assert_eq!(union.doc(), 2);
        assert!(abs_diff(union.scorer().score(), 0.2236068) < 0.001f32);
        assert!(union.next());
        assert_eq!(union.doc(), 3);
        assert!(union.next());
        assert!(abs_diff(union.scorer().score(), 0.8944272f32) < 0.001f32);
        assert_eq!(union.doc(), 8);
        assert!(!union.next());
    }

}

