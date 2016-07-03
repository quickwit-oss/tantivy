
use DocId;
use postings::Postings;
use std::collections::BinaryHeap;
use postings::SkipResult;
use std::cmp::Ordering;
use std::ops::Index;

#[derive(Eq, PartialEq)]
struct HeapItem(DocId, usize);

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other:&Self) -> Option<Ordering> {
         (self.0, self.1).partial_cmp(&(other.0, other.1)).map(|o| o.reverse())
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other:&Self) -> Ordering {
         (self.0, self.1).cmp(&(other.0, other.1)).reverse()
    }
}

pub struct UnionPostings<TPostings: Postings> {
    postings: Vec<TPostings>,
    queue: BinaryHeap<HeapItem>,
    active_posting_ordinals: Vec<usize>,
    doc: DocId,
}

impl<TPostings: Postings> From<Vec<TPostings>> for UnionPostings<TPostings> {
    fn from(postings: Vec<TPostings>) -> UnionPostings<TPostings> {
        let num_postings = postings.len();
        let active_posting_ordinals: Vec<usize> = (0..num_postings).into_iter().collect();
        UnionPostings {
            postings: postings,
            queue: BinaryHeap::new(),
            active_posting_ordinals: active_posting_ordinals,
            doc: 0,
        }
    }
}

impl<TPostings: Postings> UnionPostings<TPostings> {
    pub fn active_posting_ordinals(&self,) -> &[usize] {
        &self.active_posting_ordinals
    }
}

impl<TPostings: Postings> Index<usize> for UnionPostings<TPostings> {
    type Output = TPostings;
    fn index(&self, index: usize) -> &TPostings {
        &self.postings[index]
    }

}

impl<TPostings: Postings> Postings for UnionPostings<TPostings> {
    
    fn next(&mut self,) -> bool {
        if self.active_posting_ordinals.is_empty() {
            return false;
        }
        for &ord in self.active_posting_ordinals.iter() {
            if self.postings[ord].next() {
                let doc = self.postings[ord].doc();
                self.queue.push(HeapItem(doc, ord));
            }
        }
        self.active_posting_ordinals.clear();
        let head = self.queue.pop(); 
        match head {
            Some(HeapItem(doc, ord)) => {
                self.active_posting_ordinals.push(ord);
                self.doc = doc;
                loop {
                    {
                        let peek = self.queue.peek();
                        match peek {
                            Some(&HeapItem(peek_doc, _))  => {
                                if peek_doc != doc {
                                    break;
                                }
                            }
                            None => { break; }   
                        }
                    }
                    let HeapItem(_, peek_ord) = self.queue.pop().unwrap();
                    self.active_posting_ordinals.push(peek_ord);
                }
            }
            None => {
                return false;
            }
        }
        return true;
    }
       
    fn skip_next(&mut self, _: DocId) -> SkipResult {
        SkipResult::End
    }
    
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
    use postings::VecPostings;
    use postings::Postings;
    
    #[test]
    pub fn test_union_postings() {
        let left = VecPostings::new(vec!(1, 2, 3));
        let right = VecPostings::new(vec!(1, 3, 8));
        let mut union = UnionPostings::from(vec!(left, right));
        assert!(union.next());
        assert_eq!(union.doc(), 1);
        assert_eq!(union.active_posting_ordinals(), [0, 1]);
        assert!(union.next());
        assert_eq!(union.doc(), 2);
        assert_eq!(union.active_posting_ordinals(), [0]);
        assert!(union.next());
        assert_eq!(union.doc(), 3);
        assert_eq!(union.active_posting_ordinals(), [0, 1]);
        assert!(union.next());
        assert_eq!(union.doc(), 8);
        assert_eq!(union.active_posting_ordinals(), [1]);
        assert!(!union.next());
    }

}

