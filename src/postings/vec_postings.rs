#![allow(dead_code)]

use DocId;
use postings::{Postings, DocSet, SkipResult};
use std::num::Wrapping;
use std::cmp::Ordering;

pub struct VecPostings {
    doc_ids: Vec<DocId>,
    cursor: Wrapping<usize>,
}

impl From<Vec<DocId>> for VecPostings {
    fn from(doc_ids: Vec<DocId>) -> VecPostings {
        VecPostings {
            doc_ids: doc_ids,
            cursor: Wrapping(usize::max_value()),
        }
    }
}

impl DocSet for VecPostings {
    fn next(&mut self,) -> bool {
        self.cursor += Wrapping(1);
        self.doc_ids.len() > self.cursor.0
    }
    
    fn doc(&self,) -> DocId {
        self.doc_ids[self.cursor.0]
    }

    fn doc_freq(&self,) -> usize {
        self.doc_ids.len()
    }
    
    fn skip_next(&mut self, target: DocId) -> SkipResult {
        let mut start: usize = self.cursor.0;
        match self.doc_ids[start].cmp(&target)  {
            Ordering::Equal => {
                return SkipResult::Reached;
            }
            Ordering::Greater => {
                if self.cursor.0 < self.doc_ids.len() {
                    return SkipResult::OverStep;
                }
                else {
                    return SkipResult::End;
                }
            }
            Ordering::Less => {
                // see below
            }
        }
        
        let mut end = self.doc_ids.len();
        
        while end - start > 1 {
            // find an upper bound
            let mut jump = 1;
            loop {
                let jump_dest = start + jump;
                if jump_dest >= end {
                    // we jump out of bounds
                    break;
                }
                match self.doc_ids[jump_dest].cmp(&target) {
                    Ordering::Less => {
                        // still below the target, let's keep jumping.
                        start = jump_dest;
                        jump *= 2;
                    }
                    Ordering::Equal => {
                        self.cursor = Wrapping(jump_dest);
                        return SkipResult::Reached;
                    }
                    Ordering::Greater => {
                        end = jump_dest;
                        break;
                    }
                }           
            }
        }
        self.cursor = Wrapping(start + 1);
        if self.cursor.0 < self.doc_ids.len() {
            return SkipResult::OverStep;
        }
        else {
            return SkipResult::End;
        }
    }
}

impl Postings for VecPostings {
    fn term_freq(&self,) -> u32 {
        1u32
    }
}

#[cfg(test)]
pub mod tests {
    
    use super::*;
    use DocId;   
    use postings::{Postings, SkipResult, DocSet}; 
    
    
    #[test]
    pub fn test_vec_postings() {
        let doc_ids: Vec<DocId> = (0u32..1024u32).map(|e| e*3).collect();
        let mut postings = VecPostings::from(doc_ids);
        assert!(postings.next());
        assert_eq!(postings.doc(), 0u32);
        assert!(postings.next());
        assert_eq!(postings.doc(), 3u32);
        assert_eq!(postings.term_freq(), 1u32);
        assert_eq!(postings.skip_next(14u32), SkipResult::OverStep);
        assert_eq!(postings.doc(), 15u32);
        assert_eq!(postings.skip_next(300u32), SkipResult::Reached);
        assert_eq!(postings.doc(), 300u32);
        assert_eq!(postings.skip_next(6000u32), SkipResult::End);
    }
}

