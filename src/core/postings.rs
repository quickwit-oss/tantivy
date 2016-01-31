use std::fmt;
use std::fmt::{Debug, Formatter};
use std::io::prelude::Read;
use core::global::DocId;
use std::cmp::Ordering;
use std::vec;


////////////////////////////////////

pub trait Postings: Iterator<Item=DocId> {
}
impl<T: Iterator<Item=DocId>> Postings for T {}


#[derive(Debug)]
pub struct VecPostings {
    doc_ids: Vec<DocId>,
	cursor: usize,
}

impl VecPostings {
    pub fn new(vals: Vec<DocId>) -> VecPostings {
        VecPostings {
            doc_ids: vals,
			cursor: -1,
        }
    }
}


impl Iterator for VecPostings {
	type Item = DocId;
	fn next(&mut self,) -> Option<DocId> {
		if self.cursor + 1 >= self.doc_ids.len() {
			None
		}
		else {
			self.cursor += 1;
			Some(self.doc_ids[self.cursor])
		}
	}
}


// impl<'a, L: Postings + 'static, R: Postings + 'static> Debug for IntersectionPostings<'a, L, R> {
//     fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
//         write!(f, "Postings({:?})", self.doc_ids);
//         Ok(())
//     }
// }
