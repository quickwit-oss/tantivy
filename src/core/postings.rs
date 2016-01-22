use std::fmt;
use std::fmt::{Debug, Formatter};
use std::io::prelude::Read;
use core::global::DocId;
use std::vec;


////////////////////////////////////


pub trait Postings {
	type IteratorType: Iterator<Item=DocId>;
	fn iter(&self) -> Self::IteratorType;
}



#[derive(Clone)]
pub struct SimplePostings<R: Read + Clone> {
	reader: R,
}

pub struct SimplePostingsIterator<R: Read> {
	reader: R
}

impl<R: Read + Clone> Postings for SimplePostings<R> {

	type IteratorType = SimplePostingsIterator<R>;

	fn iter(&self) -> Self::IteratorType {
		SimplePostingsIterator {
			reader: self.reader.clone()
		}
	}
}


impl<R: Read> Iterator for SimplePostingsIterator<R> {

	type Item=DocId;

	fn next(&mut self) -> Option<DocId> {
		let mut buf: [u8; 8] = [0; 8];
		match self.reader.read(&mut buf) {
			Ok(num_bytes) => {
				if num_bytes == 8 {
					unsafe {
						let val = *(*buf.as_ptr() as *const u32);
						return Some(val)
					}
				}
				else {
					return None
				}
			},
			Err(_) => None
		}
	}
}


impl<R: Read + Clone> Debug for SimplePostings<R> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        let posting_lists: Vec<DocId> = self.iter().collect();
        write!(f, "Postings({:?})", posting_lists);
        Ok(())
    }
}

pub struct IntersectionPostings<'a, LeftPostingsType, RightPostingsType>
where LeftPostingsType: Postings + 'static,
      RightPostingsType: Postings + 'static
{
    left: &'a LeftPostingsType,
    right: &'a RightPostingsType,
}

impl<'a, LeftPostingsType, RightPostingsType> Postings for IntersectionPostings<'a, LeftPostingsType, RightPostingsType>
where LeftPostingsType: Postings + 'static,
      RightPostingsType: Postings + 'static {

    type IteratorType = IntersectionIterator<LeftPostingsType, RightPostingsType>;

    fn iter(&self) -> IntersectionIterator<LeftPostingsType, RightPostingsType> {
        let mut left_it = self.left.iter();
        let mut right_it = self.right.iter();
        let next_left = left_it.next();
        let next_right = right_it.next();
        IntersectionIterator {
            left: left_it,
            right: right_it,
            next_left: next_left,
            next_right: next_right,
        }
    }

}
pub fn intersection<'a, LeftPostingsType, RightPostingsType> (left: &'a LeftPostingsType, right: &'a RightPostingsType) -> IntersectionPostings<'a, LeftPostingsType, RightPostingsType>
where LeftPostingsType: Postings + 'static,
      RightPostingsType: Postings + 'static  {
	IntersectionPostings {
		left: left,
		right: right
	}
}


pub struct IntersectionIterator<LeftPostingsType: Postings, RightPostingsType: Postings> {
    left: LeftPostingsType::IteratorType,
    right: RightPostingsType::IteratorType,

    next_left: Option<DocId>,
    next_right: Option<DocId>,
}

impl<LeftPostingsType: Postings, RightPostingsType: Postings>
Iterator for IntersectionIterator<LeftPostingsType, RightPostingsType> {

    type Item = DocId;

    fn next(&mut self,) -> Option<DocId> {
        loop {
            match (self.next_left, self.next_right) {
                (_, None) => {
                    return None;
                },
                (None, _) => {
                    return None;
                },
                (Some(left_val), Some(right_val)) => {
                    if left_val < right_val {
                        self.next_left = self.left.next();
                    }
                    else if right_val > right_val {
                        self.next_right = self.right.next();
                    }
                    else {
                        self.next_left = self.left.next();
                        self.next_right = self.right.next();
                        return Some(left_val)
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct VecPostings {
    postings: Vec<DocId>,
}

impl VecPostings {
    pub fn new(vals: Vec<DocId>) -> VecPostings {
        VecPostings {
            postings: vals
        }
    }
}

impl Postings for VecPostings {
    type IteratorType = vec::IntoIter<DocId>;

    fn iter(&self) -> vec::IntoIter<DocId> {
        self.postings.clone().into_iter()

    }
}

impl<'a, L: Postings + 'static, R: Postings + 'static> Debug for IntersectionPostings<'a, L, R> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        let posting_lists: Vec<DocId> = self.iter().collect();
        write!(f, "Postings({:?})", posting_lists);
        Ok(())
    }
}
