use core::schema::DocId;
use std::ptr;
use std::collections::BTreeMap;
use core::schema::Term;
use core::fstmap::FstMapBuilder;
use core::index::Segment;
use core::directory::WritePtr;
use core::index::SegmentComponent;
use core::simdcompression;
use core::serialize::BinarySerializable;
use std::io::{Read, Write};
use std::io;
use std::collections::HashMap;

#[derive(Debug,Ord,PartialOrd,Eq,PartialEq,Clone)]
pub struct TermInfo {
    pub doc_freq: u32,
    pub postings_offset: u32,
}

impl BinarySerializable for TermInfo {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        Ok(
            try!(self.doc_freq.serialize(writer)) +
            try!(self.postings_offset.serialize(writer))
        )
    }
    fn deserialize(reader: &mut Read) -> io::Result<Self> {
        let doc_freq = try!(u32::deserialize(reader));
        let offset = try!(u32::deserialize(reader));
        Ok(TermInfo {
            doc_freq: doc_freq,
            postings_offset: offset,
        })
    }
}


pub struct PostingsWriter {
    postings: Vec<Vec<DocId>>,
    term_index: HashMap<Term, usize>,
}

impl PostingsWriter {

    pub fn new() -> PostingsWriter {
        PostingsWriter {
            postings: Vec::new(),
            term_index: HashMap::new(),
        }
    }

    pub fn suscribe(&mut self, doc: DocId, term: Term) {
        let doc_ids: &mut Vec<DocId> = self.get_term_postings(term);
        if doc_ids.len() == 0 || doc_ids[doc_ids.len() - 1] < doc {
			doc_ids.push(doc);
		}
    }

    fn get_term_postings(&mut self, term: Term) -> &mut Vec<DocId> {
        match self.term_index.get(&term) {
            Some(unord_id) => {
                return &mut self.postings[*unord_id];
            },
            None => {}
        }
        let unord_id = self.term_index.len();
        self.postings.push(Vec::new());
        self.term_index.insert(term, unord_id.clone());
        &mut self.postings[unord_id]
    }

    pub fn serialize(&self, serializer: &mut PostingsSerializer) -> io::Result<()> {
        let mut sorted_terms: Vec<(&Term, &usize)> = self.term_index.iter().collect();
        sorted_terms.sort();
        for (term, postings_id) in sorted_terms.into_iter() {
            let doc_ids = &self.postings[postings_id.clone()];
            let term_docfreq = doc_ids.len() as u32;
            try!(serializer.new_term(&term, term_docfreq));
            try!(serializer.write_docs(&doc_ids));
        }
        Ok(())
    }


}


//////////////////////////////////

pub trait Postings: Iterator<Item=DocId> {
    // after skipping position
    // the iterator in such a way that the
    // next call to next() will return a
    // value greater or equal to target.
    fn skip_next(&mut self, target: DocId) -> Option<DocId>;
}

pub struct IntersectionPostings<T: Postings> {
    postings: Vec<T>,
}

impl<T: Postings> IntersectionPostings<T> {
    pub fn from_postings(postings: Vec<T>) -> IntersectionPostings<T> {
        IntersectionPostings {
            postings: postings,
        }
    }
}

impl<T: Postings> Iterator for IntersectionPostings<T> {
    type Item = DocId;
    fn next(&mut self,) -> Option<DocId> {
        let mut candidate;
        match self.postings[0].next() {
            Some(val) => {
                candidate = val;
            },
            None => {
                return None;
            }
        }
        'outer: loop {
            for i in 1..self.postings.len() {
                let skip_result = self.postings[i].skip_next(candidate);
                match skip_result {
                    None => {
                        return None;
                    },
                    Some(x) if x == candidate => {
                    },
                    Some(greater) => {
                        unsafe {
                            let pa: *mut T = &mut self.postings[i];
                            let pb: *mut T = &mut self.postings[0];
                            ptr::swap(pa, pb);
                        }
                        candidate = greater;
                        continue 'outer;
                    },
                }
            }
            return Some(candidate);
        }

    }
}

pub struct PostingsSerializer {
    terms_fst_builder: FstMapBuilder<WritePtr, TermInfo>, // TODO find an alternative to work around the "move"
    postings_write: WritePtr,
    written_bytes_postings: usize,
    encoder: simdcompression::Encoder,
}

impl PostingsSerializer {

    pub fn open(segment: &Segment) -> io::Result<PostingsSerializer> {
        let terms_write = try!(segment.open_write(SegmentComponent::TERMS));
        let terms_fst_builder = try!(FstMapBuilder::new(terms_write));
        let postings_write = try!(segment.open_write(SegmentComponent::POSTINGS));
        Ok(PostingsSerializer {
            terms_fst_builder: terms_fst_builder,
            postings_write: postings_write,
            written_bytes_postings: 0,
            encoder: simdcompression::Encoder::new(),
        })
    }

    pub fn new_term(&mut self, term: &Term, doc_freq: DocId) -> io::Result<()> {
        let term_info = TermInfo {
            doc_freq: doc_freq,
            postings_offset: self.written_bytes_postings as u32,
        };
        self.terms_fst_builder
            .insert(term.as_slice(), &term_info)
    }

    pub fn write_docs(&mut self, doc_ids: &[DocId]) -> io::Result<()> {
        let docs_data = self.encoder.encode_sorted(doc_ids);
        self.written_bytes_postings += try!((docs_data.len() as u32).serialize(&mut self.postings_write));
        for num in docs_data {
            self.written_bytes_postings += try!(num.serialize(&mut self.postings_write));
        }
        Ok(())
    }

    pub fn close(mut self,) -> io::Result<()> {
        try!(self.terms_fst_builder.finish());
        try!(self.postings_write.flush());
        Ok(())
    }
}



#[cfg(test)]
mod tests {

    use super::*;
    use test::Bencher;
    use core::schema::DocId;


    #[derive(Debug)]
    pub struct VecPostings {
        doc_ids: Vec<DocId>,
    	cursor: usize,
    }

    impl VecPostings {
        pub fn new(vals: Vec<DocId>) -> VecPostings {
            VecPostings {
                doc_ids: vals,
    			cursor: 0,
            }
        }
    }

    impl Postings for VecPostings {
        // after skipping position
        // the iterator in such a way that the
        // next call to next() will return a
        // value greater or equal to target.
        fn skip_next(&mut self, target: DocId) -> Option<DocId> {
            loop {
                match Iterator::next(self) {
                    Some(val) if val >= target => {
                        return Some(val);
                    },
                    None => {
                        return None;
                    },
                    _ => {}
                }
            }
        }
    }

    impl Iterator for VecPostings {
    	type Item = DocId;
    	fn next(&mut self,) -> Option<DocId> {
    		if self.cursor >= self.doc_ids.len() {
    			None
    		}
    		else {
                self.cursor += 1;
    			Some(self.doc_ids[self.cursor - 1])
    		}
    	}
    }

    #[test]
    fn test_intersection() {
        {
            let left = VecPostings::new(vec!(1, 3, 9));
            let right = VecPostings::new(vec!(3, 4, 9, 18));
            let inter = IntersectionPostings::from_postings(vec!(left, right));
            let vals: Vec<DocId> = inter.collect();
            assert_eq!(vals, vec!(3, 9));
        }
        {
            let a = VecPostings::new(vec!(1, 3, 9));
            let b = VecPostings::new(vec!(3, 4, 9, 18));
            let c = VecPostings::new(vec!(1, 5, 9, 111));
            let inter = IntersectionPostings::from_postings(vec!(a, b, c));
            let vals: Vec<DocId> = inter.collect();
            assert_eq!(vals, vec!(9));
        }
    }

    #[bench]
    fn bench_single_intersection(b: &mut Bencher) {
        b.iter(|| {
            let docs = VecPostings::new((0..1_000_000).collect());
            let intersection = IntersectionPostings::from_postings(vec!(docs));
            intersection.count()
        });
    }
}
