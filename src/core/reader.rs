use core::directory::Directory;
use core::directory::{Segment, SegmentId};
use std::collections::BinaryHeap;
use core::schema::Term;
use fst::Streamer;
use fst;
use std::io;
use core::postings::IntersectionPostings;
use fst::raw::Fst;
use std::cmp::{Eq,PartialEq,Ord,PartialOrd,Ordering};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::borrow::Borrow;
use std::io::Cursor;
use core::global::DocId;
use core::serial::*;
use core::directory::SegmentComponent;
use fst::raw::MmapReadOnly;
use core::error::{Result, Error};
use core::postings::Postings;

// TODO file structure should be in codec

pub struct SegmentReader {
    segment: Segment,
    term_offsets: fst::Map,
    postings_data: MmapReadOnly,
}


pub struct SegmentPostings<'a> {
    cursor: Cursor<&'a [u8]>,
    num_docs_remaining: usize,
}

impl<'a> SegmentPostings<'a> {
    pub fn from_data(data: &[u8]) -> SegmentPostings {
        let mut cursor = Cursor::new(data);
        let doc_freq = cursor.read_u32::<BigEndian>().unwrap() as usize;
        SegmentPostings {
            cursor: cursor,
            num_docs_remaining: doc_freq,
        }
    }

}

impl<'a> Postings for SegmentPostings<'a> {
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


impl<'a> Iterator for SegmentPostings<'a> {

    type Item = DocId;

    fn next(&mut self,) -> Option<DocId> {
        if self.num_docs_remaining <= 0 {
            None
        }
        else {
            self.num_docs_remaining -= 1;
            Some(self.cursor.read_u32::<BigEndian>().unwrap() as DocId)
        }
    }
}





impl SegmentReader {

    pub fn id(&self,) -> SegmentId {
        self.segment.id()
    }

    pub fn open(segment: Segment) -> Result<SegmentReader> {
        let term_shared_mmap = try!(segment.mmap(SegmentComponent::TERMS));
        let term_offsets = match fst::Map::from_mmap(term_shared_mmap) {
            Ok(term_offsets) => term_offsets,
            Err(_) => {
                let filepath = segment.relative_path(&SegmentComponent::TERMS);
                return Err(Error::FSTFormat(format!("The file {:?} does not seem to be a valid term to offset transducer.", filepath)));
            }
        };
        let postings_shared_mmap = try!(segment.mmap(SegmentComponent::POSTINGS));
        Ok(SegmentReader {
            postings_data: postings_shared_mmap,
            term_offsets: term_offsets,
            segment: segment,
        })
    }


    pub fn read_postings(&self, offset: usize) -> SegmentPostings {
        let postings_data = unsafe {&self.postings_data.as_slice()[offset..]};
        SegmentPostings::from_data(&postings_data)
    }

    pub fn get_term<'a>(&'a self, term: &Term) -> Option<SegmentPostings<'a>> {
        println!("Term {:?}", term);
        match self.term_offsets.get(term.as_slice()) {
            Some(offset) => {
                println!("offset {}", offset);
                Some(self.read_postings(offset as usize))
            },
            None => None,
        }
    }

    pub fn search<'a>(&'a self, terms: &Vec<Term>) -> IntersectionPostings<SegmentPostings<'a>> {
        let segment_postings: Vec<SegmentPostings> = terms
            .iter()
            .map(|term| self.get_term(term).unwrap())
            .collect();
        IntersectionPostings::from_postings(segment_postings)
    }

}


fn write_postings<R: io::Read, Output, SegSer: SegmentSerializer<Output>>(mut cursor: R, num_docs: DocId, serializer: &mut SegSer) -> Result<()> {
    for i in 0..num_docs {
        let doc_id = cursor.read_u32::<BigEndian>().unwrap();
        try!(serializer.add_doc(doc_id));
    }
    Ok(())
}

impl SerializableSegment for SegmentReader {

    fn write<Output, SegSer: SegmentSerializer<Output>>(&self, mut serializer: SegSer) -> Result<Output> {
        let mut term_offsets_it = self.term_offsets.stream();
        loop {
            match term_offsets_it.next() {
                Some((term_data, offset_u64)) => {
                    let term = Term::from(term_data);
                    let offset = offset_u64 as usize;
                    let data = unsafe { &self.postings_data.as_slice()[offset..] };
                    let mut cursor = Cursor::new(data);
                    let num_docs = cursor.read_u32::<BigEndian>().unwrap() as DocId;
                    try!(serializer.new_term(&term, num_docs));
                    try!(write_postings(cursor, num_docs, &mut serializer));
                },
                None => { break; }
            }
        }
        serializer.close()
    }

}
