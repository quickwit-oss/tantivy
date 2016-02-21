use core::directory::Directory;
use core::directory::{Segment, SegmentId};
use std::collections::BinaryHeap;
use core::schema::Term;
use core::store::StoreReader;
use core::schema::Document;
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
use core::simdcompression::Decoder;

// TODO file structure should be in codec

pub struct SegmentReader {
    segment: Segment,
    term_offsets: fst::Map,
    postings_data: MmapReadOnly,
    store_reader: StoreReader,
}

pub struct SegmentPostings {
    doc_id: usize,
    doc_ids: Vec<u32>,
}

impl SegmentPostings {

    pub fn empty()-> SegmentPostings {
        SegmentPostings {
            doc_id: 0,
            doc_ids: Vec::new(),
        }
    }

    pub fn from_data(data: &[u8]) -> SegmentPostings {
        let mut cursor = Cursor::new(data);
        let doc_freq = cursor.read_u32::<BigEndian>().unwrap() as usize;
        let data_size = cursor.read_u32::<BigEndian>().unwrap() as usize;
        // TODO remove allocs
        let mut data = Vec::with_capacity(data_size);
        for _ in 0..data_size {
            data.push(cursor.read_u32::<BigEndian>().unwrap());
        }
        let mut doc_ids: Vec<u32> = (0..doc_freq as u32 ).collect();
        let decoder = Decoder::new();
        decoder.decode(&data, &mut doc_ids);
        SegmentPostings {
            doc_ids: doc_ids,
            doc_id: 0,
        }
    }

}

impl Postings for SegmentPostings {
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


impl Iterator for SegmentPostings {

    type Item = DocId;

    fn next(&mut self,) -> Option<DocId> {
        if self.doc_id < self.doc_ids.len() {
            let res = Some(self.doc_ids[self.doc_id]);
            self.doc_id += 1;
            return res;
        }
        else {
            None
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
        let store_reader = StoreReader::new(try!(segment.mmap(SegmentComponent::STORE)));
        let postings_shared_mmap = try!(segment.mmap(SegmentComponent::POSTINGS));
        Ok(SegmentReader {
            postings_data: postings_shared_mmap,
            term_offsets: term_offsets,
            segment: segment,
            store_reader: store_reader,
        })
    }

    pub fn get_doc(&self, doc_id: &DocId) -> Document {
        self.store_reader.get(doc_id)
    }

    pub fn read_postings(&self, offset: usize) -> SegmentPostings {
        let postings_data = unsafe {&self.postings_data.as_slice()[offset..]};
        SegmentPostings::from_data(&postings_data)
    }

    pub fn get_term<'a>(&'a self, term: &Term) -> Option<SegmentPostings> {
        self.term_offsets
            .get(term.as_slice())
            .map(|offset| self.read_postings(offset as usize))
    }

    pub fn search(&self, terms: &Vec<Term>) -> IntersectionPostings<SegmentPostings> {

        let mut segment_postings: Vec<SegmentPostings> = Vec::new();
        for term in terms.iter() {
            match self.get_term(term) {
                Some(segment_posting) => {
                    println!("term found {:?}", term);
                    segment_postings.push(segment_posting);
                }
                None => {
                    println!("not found {:?}", term);
                    segment_postings.clear();
                    segment_postings.push(SegmentPostings::empty());
                    break;
                }
            }
        }
        IntersectionPostings::from_postings(segment_postings)
    }

}


// fn write_postings<R: io::Read, Output, SegSer: SegmentSerializer<Output>>(mut cursor: R, num_docs: DocId, serializer: &mut SegSer) -> Result<()> {
//     // TODO remove allocation
//     let docs = Vec::with_capacity(num_docs);
//     for i in 0..num_docs {
//         let doc_id = cursor.read_u32::<BigEndian>().unwrap();
//         try!(serializer.add_doc(doc_id));
//     }
//     Ok(())
// }
//
// impl SerializableSegment for SegmentReader {
//
//     fn write<Output, SegSer: SegmentSerializer<Output>>(&self, mut serializer: SegSer) -> Result<Output> {
//         let mut term_offsets_it = self.term_offsets.stream();
//         loop {
//             match term_offsets_it.next() {
//                 Some((term_data, offset_u64)) => {
//                     let term = Term::from(term_data);
//                     let offset = offset_u64 as usize;
//                     let data = unsafe { &self.postings_data.as_slice()[offset..] };
//                     let mut cursor = Cursor::new(data);
//                     let num_docs = cursor.read_u32::<BigEndian>().unwrap() as DocId;
//                     try!(serializer.new_term(&term, num_docs));
//                     try!(write_postings(cursor, num_docs, &mut serializer));
//                 },
//                 None => { break; }
//             }
//         }
//         serializer.close()
//     }
//
// }
