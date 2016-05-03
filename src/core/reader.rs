use core::index::{Segment, SegmentId};
use schema::Term;
use store::StoreReader;
use schema::Document;
use directory::ReadOnlySource;
use std::io::Cursor;
use DocId;
use core::index::SegmentComponent;
use std::io;
use std::str;
use postings::TermInfo;
use datastruct::FstMap;
use std::fmt;
use rustc_serialize::json;
use common::VInt;
use core::index::SegmentInfo;
use common::OpenTimer;
use schema::U32Field;
use core::convert_to_ioerror;
use common::BinarySerializable;
use fastfield::{U32FastFieldsReader, U32FastFieldReader};
use compression;
use compression::{Block128Decoder, VIntsDecoder};
use std::mem;

impl fmt::Debug for SegmentReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentReader({:?})", self.segment_id)
    }
}

#[inline(never)]
pub fn intersection(mut postings: Vec<SegmentPostings>) -> SegmentPostings {
    let min_len = postings
        .iter()
        .map(|v| v.len())
        .min()
        .unwrap();
    let buffer: Vec<u32> = postings.pop().unwrap().0;
    let mut output: Vec<u32> = Vec::with_capacity(min_len);
    unsafe { output.set_len(min_len); }
    let mut pair = (output, buffer);
    for posting in postings.iter() {
        pair = (pair.1, pair.0);
        let output_len = compression::intersection(posting.0.as_slice(), pair.0.as_slice(), pair.1.as_mut_slice());
        unsafe { pair.1.set_len(output_len); }
    }
    SegmentPostings(pair.1)
}

pub struct SegmentPostings(Vec<DocId>);

impl IntoIterator for SegmentPostings {
    type Item = DocId;
    type IntoIter = ::std::vec::IntoIter<DocId>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl SegmentPostings {

    pub fn empty()-> SegmentPostings {
        SegmentPostings(Vec::new())
    }

    pub fn len(&self,) -> usize {
        self.0.len()
    }

    pub fn from_data(doc_freq: DocId, data: &[u8]) -> SegmentPostings {
        let mut data_u32: &[u32] = unsafe { mem::transmute(data) };
        let mut doc_ids: Vec<u32> = Vec::with_capacity(doc_freq as usize);
        {
            let mut block_decoder = Block128Decoder::new();
            let num_blocks = doc_freq / (compression::NUM_DOCS_PER_BLOCK as u32);
            for _ in 0..num_blocks {
                let (remaining, uncompressed) = block_decoder.decode_sorted(data_u32);
                doc_ids.extend_from_slice(uncompressed);
                data_u32 = remaining;
            }
            if doc_freq % 128 != 0 {
                let data_u8: &[u8] = unsafe { mem::transmute(data_u32) };
                let mut cursor = Cursor::new(data_u8);
                let vint_len: usize = VInt::deserialize(&mut cursor).unwrap().val() as usize;
                let cursor_pos = cursor.position() as usize;
                let vint_data: &[u32] = unsafe { mem::transmute(&data_u8[cursor_pos..])};
                let mut vints_decoder = VIntsDecoder::new();
                doc_ids.extend_from_slice(vints_decoder.decode_sorted(&vint_data[..vint_len]));
            }
        }
        SegmentPostings(doc_ids)

    }

}
//
// impl Postings for SegmentPostings {
//     fn skip_next(&mut self, target: DocId) -> Option<DocId> {
//         loop {
//             match Iterator::next(self) {
//                 Some(val) if val >= target => {
//                     return Some(val);
//                 },
//                 None => {
//                     return None;
//                 },
//                 _ => {}
//             }
//         }
//     }
// }

//
// impl Iterator for SegmentPostings {
//
//     type Item = DocId;
//
//     fn next(&mut self,) -> Option<DocId> {
//         if self.doc_id < self.doc_ids.len() {
//             let res = Some(self.doc_ids[self.doc_id]);
//             self.doc_id += 1;
//             return res;
//         }
//         else {
//             None
//         }
//     }
// }


pub struct SegmentReader {
    segment_info: SegmentInfo,
    segment_id: SegmentId,
    term_infos: FstMap<TermInfo>,
    postings_data: ReadOnlySource,
    store_reader: StoreReader,
    fast_fields_reader: U32FastFieldsReader,
}

impl SegmentReader {

    /// Returns the highest document id ever attributed in
    /// this segment + 1.
    /// Today, `tantivy` does not handle deletes so, it happens
    /// to also be the number of documents in the index.
    pub fn max_doc(&self,) -> DocId {
        self.segment_info.max_doc
    }

    pub fn get_store_reader(&self,) -> &StoreReader {
        &self.store_reader
    }

    /// Open a new segment for reading.
    pub fn open(segment: Segment) -> io::Result<SegmentReader> {
        let segment_info_reader = try!(segment.open_read(SegmentComponent::INFO));
        let segment_info_data = try!(str::from_utf8(&*segment_info_reader).map_err(convert_to_ioerror));
        let segment_info: SegmentInfo = try!(json::decode(&segment_info_data).map_err(convert_to_ioerror));
        let source = try!(segment.open_read(SegmentComponent::TERMS));
        let term_infos = try!(FstMap::from_source(source));
        let store_reader = StoreReader::new(try!(segment.open_read(SegmentComponent::STORE)));
        let postings_shared_mmap = try!(segment.open_read(SegmentComponent::POSTINGS));
        let fast_field_data =  try!(segment.open_read(SegmentComponent::FASTFIELDS));
        let fast_fields_reader = try!(U32FastFieldsReader::open(fast_field_data));
        Ok(SegmentReader {
            segment_info: segment_info,
            postings_data: postings_shared_mmap,
            term_infos: term_infos,
            segment_id: segment.id(),
            store_reader: store_reader,
            fast_fields_reader: fast_fields_reader,
        })
    }


    pub fn term_infos(&self,) -> &FstMap<TermInfo> {
        &self.term_infos
    }


    /// Returns the document (or to be accurate, its stored field)
    /// bearing the given doc id.
    /// This method is slow and should seldom be called from
    /// within a collector.
    pub fn  doc(&self, doc_id: &DocId) -> io::Result<Document> {
        self.store_reader.get(doc_id)
    }

    pub fn get_fast_field_reader(&self, u32_field: &U32Field) -> io::Result<U32FastFieldReader> {
        self.fast_fields_reader.get_field(u32_field)
    }

    pub fn read_postings(&self, term_info: &TermInfo) -> SegmentPostings {
        let offset = term_info.postings_offset as usize;
        let postings_data = &self.postings_data.as_slice()[offset..];
        SegmentPostings::from_data(term_info.doc_freq, &postings_data)
    }

    fn get_term<'a>(&'a self, term: &Term) -> Option<TermInfo> {
        self.term_infos.get(term.as_slice())
    }

    /// Returns the list of doc ids containing all of the
    /// given terms.
    pub fn search<'a>(&self, terms: &Vec<Term>, mut timer: OpenTimer<'a>) -> SegmentPostings {
        if terms.len() == 1 {
            match self.get_term(&terms[0]) {
                Some(term_info) => {
                    self.read_postings(&term_info)
                }
                None => {
                    SegmentPostings::empty()
                }
            }
        }
        else {
            let mut segment_postings: Vec<SegmentPostings> = Vec::new();
            {
                let mut decode_timer = timer.open("decode_all");
                for term in terms.iter() {
                    match self.get_term(term) {
                        Some(term_info) => {
                            let _decode_one_timer = decode_timer.open("decode_one");
                            let segment_posting = self.read_postings(&term_info);
                            segment_postings.push(segment_posting);
                        }
                        None => {
                            // currently this is a strict intersection.
                            return SegmentPostings::empty();
                        }
                    }
                }
            }
            {
                let _intersection_time = timer.open("intersection");
                intersection(segment_postings)
            }
        }
    }

}


// impl SerializableSegment for SegmentReader {
//
//     fn write_postings(&self, mut serializer: PostingsSerializer) -> io::Result<()> {
//         let mut term_infos_it = self.term_infos.stream();
//         loop {
//             match term_infos_it.next() {
//                 Some((term_data, term_info)) => {
//                     let term = Term::from(term_data);
//                     try!(serializer.new_term(&term, term_info.doc_freq));
//                     let segment_postings = self.read_postings(term_info.postings_offset);
//                     try!(serializer.write_docs(&segment_postings.doc_ids[..]));
//                 },
//                 None => { break; }
//             }
//         }
//         Ok(())
//     }
//
//     fn write_store(&self, )
//
//     fn write(&self, mut serializer: SegmentSerializer) -> io::Result<()> {
//         try!(self.write_postings(serializer.get_postings_serializer()));
//         try!(self.write_store(serializer.get_store_serializer()));
//
//         for doc_id in 0..self.max_doc() {
//             let doc = try!(self.store_reader.get(&doc_id));
//             try!(serializer.store_doc(&mut doc.text_fields()));
//         }
//         serializer.close()
//     }
// }
