use core::index::{Segment, SegmentId};
use schema::Term;
use store::StoreReader;
use schema::Document;
use directory::ReadOnlySource;
use DocId;
use core::index::SegmentComponent;
use std::io;
use std::str;
use postings::TermInfo;
use datastruct::FstMap;
use std::fmt;
use rustc_serialize::json;
use core::index::SegmentInfo;
use common::OpenTimer;
use schema::Field;
use core::convert_to_ioerror;
use postings::SegmentPostings;
use postings::Postings;
use fastfield::{U32FastFieldsReader, U32FastFieldReader};
use postings::intersection;
use schema::FieldEntry;
use schema::Schema;

impl fmt::Debug for SegmentReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentReader({:?})", self.segment_id)
    }
}


pub struct SegmentReader {
    segment_info: SegmentInfo,
    segment_id: SegmentId,
    term_infos: FstMap<TermInfo>,
    postings_data: ReadOnlySource,
    store_reader: StoreReader,
    fast_fields_reader: U32FastFieldsReader,
    schema: Schema,
}

impl SegmentReader {
    /// Returns the highest document id ever attributed in
    /// this segment + 1.
    /// Today, `tantivy` does not handle deletes so, it happens
    /// to also be the number of documents in the index.
    pub fn max_doc(&self) -> DocId {
        self.segment_info.max_doc
    }

    pub fn get_store_reader(&self) -> &StoreReader {
        &self.store_reader
    }

    /// Open a new segment for reading.
    pub fn open(segment: Segment) -> io::Result<SegmentReader> {
        let segment_info_reader = try!(segment.open_read(SegmentComponent::INFO));
        let segment_info_data = try!(str::from_utf8(&*segment_info_reader)
                                         .map_err(convert_to_ioerror));
        let segment_info: SegmentInfo = try!(json::decode(&segment_info_data)
                                                 .map_err(convert_to_ioerror));
        let source = try!(segment.open_read(SegmentComponent::TERMS));
        let term_infos = try!(FstMap::from_source(source));
        let store_reader = StoreReader::new(try!(segment.open_read(SegmentComponent::STORE)));
        let postings_shared_mmap = try!(segment.open_read(SegmentComponent::POSTINGS));
        let fast_field_data = try!(segment.open_read(SegmentComponent::FASTFIELDS));
        let fast_fields_reader = try!(U32FastFieldsReader::open(fast_field_data));
        
        let schema = segment.schema();
        Ok(SegmentReader {
            segment_info: segment_info,
            postings_data: postings_shared_mmap,
            term_infos: term_infos,
            segment_id: segment.id(),
            store_reader: store_reader,
            fast_fields_reader: fast_fields_reader,
            schema: schema,
        })
    }


    pub fn term_infos(&self) -> &FstMap<TermInfo> {
        &self.term_infos
    }


    /// Returns the document (or to be accurate, its stored field)
    /// bearing the given doc id.
    /// This method is slow and should seldom be called from
    /// within a collector.
    pub fn doc(&self, doc_id: &DocId) -> io::Result<Document> {
        self.store_reader.get(doc_id)
    }

    pub fn get_fast_field_reader(&self, field: &Field) -> io::Result<U32FastFieldReader> {
        let field_entry = self.schema.field_entry(field);
        match *field_entry {
            FieldEntry::Text(_, _) => {
                Err(io::Error::new(io::ErrorKind::Other, "fast field are not yet supported for text fields."))
            },
            FieldEntry::U32(_, ref options) => {
                //if options.
                //Err(io::Error::new(io::ErrorKind::Other, "fast field are not yet supported for text fields."))
                self.fast_fields_reader.get_field(field)
            },
        }
        
    }

    pub fn read_postings(&self, term_info: &TermInfo) -> SegmentPostings {
        let offset = term_info.postings_offset as usize;
        let postings_data = &self.postings_data.as_slice()[offset..];
        SegmentPostings::from_data(term_info.doc_freq, &postings_data)
    }

    pub fn get_term<'a>(&'a self, term: &Term) -> Option<TermInfo> {
        self.term_infos.get(term.as_slice())
    }

    /// Returns the list of doc ids containing all of the
    /// given terms.
    pub fn search<'a, 'b>(&'b self, terms: &Vec<Term>, mut timer: OpenTimer<'a>) -> Box<Postings + 'b> {
        if terms.len() == 1 {
            match self.get_term(&terms[0]) {
                Some(term_info) => {
                    let postings: SegmentPostings<'b> = self.read_postings(&term_info);
                    Box::new(postings)
                },
                None => {
                    Box::new(SegmentPostings::empty())
                },
            }
        } else {
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
                            return Box::new(SegmentPostings::empty());
                        }
                    }
                }
            }
            Box::new(intersection(segment_postings))
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
